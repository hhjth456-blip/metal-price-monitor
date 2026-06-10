import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time
import gspread
from google.oauth2.service_account import Credentials
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(
    page_title="비철금속·원유 시황 모니터",
    page_icon="📊",
    layout="wide"
)

# ── 상수 ─────────────────────────────────────────────────
METALS = ["알루미늄", "납", "아연", "구리", "주석", "니켈"]
OILS   = ["WTI", "브렌트유"]

# 조달청 비축물자 게시판
PPS_LIST_URL = "https://pps.go.kr/bichuk/bbs/list.do"
PPS_VIEW_URL = "https://pps.go.kr/bichuk/bbs/view.do"
PPS_BBS_KEY  = "00823"   # 비철금속국제가격 게시판

NAVER_OIL_URLS = {
    "WTI":    "https://finance.naver.com/marketindex/worldDailyQuote.naver?marketindexCd=OIL_CL&fdtc=2",
    "브렌트유": "https://finance.naver.com/marketindex/worldDailyQuote.naver?marketindexCd=OIL_BRT&fdtc=2",
}
NAVER_FX_URL = "https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_USDKRW"

# 공통 헤더 (조달청 차단 우회)
PPS_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://pps.go.kr/bichuk/bbs/list.do?key=00823",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Connection":      "keep-alive",
}


# ══════════════════════════════════════════════════════════
#  조달청 pps.go.kr 크롤링 — 목록 수집
# ══════════════════════════════════════════════════════════
def fetch_pps_list(total_pages: int = 1) -> list[dict]:
    """
    게시판 목록에서 bbsSn, 제목(날짜 포함), 등록일 수집
    반환: [{"bbsSn": "...", "title": "...", "reg_date": "..."}, ...]
    """
    entries = []
    session = requests.Session()

    for page in range(1, total_pages + 1):
        params = {
            "key":       PPS_BBS_KEY,
            "pageIndex": page,
            "orderBy":   "bbsOrdr desc",
        }
        try:
            res = session.get(
                PPS_LIST_URL,
                params=params,
                headers=PPS_HEADERS,
                timeout=20,
                verify=False,
            )
            res.raise_for_status()
        except Exception as e:
            st.warning(f"게시판 목록 요청 실패 (page={page}): {e}")
            break

        soup = BeautifulSoup(res.text, "html.parser")
        tbl  = soup.find("table")
        if not tbl:
            st.warning(f"page={page}: 테이블을 찾지 못했습니다.")
            break

        rows = tbl.select("tbody tr")
        if not rows:
            break

        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue

            # bbsSn — <a href="...bbsSn=XXXX..."> 또는 onclick 에서 추출
            link_tag = tr.find("a", href=True)
            bbsSn = None
            if link_tag:
                href = link_tag["href"]
                m = re.search(r"bbsSn=(\d+)", href)
                if m:
                    bbsSn = m.group(1)

            if not bbsSn:
                # onclick 에서도 시도
                onclick = tr.get("onclick", "")
                for tag in tr.find_all(True):
                    onclick = onclick or tag.get("onclick", "")
                m = re.search(r"bbsSn=(\d+)", onclick)
                if m:
                    bbsSn = m.group(1)

            if not bbsSn:
                continue

            title    = tds[1].get_text(strip=True)
            reg_date = tds[4].get_text(strip=True) if len(tds) > 4 else ""

            # 제목에서 날짜 추출: "(20260522,현지시간)" → "20260522"
            m_date = re.search(r"\((\d{8}),현지시간\)", title)
            price_date = m_date.group(1) if m_date else None

            entries.append({
                "bbsSn":      bbsSn,
                "title":      title,
                "price_date": price_date,
                "reg_date":   reg_date,
            })

        time.sleep(0.4)

    return entries


# ══════════════════════════════════════════════════════════
#  조달청 pps.go.kr 크롤링 — 상세 페이지 파싱
# ══════════════════════════════════════════════════════════
def fetch_pps_view(bbsSn: str, session: requests.Session | None = None) -> str | None:
    """게시물 상세 페이지 HTML 반환"""
    if session is None:
        session = requests.Session()

    params = {
        "bbsSn":     bbsSn,
        "key":       PPS_BBS_KEY,
        "pageIndex": 1,
        "orderBy":   "bbsOrdr desc",
        "sc":        "",
        "sw":        "",
    }
    try:
        res = session.get(
            PPS_VIEW_URL,
            params=params,
            headers=PPS_HEADERS,
            timeout=20,
            verify=False,
        )
        res.raise_for_status()
        return res.text
    except Exception as e:
        st.warning(f"상세 페이지 요청 실패 (bbsSn={bbsSn}): {e}")
        return None


def parse_pps_view(html: str, price_date: str) -> dict | None:
    """
    상세 페이지 HTML → {품목명: {전월평균, 전주평균, ...}} 딕셔너리
    가격 테이블은 '품목 가격구분 전월평균 전주평균 Official_전일 Closing_전일 Official_당일 Closing_당일 전일대비' 구조
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # ── 본문 텍스트에서 파싱 (테이블이 pre/text 형태인 경우 대비) ──────
    # 1) <table> 이 있는 경우
    tbl = soup.find("table")
    rows_data = []

    if tbl:
        rows_data = _parse_table_tag(tbl, price_date)

    # 2) 테이블 파싱 실패시 → 본문 텍스트 라인 파싱
    if not rows_data:
        content_div = (
            soup.find("div", class_="bbs_view_cont") or
            soup.find("div", id="bbsContent")         or
            soup.find("div", class_="cont")           or
            soup.find("td", class_="cont")
        )
        raw_text = content_div.get_text("\n") if content_div else soup.get_text("\n")
        rows_data = _parse_text_block(raw_text, price_date)

    if not rows_data:
        return None

    result = {}
    for item, vals in rows_data.items():
        result[item] = vals
    return result


def _safe_float(s: str) -> float | None:
    """콤마 제거 후 float 변환, 실패시 None"""
    try:
        return float(str(s).replace(",", "").strip())
    except Exception:
        return None


def _parse_table_tag(tbl, price_date: str) -> dict:
    """
    HTML <table> 에서 비철금속 가격 파싱
    조달청 테이블 구조:
      행1(header): 품목 | 가격구분 | 전월평균 | 전주평균 | 전일날짜(2열) | 당일날짜(2열) | 전일대비
      데이터행 : 알루미늄 | CASH | ... | ... | Official | Closing | Official | Closing | %
    """
    result = {}

    # 헤더 행으로부터 열 인덱스 탐색
    headers = []
    for tr in tbl.find_all("tr"):
        ths = tr.find_all(["th", "td"])
        texts = [th.get_text(strip=True) for th in ths]
        if any("전월" in t for t in texts):
            headers = texts
            break

    # 데이터 행 수집
    current_item = None
    for tr in tbl.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        texts = [td.get_text(strip=True).replace(",", "") for td in tds]

        # 품목명 감지
        for metal in METALS:
            if any(metal in t for t in texts[:2]):
                current_item = metal
                break

        # CASH 행만 수집 (Official 기준)
        if current_item and any("CASH" in t for t in texts):
            nums = []
            for t in texts:
                if t in METALS or "CASH" in t or "3M" in t:
                    continue
                # 숫자값만 추출
                cleaned = re.sub(r"[^\d.\-]", "", t)
                if cleaned:
                    nums.append(_safe_float(cleaned))
                else:
                    nums.append(None)

            # 숫자 순서: 전월평균, 전주평균, 전일Official, 전일Closing, 당일Official, 당일Closing, 전일대비(%)
            nums = [n for n in nums if n is not None or True]
            if len(nums) >= 6:
                result[current_item] = {
                    "전월평균":     nums[0] if len(nums) > 0 else None,
                    "전주평균":     nums[1] if len(nums) > 1 else None,
                    "전일Official": nums[2] if len(nums) > 2 else None,
                    "전일Closing":  nums[3] if len(nums) > 3 else None,
                    "당일Official": nums[4] if len(nums) > 4 else None,
                    "당일Closing":  nums[5] if len(nums) > 5 else None,
                    "전일대비":     nums[6] if len(nums) > 6 else None,
                }
            current_item = None  # 다음 품목을 위해 초기화

    return result


def _parse_text_block(text: str, price_date: str) -> dict:
    """
    텍스트 블록(pre 형태)에서 비철금속 가격 라인 파싱
    예시 라인:
      알루미늄   CASH     3,599.85    3,675.68   3,720.00  3,706.72  3,706.00  3,720.28  0.37
    """
    result = {}
    lines  = text.splitlines()

    for line in lines:
        matched_metal = None
        for metal in METALS:
            if metal in line:
                matched_metal = metal
                break
        if not matched_metal:
            continue
        if "CASH" not in line:
            continue

        # 모든 숫자 추출 (콤마 포함된 숫자 포함)
        nums_raw = re.findall(r"[\d,]+\.?\d*", line)
        nums     = []
        for n in nums_raw:
            v = _safe_float(n)
            if v is not None and v > 0:
                nums.append(v)

        # 전월평균, 전주평균, 전일Official, 전일Closing, 당일Official, 당일Closing, 전일대비(%)
        if len(nums) >= 6:
            # 전일대비는 음수일 수 있으므로 원본 텍스트에서 별도 추출
            chg_match = re.search(r"(-?\d+\.?\d*)\s*$", line.strip())
            chg_val   = _safe_float(chg_match.group(1)) if chg_match else None

            result[matched_metal] = {
                "전월평균":     nums[0],
                "전주평균":     nums[1],
                "전일Official": nums[2],
                "전일Closing":  nums[3],
                "당일Official": nums[4],
                "당일Closing":  nums[5],
                "전일대비":     chg_val,
            }

    # 환율 파싱
    for line in lines:
        if "환율" in line or "현물종가" in line:
            nums_raw = re.findall(r"[\d,]+\.?\d*", line)
            nums     = [_safe_float(n) for n in nums_raw if _safe_float(n) and _safe_float(n) > 100]
            if nums:
                chg_match = re.search(r"(-?\d+\.?\d*)\s*$", line.strip())
                chg_val   = _safe_float(chg_match.group(1)) if chg_match else None
                result["환율_pps"] = {
                    "전월평균":     nums[0] if len(nums) > 0 else None,
                    "전주평균":     nums[1] if len(nums) > 1 else None,
                    "전일Official": None,
                    "전일Closing":  nums[2] if len(nums) > 2 else None,
                    "당일Official": None,
                    "당일Closing":  nums[3] if len(nums) > 3 else None,
                    "전일대비":     chg_val,
                }
            break

    return result


# ══════════════════════════════════════════════════════════
#  통합 수집 함수 (목록 → 상세 → 파싱)
# ══════════════════════════════════════════════════════════
def fetch_pps_metals(total_pages: int = 1) -> dict:
    """
    조달청 크롤링 메인 함수
    반환: {"YYYYMMDD": {"알루미늄": {...}, "납": {...}, ...}, ...}
    """
    entries = fetch_pps_list(total_pages=total_pages)
    if not entries:
        return {}

    result  = {}
    session = requests.Session()
    total   = len(entries)

    prog_bar = st.progress(0, text="상세 페이지 크롤링 중...")

    for i, entry in enumerate(entries):
        bbsSn      = entry["bbsSn"]
        price_date = entry.get("price_date")

        if not price_date:
            prog_bar.progress((i + 1) / total)
            continue

        # 이미 파싱된 날짜는 skip
        if price_date in result:
            prog_bar.progress((i + 1) / total)
            continue

        html = fetch_pps_view(bbsSn, session=session)
        if not html:
            prog_bar.progress((i + 1) / total)
            time.sleep(0.3)
            continue

        parsed = parse_pps_view(html, price_date)
        if parsed:
            result[price_date] = parsed

        prog_bar.progress((i + 1) / total, text=f"크롤링 중... {price_date} ({i+1}/{total})")
        time.sleep(0.5)  # 서버 부하 방지

    prog_bar.empty()
    return result


# ══════════════════════════════════════════════════════════
#  원유 가격 크롤링 (네이버 금융) — 기존 유지
# ══════════════════════════════════════════════════════════
def fetch_oil_prices(pages: int = 1) -> list:
    headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Referer":         "https://finance.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }
    results = []
    for oil_name, base_url in NAVER_OIL_URLS.items():
        for page in range(1, pages + 1):
            url = f"{base_url}&page={page}"
            try:
                res = requests.get(url, headers=headers, timeout=15, verify=False)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                tbl  = (
                    soup.find("table", class_="tbl_exchange") or
                    soup.find("table", {"class": re.compile(r"tbl_exchange")}) or
                    soup.find("table")
                )
                if not tbl:
                    continue

                for tr in tbl.select("tbody tr"):
                    tds = tr.find_all("td")
                    if len(tds) < 3:
                        continue
                    date_str  = tds[0].get_text(strip=True)
                    price_str = tds[1].get_text(strip=True)
                    chg_str   = tds[2].get_text(strip=True)
                    pct_str   = tds[3].get_text(strip=True) if len(tds) > 3 else ""

                    is_down = False
                    for td_check in tds[1:3]:
                        for tag in td_check.find_all(True):
                            cls = " ".join(tag.get("class", []))
                            if "down" in cls.lower() or "fall" in cls.lower() or "minus" in cls.lower():
                                is_down = True
                    img = tds[1].find("img")
                    if img:
                        alt = img.get("alt", "")
                        src = img.get("src", "")
                        if "하락" in alt or "down" in src.lower():
                            is_down = True

                    try:
                        pct = float(pct_str.replace(",", "").replace("%", ""))
                        if pct < 0:
                            is_down = True
                        elif pct > 0:
                            is_down = False
                    except Exception:
                        pct = None

                    try:
                        price = float(price_str.replace(",", ""))
                    except Exception:
                        continue
                    try:
                        chg = float(chg_str.replace(",", ""))
                        chg = -abs(chg) if is_down else abs(chg)
                    except Exception:
                        chg = None

                    date_clean = date_str.replace(".", "").strip()
                    if len(date_clean) != 8:
                        continue

                    results.append({
                        "날짜":        date_clean,
                        "품목":        oil_name,
                        "당일Closing": price,
                        "전일대비":    chg,
                        "전일대비pct": pct,
                    })
            except Exception:
                pass
            time.sleep(0.3)
    return results


def fetch_oil_latest() -> dict:
    rows   = fetch_oil_prices(pages=1)
    latest = {}
    for r in rows:
        name = r["품목"]
        if name not in latest:
            latest[name] = r
    return latest


# ══════════════════════════════════════════════════════════
#  환율 크롤링 (네이버 금융) — 기존 유지
# ══════════════════════════════════════════════════════════
def fetch_hana_usd_rate() -> dict | None:
    headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Referer":         "https://finance.naver.com/",
    }
    try:
        res = requests.get(NAVER_FX_URL, headers=headers, timeout=15, verify=False)
        res.raise_for_status()
    except Exception as e:
        st.warning(f"네이버 환율 요청 실패: {e}")
        return None

    soup = BeautifulSoup(res.text, "html.parser")

    def _parse_split_number(container_tag):
        if container_tag is None:
            return None
        parts = []
        for span in container_tag.find_all("span"):
            t = span.get_text(strip=True)
            if re.fullmatch(r'[\d.]+', t):
                parts.append(t)
        try:
            return float("".join(parts)) if parts else None
        except Exception:
            return None

    rate = None
    no_today = soup.find("p", class_="no_today")
    if no_today:
        inner_em = no_today.find("em")
        if inner_em:
            inner_em2 = inner_em.find("em")
            rate = _parse_split_number(inner_em2 or inner_em)

    if not rate or rate < 100:
        text = soup.get_text()
        m = re.search(r'(\d{1,4}\.\d{2})', text)
        if m:
            candidate = float(m.group(1))
            if 900 < candidate < 2000:
                rate = candidate

    if not rate or rate < 100:
        for span in soup.find_all("span"):
            txt = span.get_text(strip=True).replace(",", "")
            try:
                v = float(txt)
                if 900 < v < 2000:
                    rate = v
                    break
            except Exception:
                pass

    chg = None
    no_exday = soup.find("p", class_="no_exday")
    if no_exday:
        chg_em = no_exday.find("em")
        chg    = _parse_split_number(chg_em)
        ico    = no_exday.find("span", class_="ico")
        if ico and "down" in ico.get("class", []):
            if chg is not None:
                chg = -abs(chg)

    if not rate or rate < 100:
        st.warning(f"네이버 환율 파싱 실패 (rate={rate})")
        return None

    return {"당일Official": None, "당일Closing": rate, "전일대비": chg}


# ══════════════════════════════════════════════════════════
#  Google Sheets 연결 — 기존 유지
# ══════════════════════════════════════════════════════════
@st.cache_resource
def get_gsheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(st.secrets["sheets"]["spreadsheet_id"])
    try:
        ws = sheet.worksheet(st.secrets["sheets"]["worksheet_name"])
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(
            title=st.secrets["sheets"]["worksheet_name"],
            rows=10000, cols=20
        )
    return ws


def load_gsheet() -> pd.DataFrame:
    try:
        ws   = get_gsheet()
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
        df = df.dropna(subset=["날짜"])
        for col in ["전월평균", "전주평균", "전일Official", "전일Closing",
                    "당일Official", "당일Closing", "전일대비"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Google Sheets 로드 오류: {e}")
        return pd.DataFrame()


def _safe_val(v):
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return v


def save_to_gsheet(price_date: str, data: dict) -> pd.DataFrame:
    try:
        df_existing = load_gsheet()

        if not df_existing.empty:
            existing_dates = df_existing["날짜"].dt.strftime("%Y%m%d").tolist()
            if price_date in existing_dates:
                existing_items = df_existing[
                    df_existing["날짜"].dt.strftime("%Y%m%d") == price_date
                ]["품목"].tolist()
                data = {k: v for k, v in data.items() if k not in existing_items}
                if not data:
                    return df_existing

        new_rows = []
        for item, vals in data.items():
            row = {"날짜": price_date, "품목": item}
            row.update(vals)
            new_rows.append(row)

        if not new_rows:
            return df_existing

        df_new = pd.DataFrame(new_rows)
        df_new["날짜"] = pd.to_datetime(df_new["날짜"], format="%Y%m%d", errors="coerce")
        df_new = df_new.dropna(subset=["날짜"])

        ws            = get_gsheet()
        existing_data = ws.get_all_records()
        cols = ["날짜", "품목", "전월평균", "전주평균", "전일Official",
                "전일Closing", "당일Official", "당일Closing", "전일대비"]
        if not existing_data:
            ws.append_row(cols)

        for _, row in df_new.iterrows():
            ws.append_row([
                row["날짜"].strftime("%Y-%m-%d"),
                str(row.get("품목", "")),
                _safe_val(row.get("전월평균")),
                _safe_val(row.get("전주평균")),
                _safe_val(row.get("전일Official")),
                _safe_val(row.get("전일Closing")),
                _safe_val(row.get("당일Official")),
                _safe_val(row.get("당일Closing")),
                _safe_val(row.get("전일대비")),
            ])
            time.sleep(0.1)

        return load_gsheet()

    except Exception as e:
        st.error(f"Google Sheets 저장 오류: {e}")
        return df_existing if not df_existing.empty else pd.DataFrame()


# ══════════════════════════════════════════════════════════
#  통계 계산 — 기존 유지
# ══════════════════════════════════════════════════════════
def calc_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    today      = df["날짜"].max()
    this_month = today.to_period("M")
    last_month = today.to_period("M") - 1

    results = []
    for item in METALS + OILS + ["환율"]:
        sub = df[df["품목"] == item].copy()
        if sub.empty:
            continue

        price_col = "당일Official" if item in METALS else "당일Closing"
        if price_col not in sub.columns:
            continue

        sub = sub.copy()
        sub["월"] = sub["날짜"].dt.to_period("M")

        this_m = sub[sub["월"] == this_month][price_col].dropna()
        last_m = sub[sub["월"] == last_month][price_col].dropna()

        avg_this = round(this_m.mean(), 2) if not this_m.empty else None
        avg_last = round(last_m.mean(), 2) if not last_m.empty else None
        chg_pct  = None
        if avg_this is not None and avg_last is not None and avg_last != 0:
            chg_pct = round((avg_this - avg_last) / avg_last * 100, 2)

        latest       = sub.sort_values("날짜").iloc[-1]
        latest_price = latest.get(price_col)
        chg_val      = latest.get("전일대비")
        if isinstance(chg_val, float) and pd.isna(chg_val):
            chg_val = None

        basis = "Official" if item in METALS else ("USD/bbl" if item in OILS else "현물종가")

        results.append({
            "품목":            item,
            "최신가":          latest_price,
            "가격기준":        basis,
            "전일대비(%)":     chg_val,
            "당월누적평균":    avg_this,
            "전월평균":        avg_last,
            "전월대비변동(%)": chg_pct,
            "기준일":          latest["날짜"].strftime("%Y-%m-%d"),
        })

    if not results:
        return pd.DataFrame()

    result_df = pd.DataFrame(results)
    for col in ["최신가", "전일대비(%)", "당월누적평균", "전월평균", "전월대비변동(%)"]:
        if col in result_df.columns:
            result_df[col] = pd.to_numeric(result_df[col], errors="coerce")
    return result_df


# ══════════════════════════════════════════════════════════
#  시황 코멘트 / 색상 — 기존 유지
# ══════════════════════════════════════════════════════════
def generate_comment(stats_df: pd.DataFrame) -> str:
    if stats_df.empty:
        return "데이터 없음"

    today_str = datetime.now().strftime("%Y년 %m월 %d일")
    lines     = [f"**📋 {today_str} 비철금속·원유 시황 요약**\n"]
    up_items, dn_items, flat_items, big_movers = [], [], [], []

    for _, row in stats_df.iterrows():
        item = row["품목"]
        if item == "환율":
            continue
        chg = row.get("전일대비(%)")
        mom = row.get("전월대비변동(%)")

        if chg is None or (isinstance(chg, float) and pd.isna(chg)):
            flat_items.append(item)
        elif float(str(chg)) > 0:
            up_items.append(f"{item}({float(chg):+.2f}%)")
        elif float(str(chg)) < 0:
            dn_items.append(f"{item}({float(chg):+.2f}%)")
        else:
            flat_items.append(item)

        if mom is not None and not (isinstance(mom, float) and pd.isna(mom)) and abs(float(str(mom))) >= 3:
            direction = "상승" if float(str(mom)) > 0 else "하락"
            big_movers.append(f"{item} 전월대비 {abs(float(str(mom))):.1f}% {direction}")

    if up_items:   lines.append(f"🔴 **상승:** {', '.join(up_items)}")
    if dn_items:   lines.append(f"🔵 **하락:** {', '.join(dn_items)}")
    if flat_items: lines.append(f"⬜ **보합:** {', '.join(flat_items)}")
    if big_movers: lines.append(f"\n📌 **월간 주요 변동:** {' / '.join(big_movers)}")

    fx_row = stats_df[stats_df["품목"] == "환율"]
    if not fx_row.empty:
        fx      = fx_row.iloc[0]
        fx_val  = fx.get("최신가")
        fx_chg  = fx.get("전일대비(%)")
        fx_mom  = fx.get("전월대비변동(%)")
        fx_str  = f"{float(fx_val):,.2f}"  if pd.notna(fx_val) and fx_val is not None else "-"
        chg_str = f"{float(fx_chg):+.2f}%" if pd.notna(fx_chg) and fx_chg is not None else "-"
        mom_str = f"{float(fx_mom):+.2f}%" if pd.notna(fx_mom) and fx_mom is not None else "-"
        lines.append(f"\n💱 **환율(KRW/USD):** {fx_str} (전일대비 {chg_str} / 전월대비 {mom_str})")

    return "\n\n".join(lines)


def color_val(val):
    if val is None:
        return ""
    try:
        v = float(str(val).replace(",", "").replace("%", ""))
        if v > 0:   return "color:#e74c3c; font-weight:bold"
        elif v < 0: return "color:#2980b9; font-weight:bold"
        return "color:gray"
    except Exception:
        return ""


# ══════════════════════════════════════════════════════════
#  UI
# ══════════════════════════════════════════════════════════
st.title("📊 비철금속·원유 시황 모니터")
st.caption(
    "비철금속 원가데이터: 조달청 비축물자(pps.go.kr) 직접 크롤링 | "
    "원유/환율: 네이버 금융 | Google Sheets 누적 저장"
)

col_btn, col_upd, col_info = st.columns([1, 1, 4])
with col_btn:
    refresh = st.button("🔄 오늘 데이터 수집", use_container_width=True)
with col_upd:
    bulk = st.button("📥 전체 데이터 수집", use_container_width=True)
with col_info:
    st.info(f"현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ── 데이터 수집 ───────────────────────────────────────────
if refresh or bulk:
    total_pages = 50 if bulk else 1  # 1페이지 = 최근 10건

    with st.spinner("조달청 게시판 목록 수집 중..."):
        parsed = fetch_pps_metals(total_pages=total_pages)

    if not parsed:
        st.error(
            "❌ 조달청(pps.go.kr) 크롤링 실패.\n\n"
            "네트워크 연결 또는 사이트 차단 여부를 확인하세요.\n"
            "Streamlit Cloud 배포 환경에서는 조달청이 일부 IP를 차단할 수 있습니다."
        )
    else:
        st.success(f"✅ {len(parsed)}일치 데이터 파싱 완료")

        progress    = st.progress(0)
        saved_count = 0
        date_list   = list(parsed.keys())

        oil_rows = fetch_oil_prices(pages=2)  # 원유 가격 한 번만 수집

        for i, date_str in enumerate(date_list):
            data = parsed[date_str]

            # 원유 가격 추가
            for oil_row in oil_rows:
                if oil_row["날짜"] == date_str:
                    oil_name = oil_row["품목"]
                    if oil_name not in data:
                        data[oil_name] = {
                            "전월평균":     None,
                            "전주평균":     None,
                            "전일Official": None,
                            "전일Closing":  None,
                            "당일Official": None,
                            "당일Closing":  oil_row["당일Closing"],
                            "전일대비":     oil_row["전일대비"],
                        }

            # 환율 추가 (조달청 본문에서 이미 파싱된 경우 우선, 없으면 네이버)
            if "환율_pps" in data:
                data["환율"] = data.pop("환율_pps")
            elif "환율" not in data:
                hana = fetch_hana_usd_rate()
                if hana:
                    data["환율"] = {
                        "전월평균":     None,
                        "전주평균":     None,
                        "전일Official": None,
                        "전일Closing":  None,
                        "당일Official": None,
                        "당일Closing":  hana.get("당일Closing"),
                        "전일대비":     hana.get("전일대비"),
                    }

            save_to_gsheet(date_str, data)
            saved_count += 1
            progress.progress((i + 1) / len(date_list))
            time.sleep(0.1)

        st.success(f"✅ {saved_count}일치 데이터 Google Sheets 저장 완료!")

    st.cache_data.clear()


# ── Sheets 로드 ───────────────────────────────────────────
df_all = load_gsheet()

if df_all.empty:
    st.warning("⚠️ 저장된 데이터가 없습니다. 상단 버튼을 눌러 데이터를 수집하세요!")
    st.stop()

stats_df = calc_stats(df_all)

# ── 탭 구성 ───────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📌 오늘 시황", "📈 추이 차트", "📊 통계 분석"])


# ════════════════════════════════
# TAB 1
# ════════════════════════════════
with tab1:
    st.markdown(generate_comment(stats_df))
    st.divider()

    st.subheader("💡 당일 Official (CASH, USD/ton)")
    metal_stats = stats_df[stats_df["품목"].isin(METALS)]
    cols = st.columns(len(METALS))
    for i, (_, row) in enumerate(metal_stats.iterrows()):
        price = row.get("최신가")
        chg   = row.get("전일대비(%)")
        try:
            delta_str   = f"{float(chg):+.2f}%" if chg is not None and pd.notna(chg) else "-"
            delta_color = "normal" if chg is not None and pd.notna(chg) else "off"
        except Exception:
            delta_str, delta_color = "-", "off"
        cols[i].metric(
            label=row["품목"],
            value=f"${float(price):,.2f}" if pd.notna(price) and price is not None else "-",
            delta=delta_str,
            delta_color=delta_color,
        )

    st.divider()

    st.subheader("🛢️ 국제유가 (USD/bbl)")
    oil_live = fetch_oil_latest()
    oil_cols = st.columns(len(OILS))
    for i, oil_name in enumerate(OILS):
        if oil_name in oil_live:
            o       = oil_live[oil_name]
            o_price = o.get("당일Closing")
            o_pct   = o.get("전일대비pct")
            o_chg   = o.get("전일대비")
            try:
                if o_pct is not None:
                    delta_str, delta_color = f"{float(o_pct):+.2f}%", "normal"
                elif o_chg is not None:
                    delta_str, delta_color = f"{float(o_chg):+.2f}", "normal"
                else:
                    delta_str, delta_color = "-", "off"
            except Exception:
                delta_str, delta_color = "-", "off"
            oil_cols[i].metric(
                label=oil_name,
                value=f"${float(o_price):,.2f}" if o_price else "-",
                delta=delta_str,
                delta_color=delta_color,
            )
        else:
            oil_cols[i].metric(label=oil_name, value="-", delta="-", delta_color="off")

    oil_stats = stats_df[stats_df["품목"].isin(OILS)]
    if not oil_stats.empty:
        st.markdown("**월간 비교**")
        oc1, oc2 = st.columns(2)
        for idx, (_, row) in enumerate(oil_stats.iterrows()):
            col_target = oc1 if idx == 0 else oc2
            avg_this   = row.get("당월누적평균")
            avg_last   = row.get("전월평균")
            mom        = row.get("전월대비변동(%)")
            col_target.markdown(
                f"**{row['품목']}** · 당월평균 "
                f"{'${:,.2f}'.format(float(avg_this)) if pd.notna(avg_this) else '-'} · "
                f"전월평균 "
                f"{'${:,.2f}'.format(float(avg_last)) if pd.notna(avg_last) else '-'} · "
                f"변동 "
                f"{'{:+.2f}%'.format(float(mom)) if pd.notna(mom) else '-'}"
            )

    st.divider()

    st.subheader("📋 전월대비 분석 테이블")
    display_cols = ["품목", "최신가", "가격기준", "전일대비(%)",
                    "당월누적평균", "전월평균", "전월대비변동(%)", "기준일"]
    display_cols = [c for c in display_cols if c in stats_df.columns]
    style_cols   = [c for c in ["전일대비(%)", "전월대비변동(%)"] if c in display_cols]
    fmt_dict     = {}
    if "최신가"          in display_cols: fmt_dict["최신가"]          = "{:,.2f}"
    if "전일대비(%)"     in display_cols: fmt_dict["전일대비(%)"]     = "{:+.2f}%"
    if "당월누적평균"    in display_cols: fmt_dict["당월누적평균"]    = "{:,.2f}"
    if "전월평균"        in display_cols: fmt_dict["전월평균"]        = "{:,.2f}"
    if "전월대비변동(%)" in display_cols: fmt_dict["전월대비변동(%)"] = "{:+.2f}%"

    styled = stats_df[display_cols].style.format(fmt_dict, na_rep="-")
    if style_cols:
        styled = styled.map(color_val, subset=style_cols)

    st.dataframe(styled, use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("💱 환율 (KRW/USD)")
    hana_live = fetch_hana_usd_rate()
    if hana_live:
        live_rate = hana_live.get("당일Closing")
        live_chg  = hana_live.get("전일대비")
        fx_row    = stats_df[stats_df["품목"] == "환율"]
        avg_this  = fx_row.iloc[0].get("당월누적평균")    if not fx_row.empty else None
        avg_last  = fx_row.iloc[0].get("전월평균")        if not fx_row.empty else None
        mom_chg   = fx_row.iloc[0].get("전월대비변동(%)") if not fx_row.empty else None

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("💱 환율 (실시간)",
                  f"{live_rate:,.2f}" if live_rate else "-",
                  delta=f"{live_chg:+.2f}" if live_chg else None)
        c2.metric("당월 누적 평균",
                  f"{float(avg_this):,.2f}" if avg_this and pd.notna(avg_this) else "-")
        c3.metric("전월 평균",
                  f"{float(avg_last):,.2f}" if avg_last and pd.notna(avg_last) else "-")
        c4.metric("전월대비 변동",
                  f"{float(mom_chg):+.2f}%" if mom_chg and pd.notna(mom_chg) else "-")
    else:
        st.warning("⚠️ 환율 실시간 조회 실패")


# ════════════════════════════════
# TAB 2
# ════════════════════════════════
with tab2:
    st.subheader("📈 품목별 Official 가격 추이 (CASH, USD/ton)")
    selected = st.multiselect("품목 선택", options=METALS, default=["구리", "알루미늄", "니켈"])
    period   = st.radio("기간", ["1개월", "3개월", "전체"], horizontal=True, key="period_metal")
    today_dt = df_all["날짜"].max()

    if selected:
        df_chart = df_all[df_all["품목"].isin(selected)].copy()
        if period == "1개월":
            df_chart = df_chart[df_chart["날짜"] >= today_dt - pd.Timedelta(days=30)]
        elif period == "3개월":
            df_chart = df_chart[df_chart["날짜"] >= today_dt - pd.Timedelta(days=90)]
        pivot = df_chart.pivot_table(index="날짜", columns="품목", values="당일Official")
        st.line_chart(pivot, use_container_width=True)

    st.divider()
    st.subheader("🛢️ 국제유가 추이 (USD/bbl)")
    period_oil   = st.radio("기간", ["1개월", "3개월", "전체"], horizontal=True, key="period_oil")
    df_oil_chart = df_all[df_all["품목"].isin(OILS)].copy()
    if not df_oil_chart.empty:
        if period_oil == "1개월":
            df_oil_chart = df_oil_chart[df_oil_chart["날짜"] >= today_dt - pd.Timedelta(days=30)]
        elif period_oil == "3개월":
            df_oil_chart = df_oil_chart[df_oil_chart["날짜"] >= today_dt - pd.Timedelta(days=90)]
        pivot_oil = df_oil_chart.pivot_table(index="날짜", columns="품목", values="당일Closing")
        st.line_chart(pivot_oil, use_container_width=True)
    else:
        st.info("원유 데이터가 아직 없습니다.")

    st.divider()
    st.subheader("💱 환율 추이 (KRW/USD)")
    df_fx = df_all[df_all["품목"] == "환율"].set_index("날짜")[["당일Closing"]].copy()
    df_fx.columns = ["환율(KRW/USD)"]
    if period == "1개월":
        df_fx = df_fx[df_fx.index >= today_dt - pd.Timedelta(days=30)]
    elif period == "3개월":
        df_fx = df_fx[df_fx.index >= today_dt - pd.Timedelta(days=90)]
    st.line_chart(df_fx, use_container_width=True)


# ════════════════════════════════
# TAB 3
# ════════════════════════════════
with tab3:
    st.subheader("📊 월별 평균가 비교")
    item_sel = st.selectbox("품목 선택", METALS + OILS + ["환율"])
    df_item  = df_all[df_all["품목"] == item_sel].copy()
    df_item["월"] = df_item["날짜"].dt.to_period("M").astype(str)

    if item_sel in METALS:
        val_col, label = "당일Official", "Official"
    elif item_sel in OILS:
        val_col, label = "당일Closing", "USD/bbl"
    else:
        val_col, label = "당일Closing", "현물종가"

    monthly = df_item.groupby("월")[val_col].mean().reset_index()
    monthly.columns = ["월", f"월평균({label})"]
    monthly[f"월평균({label})"] = monthly[f"월평균({label})"].round(2)
    monthly["전월대비(%)"] = monthly[f"월평균({label})"].pct_change().mul(100).round(2)

    st.bar_chart(monthly.set_index("월")[f"월평균({label})"], use_container_width=True)
    st.dataframe(
        monthly.style
        .map(color_val, subset=["전월대비(%)"])
        .format({
            f"월평균({label})": "{:,.2f}",
            "전월대비(%)": lambda x: f"{x:+.2f}%" if pd.notna(x) else "-",
        }),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("📁 원본 데이터 다운로드")
    csv_export         = df_all.copy()
    csv_export["날짜"] = csv_export["날짜"].dt.strftime("%Y-%m-%d")
    st.download_button(
        label="⬇️ CSV 다운로드",
        data=csv_export.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"metal_oil_prices_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

st.divider()
st.caption(
    "📌 CASH 기준 LME Official 가격 / 조달청 비축물자(pps.go.kr) 크롤링 / "
    "🛢️ WTI·브렌트유: 네이버 금융 (선물 종가, USD/bbl) / "
    "환율: 조달청 게시물 또는 네이버 금융 현물종가 기준 / 비상업적 참고용"
)
