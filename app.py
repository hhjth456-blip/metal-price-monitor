# app.py
# ══════════════════════════════════════════════════════════
#  비철금속·원유 시황 모니터
#  - 비철금속 6종 : westmetall.com  (LME Cash, USD/ton)
#  - 원유 (WTI/브렌트유) : 네이버 금융
#  - 환율 (KRW/USD)     : 네이버 금융 exchangeDailyQuote (과거 시계열 포함)
#  - 누적 저장 : Google Sheets (배치 저장 · API 호출 최소화)
# ══════════════════════════════════════════════════════════

import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import re
import time
import gspread
from google.oauth2.service_account import Credentials
import urllib3
import os
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(
    page_title="비철금속·원유 시황 모니터",
    page_icon="📊",
    layout="wide",
)

# ══════════════════════════════════════════════════════════
#  상수
# ══════════════════════════════════════════════════════════
METALS = ["알루미늄", "납", "아연", "구리", "주석", "니켈"]
OILS   = ["WTI", "브렌트유"]

WESTMETALL_FIELDS = {
    "LME_Cu_cash": "구리",
    "LME_Al_cash": "알루미늄",
    "LME_Pb_cash": "납",
    "LME_Zn_cash": "아연",
    "LME_Ni_cash": "니켈",
    "LME_Sn_cash": "주석",
}
WESTMETALL_ENG = {
    "Copper":    "구리",
    "Aluminium": "알루미늄",
    "Lead":      "납",
    "Zinc":      "아연",
    "Nickel":    "니켈",
    "Tin":       "주석",
}
MONTH_MAP = {
    "January": 1,  "February": 2,  "March": 3,    "April": 4,
    "May": 5,      "June": 6,      "July": 7,      "August": 8,
    "September": 9,"October": 10,  "November": 11, "December": 12,
}
METAL_PRICE_RANGE = {
    "구리":    (3_000,  30_000),
    "알루미늄": (1_000,  10_000),
    "납":      (500,    5_000),
    "아연":    (1_000,  10_000),
    "니켈":    (5_000,  80_000),
    "주석":    (10_000, 100_000),
}

WESTMETALL_URL     = "https://www.westmetall.com/en/markdaten.php"
WESTMETALL_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://www.westmetall.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

NAVER_OIL_URLS = {
    "WTI":    "https://finance.naver.com/marketindex/worldDailyQuote.naver"
              "?marketindexCd=OIL_CL&fdtc=2",
    "브렌트유": "https://finance.naver.com/marketindex/worldDailyQuote.naver"
              "?marketindexCd=OIL_BRT&fdtc=2",
}
# ↓ 핵심: 일별 환율 시계열 URL
NAVER_FX_DAILY_URL  = (
    "https://finance.naver.com/marketindex/exchangeDailyQuote.naver"
    "?marketindexCd=FX_USDKRW"
)
NAVER_FX_TODAY_URL  = (
    "https://finance.naver.com/marketindex/exchangeDetail.naver"
    "?marketindexCd=FX_USDKRW"
)
NAVER_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://finance.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}
GSHEET_COLS = [
    "날짜", "품목", "전월평균", "전주평균",
    "전일Official", "전일Closing", "당일Official", "당일Closing", "전일대비",
]


# ══════════════════════════════════════════════════════════
#  헬퍼
# ══════════════════════════════════════════════════════════
def _parse_westmetall_date(raw: str) -> datetime | None:
    raw = raw.strip()
    m = re.search(
        r"(\d{1,2})\.\s*(January|February|March|April|May|June|"
        r"July|August|September|October|November|December)\s+(\d{4})", raw,
    )
    if m:
        return datetime(int(m.group(3)), MONTH_MAP[m.group(2)], int(m.group(1)))
    m2 = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m2:
        return datetime(int(m2.group(3)), int(m2.group(2)), int(m2.group(1)))
    return None


def _is_valid_metal_price(kr_name: str, value: float) -> bool:
    lo, hi = METAL_PRICE_RANGE.get(kr_name, (50, 999_999))
    return lo <= value <= hi


def _safe(v):
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return v


# ══════════════════════════════════════════════════════════
#  [A] westmetall 오늘 시황 (메인 페이지)
# ══════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def fetch_metals_today_westmetall() -> dict:
    try:
        resp = requests.get(
            WESTMETALL_URL, headers=WESTMETALL_HEADERS, timeout=15, verify=False
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        date_today = datetime.now().strftime("%Y%m%d")
        date_prev  = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        found_dates = []
        for th in soup.find_all("th"):
            dt = _parse_westmetall_date(th.get_text(strip=True))
            if dt:
                found_dates.append(dt.strftime("%Y%m%d"))
        if len(found_dates) >= 2:
            date_today, date_prev = found_dates[0], found_dates[1]
        elif len(found_dates) == 1:
            date_today = found_dates[0]

        metals_today: dict = {}
        metals_prev:  dict = {}

        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            first_a = tds[0].find("a")
            if not first_a:
                continue
            eng_name = first_a.get_text(strip=True)
            if eng_name not in WESTMETALL_ENG:
                continue

            kr_name = WESTMETALL_ENG[eng_name]
            prices  = []
            for td in tds[1:]:
                a_tag = td.find("a")
                raw   = (a_tag.get_text(strip=True)
                         if a_tag else td.get_text(strip=True))
                raw   = raw.replace(",", "").strip()
                try:
                    v = float(raw)
                    if _is_valid_metal_price(kr_name, v):
                        prices.append(v)
                except ValueError:
                    pass

            if not prices:
                continue

            today_p = prices[0]
            prev_p  = prices[1] if len(prices) >= 2 else None
            chg_pct = None
            if prev_p and prev_p != 0:
                chg_pct = round((today_p - prev_p) / prev_p * 100, 2)

            metals_today[kr_name] = {
                "전월평균": None, "전주평균": None,
                "전일Official": prev_p, "전일Closing": prev_p,
                "당일Official": today_p, "당일Closing": today_p,
                "전일대비": chg_pct,
            }
            if prev_p:
                metals_prev[kr_name] = {
                    "전월평균": None, "전주평균": None,
                    "전일Official": None, "전일Closing": None,
                    "당일Official": prev_p, "당일Closing": prev_p,
                    "전일대비": None,
                }

        result: dict = {}
        if metals_today:
            result[date_today] = metals_today
        if metals_prev:
            result[date_prev]  = metals_prev
        if not result:
            st.warning("westmetall 메인: 금속 데이터 파싱 실패")
        return result

    except Exception as e:
        st.error(f"westmetall 오늘 시황 오류: {e}")
        return {}


# ══════════════════════════════════════════════════════════
#  [B] westmetall 과거 시계열
# ══════════════════════════════════════════════════════════
def _fetch_one_metal_history(field: str, kr_name: str,
                              days: int = 90) -> list[dict]:
    cutoff = datetime.now() - timedelta(days=days)
    rows: list[dict] = []
    try:
        resp = requests.get(
            WESTMETALL_URL,
            params={"action": "table", "field": field},
            headers=WESTMETALL_HEADERS,
            timeout=20, verify=False,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            first_text = tds[0].get_text(strip=True).lower()
            if first_text in ("date", "datum", "") or \
               not re.search(r"\d", first_text):
                continue

            dt = _parse_westmetall_date(tds[0].get_text(strip=True))
            if dt is None or dt < cutoff:
                continue

            price = None
            raw1  = tds[1].get_text(strip=True).replace(",", "").strip()
            try:
                v = float(raw1)
                if _is_valid_metal_price(kr_name, v):
                    price = v
            except ValueError:
                pass

            if price is None:
                for td in tds[2:]:
                    raw = td.get_text(strip=True).replace(",", "").strip()
                    try:
                        v = float(raw)
                        if _is_valid_metal_price(kr_name, v):
                            price = v
                            break
                    except ValueError:
                        pass

            if price is not None:
                rows.append({
                    "날짜":         dt.strftime("%Y%m%d"),
                    "당일Closing":  price,
                    "당일Official": price,
                    "전일대비":     None,
                })

    except Exception as e:
        st.warning(f"westmetall {kr_name} 히스토리 오류: {e}")

    rows.sort(key=lambda x: x["날짜"])
    for i in range(1, len(rows)):
        prev = rows[i - 1]["당일Closing"]
        curr = rows[i]["당일Closing"]
        if prev and prev != 0:
            rows[i]["전일대비"] = round((curr - prev) / prev * 100, 2)
    if rows:
        rows[0]["전일대비"] = None
    return rows


@st.cache_data(ttl=3600)
def fetch_metals_history_westmetall(days: int = 90) -> dict:
    result: dict = {}
    for field, kr_name in WESTMETALL_FIELDS.items():
        rows = _fetch_one_metal_history(field, kr_name, days=days)
        for row in rows:
            ds = row["날짜"]
            if ds not in result:
                result[ds] = {}
            result[ds][kr_name] = {
                "전월평균": None, "전주평균": None,
                "전일Official": None, "전일Closing": None,
                "당일Official": row["당일Closing"],
                "당일Closing":  row["당일Closing"],
                "전일대비":     row.get("전일대비"),
            }
        time.sleep(0.4)
    return result


# ══════════════════════════════════════════════════════════
#  [C] 원유 크롤링 (네이버 금융)
# ══════════════════════════════════════════════════════════
def fetch_oil_prices(pages: int = 1) -> list[dict]:
    results: list[dict] = []
    for oil_name, base_url in NAVER_OIL_URLS.items():
        for page in range(1, pages + 1):
            url = f"{base_url}&page={page}"
            try:
                res = requests.get(
                    url, headers=NAVER_HEADERS, timeout=15, verify=False
                )
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                tbl  = (soup.find("table", class_="tbl_exchange")
                        or soup.find("table"))
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
                    for td_c in tds[1:3]:
                        for tag in td_c.find_all(True):
                            cls = " ".join(tag.get("class", []))
                            if any(k in cls.lower()
                                   for k in ["down", "fall", "minus"]):
                                is_down = True
                    img = tds[1].find("img")
                    if img:
                        alt = img.get("alt", "")
                        src = img.get("src", "")
                        if "하락" in alt or "down" in src.lower():
                            is_down = True
                    pct = None
                    try:
                        pct     = float(pct_str.replace(",", "").replace("%", ""))
                        is_down = pct < 0 if pct != 0 else is_down
                    except Exception:
                        pass
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
    rows = fetch_oil_prices(pages=1)
    latest: dict = {}
    for r in rows:
        if r["품목"] not in latest:
            latest[r["품목"]] = r
    return latest


# ══════════════════════════════════════════════════════════
#  [D] 환율 크롤링 (네이버 금융)
#
#  ▸ fetch_usd_krw_today()   : 실시간 현재가 1건  (exchangeDetail)
#  ▸ fetch_usd_krw_history() : 일별 시계열 (exchangeDailyQuote + page 파라미터)
#    - 1페이지 = 10 영업일
#    - pages 파라미터로 수집 범위 조정
# ══════════════════════════════════════════════════════════
def fetch_usd_krw_today() -> dict | None:
    """실시간 환율 1건 (exchangeDetail 페이지)"""
    try:
        res = requests.get(
            NAVER_FX_TODAY_URL, headers=NAVER_HEADERS, timeout=15, verify=False
        )
        res.raise_for_status()
    except Exception as e:
        st.warning(f"환율 실시간 요청 실패: {e}")
        return None

    soup = BeautifulSoup(res.text, "html.parser")

    def _parse_split(tag):
        if not tag:
            return None
        parts = [
            s.get_text(strip=True)
            for s in tag.find_all("span")
            if re.fullmatch(r"[\d.]+", s.get_text(strip=True))
        ]
        try:
            return float("".join(parts)) if parts else None
        except Exception:
            return None

    rate = None
    no_today = soup.find("p", class_="no_today")
    if no_today:
        em = no_today.find("em")
        if em:
            rate = _parse_split(em.find("em") or em)
    if not rate or rate < 100:
        m = re.search(r"(\d{1,4}\.\d{2})", soup.get_text())
        if m:
            c = float(m.group(1))
            if 900 < c < 2000:
                rate = c
    if not rate or rate < 100:
        for span in soup.find_all("span"):
            try:
                v = float(span.get_text(strip=True).replace(",", ""))
                if 900 < v < 2000:
                    rate = v
                    break
            except Exception:
                pass
    if not rate or rate < 100:
        st.warning(f"환율 파싱 실패 (rate={rate})")
        return None

    chg = None
    no_exday = soup.find("p", class_="no_exday")
    if no_exday:
        chg = _parse_split(no_exday.find("em"))
        ico = no_exday.find("span", class_="ico")
        if ico and "down" in " ".join(ico.get("class", [])):
            chg = -abs(chg) if chg is not None else chg

    return {"당일Official": None, "당일Closing": rate, "전일대비": chg}


def fetch_usd_krw_history(pages: int = 3) -> list[dict]:
    """
    네이버 금융 exchangeDailyQuote 페이지에서 일별 환율 시계열 수집
    1페이지 = 10 영업일
    pages=3  → 약 30 영업일 (1개월)
    pages=13 → 약 130 영업일 (6개월)
    pages=26 → 약 260 영업일 (1년)

    반환: [{"날짜": "YYYYMMDD", "당일Closing": float, "전일대비": float}, ...]
    """
    rows: list[dict] = []

    for page in range(1, pages + 1):
        url = f"{NAVER_FX_DAILY_URL}&page={page}"
        try:
            res = requests.get(
                url, headers=NAVER_HEADERS, timeout=15, verify=False
            )
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            # 테이블 파싱
            # 구조: 날짜 | 매매기준율 | 전일대비(img+숫자) | 현찰사실때 | 현찰파실때 | 송금보내실때 | 송금받으실때
            tbl = soup.find("table")
            if not tbl:
                break

            for tr in tbl.select("tbody tr, tr"):
                tds = tr.find_all("td")
                if len(tds) < 3:
                    continue

                # 날짜 파싱 (2026.06.12 형식)
                date_raw   = tds[0].get_text(strip=True)
                date_clean = date_raw.replace(".", "").strip()
                if len(date_clean) != 8 or not date_clean.isdigit():
                    continue

                # 매매기준율 (두 번째 열)
                rate_str = tds[1].get_text(strip=True).replace(",", "").strip()
                try:
                    rate = float(rate_str)
                    if not (900 < rate < 2000):
                        continue
                except ValueError:
                    continue

                # 전일대비 (세 번째 열: img alt + 숫자)
                chg_td  = tds[2]
                chg_raw = chg_td.get_text(strip=True).replace(",", "").strip()
                chg     = None
                try:
                    chg = float(chg_raw)
                except ValueError:
                    pass

                # 방향 감지 (img alt 태그)
                is_down = False
                img = chg_td.find("img")
                if img:
                    alt = img.get("alt", "")
                    if "하락" in alt:
                        is_down = True
                    elif "상승" in alt:
                        is_down = False

                if chg is not None and is_down:
                    chg = -abs(chg)
                elif chg is not None:
                    chg = abs(chg)

                # 전일대비(%) 계산
                chg_pct = None
                if chg is not None and rate and rate != 0:
                    prev = rate - chg
                    if prev != 0:
                        chg_pct = round(chg / prev * 100, 2)

                rows.append({
                    "날짜":        date_clean,
                    "당일Closing": rate,
                    "전일대비":    chg_pct,   # % 값으로 저장
                })

        except Exception:
            pass
        time.sleep(0.3)

    return rows


# ══════════════════════════════════════════════════════════
#  [E] Google Sheets (배치 저장 · API 최소화)
# ══════════════════════════════════════════════════════════
@st.cache_resource
def get_gsheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    gcp_env = os.environ.get("GCP_SERVICE_ACCOUNT")
    if gcp_env:
        svc_info       = json.loads(gcp_env)
        spreadsheet_id = os.environ.get("SPREADSHEET_ID", "")
        worksheet_name = os.environ.get("WORKSHEET_NAME", "Sheet1")
    else:
        try:
            svc_info       = dict(st.secrets["gcp_service_account"])
            spreadsheet_id = st.secrets["sheets"]["spreadsheet_id"]
            worksheet_name = st.secrets["sheets"]["worksheet_name"]
        except Exception:
            st.error("❌ GCP 인증 정보가 없습니다. Secrets 설정을 확인하세요.")
            st.stop()

    creds  = Credentials.from_service_account_info(svc_info, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(spreadsheet_id)
    try:
        ws = sheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=worksheet_name, rows=10000, cols=20)
    return ws


def _append_rows_with_backoff(ws, rows: list[list], max_retry: int = 5):
    for attempt in range(max_retry):
        try:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            return
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                wait = 2 ** attempt * 5
                st.warning(
                    f"⏳ API 한도 초과 → {wait}초 대기 후 재시도 "
                    f"({attempt + 1}/{max_retry})"
                )
                time.sleep(wait)
            else:
                raise
    st.error("❌ Google Sheets 저장 실패: API 한도 재시도 소진")


@st.cache_data(ttl=300)
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


def save_batch_to_gsheet(all_data: dict[str, dict]) -> pd.DataFrame:
    df_existing = load_gsheet()

    existing_keys: set[tuple] = set()
    if not df_existing.empty:
        for _, row in df_existing.iterrows():
            existing_keys.add((
                row["날짜"].strftime("%Y%m%d"),
                str(row["품목"]),
            ))

    new_rows: list[list] = []
    for date_str, items in sorted(all_data.items()):
        for item_name, vals in items.items():
            if (date_str, item_name) in existing_keys:
                continue
            try:
                date_fmt = datetime.strptime(
                    date_str, "%Y%m%d"
                ).strftime("%Y-%m-%d")
            except ValueError:
                continue
            new_rows.append([
                date_fmt,
                item_name,
                _safe(vals.get("전월평균")),
                _safe(vals.get("전주평균")),
                _safe(vals.get("전일Official")),
                _safe(vals.get("전일Closing")),
                _safe(vals.get("당일Official")),
                _safe(vals.get("당일Closing")),
                _safe(vals.get("전일대비")),
            ])

    if not new_rows:
        st.info("ℹ️ 모든 데이터가 이미 저장되어 있습니다.")
        return df_existing

    ws = get_gsheet()
    try:
        existing_raw = ws.get_all_values()
    except gspread.exceptions.APIError:
        existing_raw = []
    if not existing_raw:
        _append_rows_with_backoff(ws, [GSHEET_COLS])

    _append_rows_with_backoff(ws, new_rows)
    st.success(f"✅ {len(new_rows)}행 저장 완료")

    load_gsheet.clear()
    return load_gsheet()


# ══════════════════════════════════════════════════════════
#  [F] 통계 계산
# ══════════════════════════════════════════════════════════
def calc_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    today      = df["날짜"].max()
    this_month = today.to_period("M")
    last_month = this_month - 1
    results    = []

    BASIS = {
        "구리":    "LME Cash USD/ton (westmetall)",
        "알루미늄": "LME Cash USD/ton (westmetall)",
        "납":      "LME Cash USD/ton (westmetall)",
        "아연":    "LME Cash USD/ton (westmetall)",
        "니켈":    "LME Cash USD/ton (westmetall)",
        "주석":    "LME Cash USD/ton (westmetall)",
        "WTI":    "USD/bbl (네이버금융)",
        "브렌트유": "USD/bbl (네이버금융)",
        "환율":    "매매기준율 KRW/USD (네이버금융)",
    }

    for item in METALS + OILS + ["환율"]:
        sub = df[df["품목"] == item].copy()
        if sub.empty:
            continue
        price_col = "당일Official" if item in METALS else "당일Closing"
        if price_col not in sub.columns:
            continue

        sub["월"] = sub["날짜"].dt.to_period("M")
        this_m    = sub[sub["월"] == this_month][price_col].dropna()
        last_m    = sub[sub["월"] == last_month][price_col].dropna()
        avg_this  = round(this_m.mean(), 2) if not this_m.empty else None
        avg_last  = round(last_m.mean(), 2) if not last_m.empty else None
        chg_pct   = None
        if avg_this and avg_last and avg_last != 0:
            chg_pct = round((avg_this - avg_last) / avg_last * 100, 2)

        latest       = sub.sort_values("날짜").iloc[-1]
        latest_price = latest.get(price_col)
        chg_val      = latest.get("전일대비")
        if isinstance(chg_val, float) and pd.isna(chg_val):
            chg_val = None

        results.append({
            "품목":            item,
            "최신가":          latest_price,
            "가격기준":        BASIS.get(item, ""),
            "전일대비(%)":     chg_val,
            "당월누적평균":    avg_this,
            "전월평균":        avg_last,
            "전월대비변동(%)": chg_pct,
            "기준일":          latest["날짜"].strftime("%Y-%m-%d"),
        })

    if not results:
        return pd.DataFrame()
    result_df = pd.DataFrame(results)
    for col in ["최신가", "전일대비(%)", "당월누적평균",
                "전월평균", "전월대비변동(%)"]:
        if col in result_df.columns:
            result_df[col] = pd.to_numeric(result_df[col], errors="coerce")
    return result_df


# ══════════════════════════════════════════════════════════
#  [G] 시황 코멘트 / 색상
# ══════════════════════════════════════════════════════════
def generate_comment(stats_df: pd.DataFrame) -> str:
    if stats_df.empty:
        return "데이터 없음"
    today_str = datetime.now().strftime("%Y년 %m월 %d일")
    lines = [f"**📋 {today_str} 비철금속·원유 시황 요약**\n"]
    up, dn, flat, big = [], [], [], []

    for _, row in stats_df.iterrows():
        item = row["품목"]
        if item == "환율":
            continue
        chg = row.get("전일대비(%)")
        mom = row.get("전월대비변동(%)")
        try:
            chg_f = float(chg) if chg is not None and pd.notna(chg) else None
        except Exception:
            chg_f = None

        if chg_f is None:   flat.append(item)
        elif chg_f > 0:     up.append(f"{item}({chg_f:+.2f}%)")
        elif chg_f < 0:     dn.append(f"{item}({chg_f:+.2f}%)")
        else:               flat.append(item)

        try:
            mom_f = float(mom) if mom is not None and pd.notna(mom) else None
            if mom_f and abs(mom_f) >= 3:
                big.append(
                    f"{item} 전월대비 {abs(mom_f):.1f}% "
                    f"{'상승' if mom_f > 0 else '하락'}"
                )
        except Exception:
            pass

    if up:   lines.append(f"🔴 **상승:** {', '.join(up)}")
    if dn:   lines.append(f"🔵 **하락:** {', '.join(dn)}")
    if flat: lines.append(f"⬜ **보합:** {', '.join(flat)}")
    if big:  lines.append(f"\n📌 **월간 주요 변동:** {' / '.join(big)}")

    fx_row = stats_df[stats_df["품목"] == "환율"]
    if not fx_row.empty:
        fx    = fx_row.iloc[0]
        fx_s  = f"{float(fx['최신가']):,.2f}"           if pd.notna(fx.get('최신가'))          else "-"
        chg_s = f"{float(fx['전일대비(%)']):+.2f}%"      if pd.notna(fx.get('전일대비(%)'))    else "-"
        mom_s = f"{float(fx['전월대비변동(%)']):+.2f}%"  if pd.notna(fx.get('전월대비변동(%)')) else "-"
        lines.append(
            f"\n💱 **환율(KRW/USD):** {fx_s}원 "
            f"(전일대비 {chg_s} / 전월대비 {mom_s})"
        )
    return "\n\n".join(lines)


def color_val(val):
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
    "비철금속 6종: westmetall.com LME Cash (USD/ton) | "
    "원유/환율: 네이버 금융 | Google Sheets 누적 저장"
)

with st.expander("📐 데이터 출처 & 단위 안내", expanded=False):
    st.markdown("""
| 품목 | 출처 | 단위 | 비고 |
|------|------|------|------|
| 구리 / 알루미늄 / 납 / 아연 / 니켈 / 주석 | westmetall.com | **USD/ton** | LME Cash Settlement |
| WTI / 브렌트유 | 네이버 금융 | **USD/bbl** | — |
| 환율 | 네이버 금융 exchangeDailyQuote | **KRW/USD** | 매매기준율, 과거 시계열 수집 가능 |
""")

col_btn, col_bulk, col_info = st.columns([1, 1, 4])
with col_btn:
    refresh = st.button("🔄 오늘 데이터 수집", use_container_width=True)
with col_bulk:
    bulk = st.button("📥 과거 데이터 수집", use_container_width=True)
with col_info:
    st.info(
        f"현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  "
        "※ LME Cash 기준 | 매 영업일 1회 수집 권장"
    )


# ══════════════════════════════════════════════════════════
#  데이터 수집 & 저장
# ══════════════════════════════════════════════════════════
if refresh or bulk:
    fetch_metals_today_westmetall.clear()
    fetch_metals_history_westmetall.clear()

    # ── ① 비철금속 ──────────────────────────────────────
    if bulk:
        with st.spinner("과거 1년치 LME 데이터 수집 중 (westmetall)..."):
            parsed = fetch_metals_history_westmetall(days=365)
    else:
        with st.spinner("오늘 LME 시황 수집 중 (westmetall)..."):
            parsed = fetch_metals_today_westmetall()

    if not parsed:
        st.error("❌ 비철금속 데이터 수집 실패")
        st.stop()

    latest_date  = max(parsed.keys())
    preview_data = parsed.get(latest_date, {})
    if preview_data:
        ld = latest_date
        with st.expander(
            f"📋 비철금속 수신 미리보기 ({ld[:4]}-{ld[4:6]}-{ld[6:]})",
            expanded=True,
        ):
            st.table(pd.DataFrame([
                {
                    "품목":           m,
                    "당일 (USD/ton)": (
                        f"{preview_data[m]['당일Closing']:,.2f}"
                        if preview_data.get(m, {}).get("당일Closing") else "-"
                    ),
                    "전일 (USD/ton)": (
                        f"{preview_data[m]['전일Closing']:,.2f}"
                        if preview_data.get(m, {}).get("전일Closing") else "-"
                    ),
                    "전일대비(%)": (
                        f"{preview_data[m]['전일대비']:+.2f}%"
                        if preview_data.get(m, {}).get("전일대비") is not None
                        else "-"
                    ),
                }
                for m in METALS if m in preview_data
            ]))
    st.success(f"✅ {len(parsed)}일치 비철금속 데이터 수신")

    # ── ② 원유 ─────────────────────────────────────────
    oil_pages = 12 if bulk else 1
    with st.spinner("원유 가격 수집 중 (네이버 금융)..."):
        oil_rows = fetch_oil_prices(pages=oil_pages)

    # ── ③ 환율 시계열 ───────────────────────────────────
    # 오늘: 3페이지(약 30 영업일) / 과거: 26페이지(약 1년)
    fx_pages = 26 if bulk else 3
    with st.spinner(
        f"환율 {'과거 1년치' if bulk else '최근 1개월'} 수집 중 (네이버 금융)..."
    ):
        fx_rows = fetch_usd_krw_history(pages=fx_pages)

    if fx_rows:
        st.success(f"✅ 환율 {len(fx_rows)}일치 수신 "
                   f"({fx_rows[-1]['날짜'][:4]}-{fx_rows[-1]['날짜'][4:6]}-{fx_rows[-1]['날짜'][6:]} "
                   f"~ {fx_rows[0]['날짜'][:4]}-{fx_rows[0]['날짜'][4:6]}-{fx_rows[0]['날짜'][6:]})")

    # ── ④ 데이터 병합 ───────────────────────────────────
    all_data: dict[str, dict] = {
        ds: dict(items) for ds, items in parsed.items()
    }

    # 원유 병합
    for oil_row in oil_rows:
        ds   = oil_row["날짜"]
        name = oil_row["품목"]
        if ds not in all_data:
            all_data[ds] = {}
        if name not in all_data[ds]:
            all_data[ds][name] = {
                "전월평균": None, "전주평균": None,
                "전일Official": None, "전일Closing": None,
                "당일Official": None,
                "당일Closing":  oil_row["당일Closing"],
                "전일대비":     oil_row["전일대비"],
            }

    # 환율 병합 (과거 시계열 전체)
    for fx_row in fx_rows:
        ds = fx_row["날짜"]
        if ds not in all_data:
            all_data[ds] = {}
        if "환율" not in all_data[ds]:
            all_data[ds]["환율"] = {
                "전월평균": None, "전주평균": None,
                "전일Official": None, "전일Closing": None,
                "당일Official": None,
                "당일Closing":  fx_row["당일Closing"],
                "전일대비":     fx_row["전일대비"],
            }

    # ── ⑤ 배치 저장 ────────────────────────────────────
    with st.spinner("Google Sheets 저장 중 (일괄 저장)..."):
        save_batch_to_gsheet(all_data)

    st.cache_data.clear()


# ══════════════════════════════════════════════════════════
#  Sheets 로드 & 통계
# ══════════════════════════════════════════════════════════
df_all = load_gsheet()

if df_all.empty:
    st.warning("⚠️ 저장된 데이터가 없습니다. 상단 버튼으로 데이터를 수집하세요!")
    st.stop()

stats_df = calc_stats(df_all)


# ══════════════════════════════════════════════════════════
#  탭
# ══════════════════════════════════════════════════════════
tab1, tab2, tab3 = st.tabs(["📌 오늘 시황", "📈 추이 차트", "📊 통계 분석"])


# ════════════════════════════════
# TAB 1
# ════════════════════════════════
with tab1:
    st.markdown(generate_comment(stats_df))
    st.divider()

    st.subheader("💡 비철금속 LME Cash (USD/ton)")
    metal_stats = stats_df[stats_df["품목"].isin(METALS)]
    cols = st.columns(len(METALS))
    for i, metal_name in enumerate(METALS):
        row_df = metal_stats[metal_stats["품목"] == metal_name]
        if row_df.empty:
            cols[i].metric(label=metal_name, value="-",
                           delta="-", delta_color="off")
            continue
        row   = row_df.iloc[0]
        price = row.get("최신가")
        chg   = row.get("전일대비(%)")
        try:
            delta_str   = (f"{float(chg):+.2f}%"
                           if chg is not None and pd.notna(chg) else "-")
            delta_color = ("normal"
                           if chg is not None and pd.notna(chg) else "off")
        except Exception:
            delta_str, delta_color = "-", "off"
        cols[i].metric(
            label=metal_name,
            value=(f"${float(price):,.0f}"
                   if price is not None and pd.notna(price) else "-"),
            delta=delta_str,
            delta_color=delta_color,
            help=row.get("가격기준", ""),
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
            oil_cols[i].metric(
                label=oil_name, value="-", delta="-", delta_color="off"
            )

    oil_stats = stats_df[stats_df["품목"].isin(OILS)]
    if not oil_stats.empty:
        st.markdown("**원유 월간 비교**")
        oc1, oc2 = st.columns(2)
        for idx, (_, row) in enumerate(oil_stats.iterrows()):
            tgt      = oc1 if idx == 0 else oc2
            avg_this = row.get("당월누적평균")
            avg_last = row.get("전월평균")
            mom      = row.get("전월대비변동(%)")
            tgt.markdown(
                f"**{row['품목']}** · 당월평균 "
                f"{'${:,.2f}'.format(float(avg_this)) if pd.notna(avg_this) else '-'}"
                f" · 전월평균 "
                f"{'${:,.2f}'.format(float(avg_last)) if pd.notna(avg_last) else '-'}"
                f" · 변동 "
                f"{'{:+.2f}%'.format(float(mom)) if pd.notna(mom) else '-'}"
            )

    st.divider()

    st.subheader("📋 전월대비 분석 테이블")
    display_cols = [
        "품목", "최신가", "가격기준", "전일대비(%)",
        "당월누적평균", "전월평균", "전월대비변동(%)", "기준일",
    ]
    display_cols = [c for c in display_cols if c in stats_df.columns]
    style_cols   = [c for c in ["전일대비(%)", "전월대비변동(%)"]
                    if c in display_cols]
    fmt_dict = {}
    for c, f in [
        ("최신가",         "{:,.2f}"),
        ("전일대비(%)",    "{:+.2f}%"),
        ("당월누적평균",   "{:,.2f}"),
        ("전월평균",       "{:,.2f}"),
        ("전월대비변동(%)", "{:+.2f}%"),
    ]:
        if c in display_cols:
            fmt_dict[c] = f
    styled = stats_df[display_cols].style.format(fmt_dict, na_rep="-")
    if style_cols:
        styled = styled.map(color_val, subset=style_cols)
    st.dataframe(styled, use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("💱 환율 (KRW/USD 매매기준율)")
    fx_live = fetch_usd_krw_today()
    if fx_live:
        live_rate = fx_live.get("당일Closing")
        live_chg  = fx_live.get("전일대비")
        fx_row    = stats_df[stats_df["품목"] == "환율"]
        avg_this  = fx_row.iloc[0].get("당월누적평균")    if not fx_row.empty else None
        avg_last  = fx_row.iloc[0].get("전월평균")        if not fx_row.empty else None
        mom_chg   = fx_row.iloc[0].get("전월대비변동(%)") if not fx_row.empty else None

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("💱 환율 (실시간)",
                  f"{live_rate:,.2f}원" if live_rate else "-",
                  delta=(f"{live_chg:+.2f}원" if live_chg else None))
        c2.metric("당월 누적 평균",
                  f"{float(avg_this):,.2f}원"
                  if avg_this and pd.notna(avg_this) else "-")
        c3.metric("전월 평균",
                  f"{float(avg_last):,.2f}원"
                  if avg_last and pd.notna(avg_last) else "-")
        c4.metric("전월대비 변동",
                  f"{float(mom_chg):+.2f}%"
                  if mom_chg and pd.notna(mom_chg) else "-")
    else:
        st.warning("⚠️ 환율 실시간 조회 실패")


# ════════════════════════════════
# TAB 2
# ════════════════════════════════
with tab2:
    today_dt = df_all["날짜"].max()

    st.subheader("📈 비철금속 가격 추이 (USD/ton)")
    selected = st.multiselect(
        "품목 선택", options=METALS, default=["구리", "알루미늄", "니켈"]
    )
    period = st.radio(
        "기간", ["1개월", "3개월", "전체"], horizontal=True, key="period_metal"
    )
    if selected:
        df_chart = df_all[df_all["품목"].isin(selected)].copy()
        if period == "1개월":
            df_chart = df_chart[
                df_chart["날짜"] >= today_dt - pd.Timedelta(days=30)
            ]
        elif period == "3개월":
            df_chart = df_chart[
                df_chart["날짜"] >= today_dt - pd.Timedelta(days=90)
            ]
        pivot = df_chart.pivot_table(
            index="날짜", columns="품목", values="당일Official"
        )
        if not pivot.empty:
            st.line_chart(pivot, use_container_width=True)
        else:
            st.info("선택 기간에 데이터가 없습니다.")

    st.divider()
    st.subheader("🛢️ 국제유가 추이 (USD/bbl)")
    period_oil = st.radio(
        "기간", ["1개월", "3개월", "전체"], horizontal=True, key="period_oil"
    )
    df_oil_chart = df_all[df_all["품목"].isin(OILS)].copy()
    if not df_oil_chart.empty:
        if period_oil == "1개월":
            df_oil_chart = df_oil_chart[
                df_oil_chart["날짜"] >= today_dt - pd.Timedelta(days=30)
            ]
        elif period_oil == "3개월":
            df_oil_chart = df_oil_chart[
                df_oil_chart["날짜"] >= today_dt - pd.Timedelta(days=90)
            ]
        pivot_oil = df_oil_chart.pivot_table(
            index="날짜", columns="품목", values="당일Closing"
        )
        if not pivot_oil.empty:
            st.line_chart(pivot_oil, use_container_width=True)
    else:
        st.info("원유 데이터가 아직 없습니다.")

    st.divider()
    st.subheader("💱 환율 추이 (KRW/USD 매매기준율)")
    df_fx = (
        df_all[df_all["품목"] == "환율"]
        .set_index("날짜")[["당일Closing"]]
        .copy()
    )
    df_fx.columns = ["환율(KRW/USD)"]
    if period == "1개월":
        df_fx = df_fx[df_fx.index >= today_dt - pd.Timedelta(days=30)]
    elif period == "3개월":
        df_fx = df_fx[df_fx.index >= today_dt - pd.Timedelta(days=90)]
    if not df_fx.empty:
        st.line_chart(df_fx, use_container_width=True)
    else:
        st.info("환율 데이터가 아직 없습니다.")


# ════════════════════════════════
# TAB 3
# ════════════════════════════════
with tab3:
    st.subheader("📊 월별 평균가 비교")
    item_sel = st.selectbox("품목 선택", METALS + OILS + ["환율"])
    df_item  = df_all[df_all["품목"] == item_sel].copy()
    df_item["월"] = df_item["날짜"].dt.to_period("M").astype(str)

    if item_sel in METALS:
        val_col, label = "당일Official", "USD/ton"
    elif item_sel in OILS:
        val_col, label = "당일Closing",  "USD/bbl"
    else:
        val_col, label = "당일Closing",  "KRW"

    monthly = df_item.groupby("월")[val_col].mean().reset_index()
    monthly.columns = ["월", f"월평균({label})"]
    monthly[f"월평균({label})"] = monthly[f"월평균({label})"].round(2)
    monthly["전월대비(%)"] = (
        monthly[f"월평균({label})"].pct_change().mul(100).round(2)
    )

    if not monthly.empty:
        st.bar_chart(
            monthly.set_index("월")[f"월평균({label})"],
            use_container_width=True,
        )
        st.dataframe(
            monthly.style
            .map(color_val, subset=["전월대비(%)"])
            .format({
                f"월평균({label})": "{:,.2f}",
                "전월대비(%)": lambda x: (
                    f"{x:+.2f}%" if pd.notna(x) else "-"
                ),
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("해당 품목 데이터가 없습니다.")

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
    "📌 비철금속: westmetall.com LME Cash Settlement (USD/ton) | "
    "🛢️ 원유: 네이버 금융 | "
    "💱 환율: 네이버 금융 매매기준율 (exchangeDailyQuote) | 비상업적 참고용"
)
