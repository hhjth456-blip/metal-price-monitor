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
import os
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(
    page_title="비철금속·원유 시황 모니터",
    page_icon="📊",
    layout="wide"
)

# ── 상수 ─────────────────────────────────────────────────
METALS = ["알루미늄", "납", "아연", "구리", "주석", "니켈"]
OILS   = ["WTI", "브렌트유"]

HIGHMETAL_URL = "https://highmetal.co.kr/"
HIGHMETAL_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://highmetal.co.kr/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

NAVER_OIL_URLS = {
    "WTI":    "https://finance.naver.com/marketindex/worldDailyQuote.naver"
              "?marketindexCd=OIL_CL&fdtc=2",
    "브렌트유": "https://finance.naver.com/marketindex/worldDailyQuote.naver"
              "?marketindexCd=OIL_BRT&fdtc=2",
}
NAVER_FX_URL = ("https://finance.naver.com/marketindex/exchangeDetail.naver"
                "?marketindexCd=FX_USDKRW")

NAVER_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://finance.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}


# ══════════════════════════════════════════════════════════
#  비철금속 LME 크롤링 (하이메탈)
# ══════════════════════════════════════════════════════════
def fetch_nonferrous_lme() -> dict:
    METAL_KOR = {
        "구리": "구리", "알루미늄": "알루미늄", "아연": "아연",
        "납": "납", "니켈": "니켈", "주석": "주석",
    }
    try:
        resp = requests.get(
            HIGHMETAL_URL, headers=HIGHMETAL_HEADERS, timeout=15, verify=False
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 헤더 th에서 날짜 파싱 (06/03 형식)
        dates = []
        for th in soup.find_all("th"):
            m = re.search(r"(\d{2})/(\d{2})", th.get_text(strip=True))
            if m:
                year = datetime.now().strftime("%Y")
                dates.append(f"{year}{m.group(1)}{m.group(2)}")

        today_date = dates[0] if dates else datetime.now().strftime("%Y%m%d")

        def _parse_price(td):
            # td 직접 텍스트에서 첫 줄 숫자만 추출 (p태그 내용 제외)
            try:
                # p 태그를 제거하고 순수 텍스트만
                for p in td.find_all("p"):
                    p.decompose()
                raw = td.get_text(strip=True).replace(",", "")
                return float(raw)
            except Exception:
                return None

        def _parse_pct(td):
            # p 태그 텍스트에서 % 추출
            try:
                p_tag = td.find("p")
                if not p_tag:
                    return None
                raw   = p_tag.get_text(strip=True)
                m     = re.search(r"(-?[\d.]+)%", raw)
                if not m:
                    return None
                return float(m.group(1))
            except Exception:
                return None

        result = {}
        # class="name" td를 가진 tr만 처리
        for tr in soup.find_all("tr"):
            name_td  = tr.find("td", class_="name")
            price_tds = tr.find_all("td", class_="price")

            if not name_td or len(price_tds) < 2:
                continue

            # lan_Ko 클래스에서 한글 금속명 추출
            lan_ko = name_td.find("p", class_="lan_Ko")
            if not lan_ko:
                continue
            metal_name = lan_ko.get_text(strip=True)

            if metal_name not in METAL_KOR:
                continue

            # price td를 복사해서 파싱 (원본 수정 방지)
            import copy
            td1 = copy.copy(price_tds[0])
            td2 = copy.copy(price_tds[1])

            today_pct = _parse_pct(td1)
            today_p   = _parse_price(td1)
            prev_p    = _parse_price(td2)

            result[METAL_KOR[metal_name]] = {
                "price_date":   today_date,
                "전월평균":     None,
                "전주평균":     None,
                "전일Official": prev_p,
                "전일Closing":  prev_p,
                "당일Official": today_p,
                "당일Closing":  today_p,
                "전일대비":     today_pct,
            }

        if not result:
            st.warning("하이메탈: 금속 데이터를 파싱하지 못했습니다.")
        return result

    except Exception as e:
        st.error(f"하이메탈 크롤링 오류: {e}")
        return {}


        # 헤더에서 날짜 2개 파싱 (06/03 형식)
        header_row = table.find("tr")
        header_ths = header_row.find_all("th") if header_row else []
        dates = []
        for th in header_ths:
            txt = th.get_text(strip=True)
            m = re.search(r"(\d{2})/(\d{2})", txt)
            if m:
                year = datetime.now().strftime("%Y")
                dates.append(f"{year}{m.group(1)}{m.group(2)}")

        today_date = dates[0] if len(dates) > 0 else datetime.now().strftime("%Y%m%d")

        def _parse_price(td):
            try:
                raw = td.get_text(separator=" ", strip=True).replace(",", "")
                return float(raw.split()[0])
            except Exception:
                return None

        def _parse_pct(td):
            try:
                raw   = td.get_text(separator=" ", strip=True)
                m     = re.search(r"([\d.]+)%", raw)
                if not m:
                    return None
                pct   = float(m.group(1))
                is_dn = bool(td.find("img", src=re.compile(r"arrowDown")))
                return -pct if is_dn else pct
            except Exception:
                return None

        result = {}
        for row in table.select("tbody tr"):
            tds = row.find_all("td")
            if not tds:
                continue
            # colspan 구분행 스킵
            if len(tds) == 1 and tds[0].get("colspan"):
                continue

            name_text = tds[0].get_text(separator=" ", strip=True)
            metal_kr  = None
            for k in METAL_KOR:
                if k in name_text:
                    metal_kr = METAL_KOR[k]
                    break

            if metal_kr and len(tds) >= 3:
                today_p   = _parse_price(tds[1])
                today_pct = _parse_pct(tds[1])
                prev_p    = _parse_price(tds[2])

                result[metal_kr] = {
                    "price_date":   today_date,
                    "전월평균":     None,
                    "전주평균":     None,
                    "전일Official": prev_p,
                    "전일Closing":  prev_p,
                    "당일Official": today_p,
                    "당일Closing":  today_p,
                    "전일대비":     today_pct,
                }

        if not result:
            st.warning("하이메탈: 금속 데이터를 파싱하지 못했습니다.")
        return result

    except Exception as e:
        st.error(f"하이메탈 크롤링 오류: {e}")
        return {}


# ══════════════════════════════════════════════════════════
#  원유 가격 크롤링 (네이버 금융)
# ══════════════════════════════════════════════════════════
def fetch_oil_prices(pages: int = 1) -> list:
    results = []
    for oil_name, base_url in NAVER_OIL_URLS.items():
        for page in range(1, pages + 1):
            url = f"{base_url}&page={page}"
            try:
                res = requests.get(
                    url, headers=NAVER_HEADERS, timeout=15, verify=False
                )
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                tbl  = (
                    soup.find("table", class_="tbl_exchange") or
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

                    # 방향 감지
                    is_down = False
                    for td_check in tds[1:3]:
                        for tag in td_check.find_all(True):
                            cls = " ".join(tag.get("class", []))
                            if any(k in cls.lower() for k in ["down", "fall", "minus"]):
                                is_down = True
                    img = tds[1].find("img")
                    if img:
                        alt = img.get("alt", "")
                        src = img.get("src", "")
                        if "하락" in alt or "down" in src.lower():
                            is_down = True

                    pct = None
                    try:
                        pct_clean = pct_str.replace(",", "").replace("%", "").strip()
                        if pct_clean:
                            pct     = float(pct_clean)
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

                    if pct is None and chg is not None and price and price != 0:
                        prev_price = price - chg
                        if prev_price != 0:
                            pct = round(chg / prev_price * 100, 2)

                    date_clean = date_str.replace(".", "").replace(" ", "").strip()
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
        if r["품목"] not in latest:
            latest[r["품목"]] = r
    return latest


# ══════════════════════════════════════════════════════════
#  환율 크롤링 (네이버 금융)
# ══════════════════════════════════════════════════════════
def fetch_hana_usd_rate() -> dict | None:
    try:
        res = requests.get(
            NAVER_FX_URL, headers=NAVER_HEADERS, timeout=15, verify=False
        )
        res.raise_for_status()
    except Exception as e:
        st.warning(f"네이버 환율 요청 실패: {e}")
        return None

    soup = BeautifulSoup(res.text, "html.parser")

    def _parse_number(tag):
        if not tag:
            return None
        text = tag.get_text(strip=True).replace(",", "").replace("원", "").strip()
        try:
            v = float(text)
            return v if 900 < v < 2000 else None
        except Exception:
            return None

    rate = None
    no_today = soup.find("p", class_="no_today")
    if no_today:
        em = no_today.find("em")
        if em:
            rate = _parse_number(em)

    if not rate:
        text_all = soup.get_text()
        matches  = re.findall(r"1[0-3]\d{2}\.\d{2}", text_all)
        for m in matches:
            v = float(m)
            if 900 < v < 2000:
                rate = v
                break

    if not rate:
        for span in soup.find_all("span"):
            try:
                v = float(span.get_text(strip=True).replace(",", ""))
                if 900 < v < 2000:
                    rate = v
                    break
            except Exception:
                pass

    if not rate:
        st.warning("네이버 환율 파싱 실패")
        return None

    chg     = None
    is_down = False
    no_exday = soup.find("p", class_="no_exday")
    if no_exday:
        ico = no_exday.find("span", class_="ico")
        if ico and "down" in " ".join(ico.get("class", [])):
            is_down = True
        em_chg = no_exday.find("em")
        if em_chg:
            try:
                chg = float(em_chg.get_text(strip=True).replace(",", "").replace("원", ""))
                chg = -abs(chg) if is_down else abs(chg)
            except Exception:
                pass

    pct = None
    if chg is not None and rate and rate != 0:
        prev = rate - chg
        if prev != 0:
            pct = round(chg / prev * 100, 2)

    return {
        "당일Official": None,
        "당일Closing":  rate,
        "전일대비":     chg,
        "전일대비pct":  pct,
    }


# ══════════════════════════════════════════════════════════
#  Google Sheets
# ══════════════════════════════════════════════════════════
@st.cache_resource
def get_gsheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    gcp_env = os.environ.get("GCP_SERVICE_ACCOUNT")
    if gcp_env:
        service_account_info = json.loads(gcp_env)
        spreadsheet_id       = os.environ.get("SPREADSHEET_ID", "")
        worksheet_name       = os.environ.get("WORKSHEET_NAME", "Sheet1")
    else:
        try:
            service_account_info = dict(st.secrets["gcp_service_account"])
            spreadsheet_id       = st.secrets["sheets"]["spreadsheet_id"]
            worksheet_name       = st.secrets["sheets"]["worksheet_name"]
        except Exception:
            st.error("❌ GCP 인증 정보가 없습니다. Secrets 설정을 확인하세요.")
            st.stop()

    creds  = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(spreadsheet_id)
    try:
        ws = sheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=worksheet_name, rows=10000, cols=20)
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
    df_existing = pd.DataFrame()
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
                    st.info("ℹ️ 오늘 데이터가 이미 저장되어 있습니다.")
                    return df_existing

        new_rows = []
        for item, vals in data.items():
            row = {"날짜": price_date, "품목": item}
            row.update(vals)
            new_rows.append(row)

        if not new_rows:
            return df_existing

        df_new         = pd.DataFrame(new_rows)
        df_new["날짜"] = pd.to_datetime(
            df_new["날짜"], format="%Y%m%d", errors="coerce"
        )
        df_new = df_new.dropna(subset=["날짜"])

        ws            = get_gsheet()
        existing_data = ws.get_all_records()
        COLS = ["날짜", "품목", "전월평균", "전주평균", "전일Official",
                "전일Closing", "당일Official", "당일Closing", "전일대비"]
        if not existing_data:
            ws.append_row(COLS)

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
#  통계 계산
# ══════════════════════════════════════════════════════════
def calc_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    today      = df["날짜"].max()
    this_month = today.to_period("M")
    last_month = this_month - 1
    results    = []

    for item in METALS + OILS + ["환율"]:
        sub = df[df["품목"] == item].copy()
        if sub.empty:
            continue

        price_col = "당일Closing"
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
        if pd.isna(latest_price):
            latest_price = None

        chg_val = latest.get("전일대비")
        if chg_val is None or (isinstance(chg_val, float) and pd.isna(chg_val)):
            sorted_sub = sub.sort_values("날짜")[price_col].dropna()
            if len(sorted_sub) >= 2:
                cur  = sorted_sub.iloc[-1]
                prev = sorted_sub.iloc[-2]
                if prev != 0:
                    chg_val = round((cur - prev) / prev * 100, 2)

        basis = "LME Closing (USD/ton)" if item in METALS else (
            "USD/bbl" if item in OILS else "현물종가 (KRW)"
        )

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
#  시황 코멘트 / 색상
# ══════════════════════════════════════════════════════════
def generate_comment(stats_df: pd.DataFrame) -> str:
    if stats_df.empty:
        return "데이터 없음"

    today_str = datetime.now().strftime("%Y년 %m월 %d일")
    lines     = [f"**📋 {today_str} 비철금속·원유 시황 요약**\n"]
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

        if chg_f is None:
            flat.append(item)
        elif chg_f > 0:
            up.append(f"{item}({chg_f:+.2f}%)")
        elif chg_f < 0:
            dn.append(f"{item}({chg_f:+.2f}%)")
        else:
            flat.append(item)

        try:
            mom_f = float(mom) if mom is not None and pd.notna(mom) else None
            if mom_f and abs(mom_f) >= 3:
                direction = "상승" if mom_f > 0 else "하락"
                big.append(f"{item} 전월대비 {abs(mom_f):.1f}% {direction}")
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
        chg_s = f"{float(fx['전일대비(%)']):+.4f}"      if pd.notna(fx.get('전일대비(%)'))    else "-"
        mom_s = f"{float(fx['전월대비변동(%)']):+.2f}%" if pd.notna(fx.get('전월대비변동(%)')) else "-"
        lines.append(
            f"\n💱 **환율(KRW/USD):** {fx_s}원 "
            f"(전일대비 {chg_s}원 / 전월대비 {mom_s})"
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
    "비철금속: 하이메탈 LME Closing | "
    "원유/환율: 네이버 금융 | Google Sheets 누적 저장"
)

col_btn, col_info = st.columns([2, 4])
with col_btn:
    refresh = st.button("🔄 오늘 데이터 수집 & 저장", use_container_width=True)
with col_info:
    st.info(
        f"현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  "
        "※ LME 클로징 기준 | 매 영업일 1회 수집을 권장합니다."
    )


# ── 데이터 수집 ───────────────────────────────────────────
if refresh:

    # ① 비철금속
    with st.spinner("LME 클로징 가격 수집 중 (하이메탈)..."):
        metal_data = fetch_nonferrous_lme()

    if not metal_data:
        st.error("❌ 비철금속 수집 실패")
        st.stop()

    price_date = list(metal_data.values())[0].get(
        "price_date", datetime.now().strftime("%Y%m%d")
    )
    clean_metal = {
        k: {fk: fv for fk, fv in v.items() if fk != "price_date"}
        for k, v in metal_data.items()
    }
    st.success(
        f"✅ 비철금속 {len(metal_data)}종 수신 "
        f"(기준일: {price_date[:4]}-{price_date[4:6]}-{price_date[6:]})"
    )

    with st.expander("📋 비철금속 수신 데이터 미리보기", expanded=True):
        preview_rows = [
            {
                "품목":        k,
                "당일Closing": v["당일Closing"],
                "전일Closing": v["전일Closing"],
                "전일대비(%)": v["전일대비"],
            }
            for k, v in clean_metal.items()
        ]
        st.table(pd.DataFrame(preview_rows))

    combined = dict(clean_metal)

    # ② 원유
    with st.spinner("원유 가격 수집 중 (네이버 금융)..."):
        oil_rows = fetch_oil_prices(pages=1)

    oil_latest_by_name = {}
    for oil_row in oil_rows:
        name = oil_row["품목"]
        if name not in oil_latest_by_name:
            oil_latest_by_name[name] = oil_row

    for oil_name, oil_row in oil_latest_by_name.items():
        combined[oil_name] = {
            "전월평균":     None,
            "전주평균":     None,
            "전일Official": None,
            "전일Closing":  None,
            "당일Official": None,
            "당일Closing":  oil_row["당일Closing"],
            "전일대비":     oil_row.get("전일대비pct"),
        }

    if oil_latest_by_name:
        st.success(f"✅ 원유 {len(oil_latest_by_name)}종 수신")

    # ③ 환율
    with st.spinner("환율 수집 중 (네이버 금융)..."):
        hana = fetch_hana_usd_rate()

    if hana:
        combined["환율"] = {
            "전월평균":     None,
            "전주평균":     None,
            "전일Official": None,
            "전일Closing":  None,
            "당일Official": None,
            "당일Closing":  hana.get("당일Closing"),
            "전일대비":     hana.get("전일대비pct"),
        }
        st.success(f"✅ 환율 수신: {hana.get('당일Closing'):,.2f}원")

    # ④ 저장
    with st.spinner("Google Sheets 저장 중..."):
        save_to_gsheet(price_date, combined)

    st.success("✅ Google Sheets 저장 완료!")
    st.cache_data.clear()


# ── Sheets 로드 ───────────────────────────────────────────
df_all = load_gsheet()

if df_all.empty:
    st.warning("⚠️ 저장된 데이터가 없습니다. 상단 버튼으로 오늘 데이터를 수집하세요!")
    st.stop()

stats_df = calc_stats(df_all)


# ── 탭 ───────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📌 오늘 시황", "📈 추이 차트", "📊 통계 분석"])


# ════════════════════════════════
# TAB 1 – 오늘 시황
# ════════════════════════════════
with tab1:
    st.markdown(generate_comment(stats_df))
    st.divider()

    # 비철금속 지표
    st.subheader("💡 비철금속 LME Closing (USD/ton)")
    metal_stats = stats_df[stats_df["품목"].isin(METALS)]
    if not metal_stats.empty:
        cols = st.columns(len(METALS))
        for i, (_, row) in enumerate(metal_stats.iterrows()):
            price = row.get("최신가")
            chg   = row.get("전일대비(%)")
            try:
                delta_str   = f"{float(chg):+.2f}%" if pd.notna(chg) else "-"
                delta_color = "normal" if pd.notna(chg) else "off"
            except Exception:
                delta_str, delta_color = "-", "off"
            cols[i].metric(
                label=row["품목"],
                value=f"${float(price):,.2f}" if pd.notna(price) else "-",
                delta=delta_str,
                delta_color=delta_color,
            )
    else:
        st.info("비철금속 데이터가 없습니다.")

    st.divider()

    # 원유 지표
    st.subheader("🛢️ 국제유가 (USD/bbl)")
    oil_stats = stats_df[stats_df["품목"].isin(OILS)]
    oil_cols  = st.columns(len(OILS))
    for i, oil_name in enumerate(OILS):
        row = oil_stats[oil_stats["품목"] == oil_name]
        if not row.empty:
            r       = row.iloc[0]
            o_price = r.get("최신가")
            o_chg   = r.get("전일대비(%)")
            try:
                delta_str   = f"{float(o_chg):+.2f}%" if pd.notna(o_chg) else "-"
                delta_color = "normal" if pd.notna(o_chg) else "off"
            except Exception:
                delta_str, delta_color = "-", "off"
            oil_cols[i].metric(
                label=oil_name,
                value=f"${float(o_price):,.2f}" if pd.notna(o_price) else "-",
                delta=delta_str,
                delta_color=delta_color,
            )
        else:
            oil_cols[i].metric(label=oil_name, value="-", delta="-", delta_color="off")

    if not oil_stats.empty:
        st.markdown("**월간 비교**")
        oc1, oc2 = st.columns(2)
        for idx, (_, row) in enumerate(oil_stats.iterrows()):
            tgt      = oc1 if idx == 0 else oc2
            avg_this = row.get("당월누적평균")
            avg_last = row.get("전월평균")
            mom      = row.get("전월대비변동(%)")
            tgt.markdown(
                f"**{row['품목']}** · 당월평균 "
                f"{'${:,.2f}'.format(float(avg_this)) if pd.notna(avg_this) else '-'} · "
                f"전월평균 "
                f"{'${:,.2f}'.format(float(avg_last)) if pd.notna(avg_last) else '-'} · "
                f"변동 "
                f"{'{:+.2f}%'.format(float(mom)) if pd.notna(mom) else '-'}"
            )

    st.divider()

    # 전월대비 테이블
    st.subheader("📋 전월대비 분석 테이블")
    display_cols = ["품목", "최신가", "가격기준", "전일대비(%)",
                    "당월누적평균", "전월평균", "전월대비변동(%)", "기준일"]
    display_cols = [c for c in display_cols if c in stats_df.columns]
    style_cols   = [c for c in ["전일대비(%)", "전월대비변동(%)"] if c in display_cols]
    fmt_dict     = {}
    for c, f in [("최신가", "{:,.2f}"), ("전일대비(%)", "{:+.2f}%"),
                 ("당월누적평균", "{:,.2f}"), ("전월평균", "{:,.2f}"),
                 ("전월대비변동(%)", "{:+.2f}%")]:
        if c in display_cols:
            fmt_dict[c] = f

    styled = stats_df[display_cols].style.format(fmt_dict, na_rep="-")
    if style_cols:
        styled = styled.map(color_val, subset=style_cols)
    st.dataframe(styled, use_container_width=True, hide_index=True)

    st.divider()

    # 환율
    st.subheader("💱 환율 (KRW/USD)")
    fx_stats  = stats_df[stats_df["품목"] == "환율"]
    hana_live = fetch_hana_usd_rate()
    if hana_live:
        live_rate = hana_live.get("당일Closing")
        live_chg  = hana_live.get("전일대비")
        avg_this  = fx_stats.iloc[0].get("당월누적평균")    if not fx_stats.empty else None
        avg_last  = fx_stats.iloc[0].get("전월평균")        if not fx_stats.empty else None
        mom_chg   = fx_stats.iloc[0].get("전월대비변동(%)") if not fx_stats.empty else None

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(
            "💱 환율 (실시간)",
            f"{live_rate:,.2f}원" if live_rate else "-",
            delta=f"{live_chg:+.2f}원" if live_chg else None,
        )
        c2.metric("당월 누적 평균",
                  f"{float(avg_this):,.2f}원" if avg_this and pd.notna(avg_this) else "-")
        c3.metric("전월 평균",
                  f"{float(avg_last):,.2f}원" if avg_last and pd.notna(avg_last) else "-")
        c4.metric("전월대비 변동",
                  f"{float(mom_chg):+.2f}%" if mom_chg and pd.notna(mom_chg) else "-")
    else:
        st.warning("⚠️ 환율 실시간 조회 실패")


# ════════════════════════════════
# TAB 2 – 추이 차트
# ════════════════════════════════
with tab2:
    st.subheader("📈 비철금속 가격 추이 (USD/ton)")
    selected = st.multiselect(
        "품목 선택", options=METALS, default=["구리", "알루미늄", "니켈"]
    )
    period = st.radio(
        "기간", ["1개월", "3개월", "전체"], horizontal=True, key="period_metal"
    )
    today_dt = df_all["날짜"].max()

    if selected:
        df_chart = df_all[df_all["품목"].isin(selected)].copy()
        if period == "1개월":
            df_chart = df_chart[df_chart["날짜"] >= today_dt - pd.Timedelta(days=30)]
        elif period == "3개월":
            df_chart = df_chart[df_chart["날짜"] >= today_dt - pd.Timedelta(days=90)]
        pivot = df_chart.pivot_table(index="날짜", columns="품목", values="당일Closing")
        if not pivot.empty:
            st.line_chart(pivot, use_container_width=True)

    st.divider()
    st.subheader("🛢️ 국제유가 추이 (USD/bbl)")
    period_oil   = st.radio(
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
    st.subheader("💱 환율 추이 (KRW/USD)")
    df_fx = df_all[df_all["품목"] == "환율"].set_index("날짜")[["당일Closing"]].copy()
    df_fx.columns = ["환율(KRW/USD)"]
    if period == "1개월":
        df_fx = df_fx[df_fx.index >= today_dt - pd.Timedelta(days=30)]
    elif period == "3개월":
        df_fx = df_fx[df_fx.index >= today_dt - pd.Timedelta(days=90)]
    if not df_fx.empty:
        st.line_chart(df_fx, use_container_width=True)


# ════════════════════════════════
# TAB 3 – 통계 분석
# ════════════════════════════════
with tab3:
    st.subheader("📊 월별 평균가 비교")
    item_sel = st.selectbox("품목 선택", METALS + OILS + ["환율"])
    df_item  = df_all[df_all["품목"] == item_sel].copy()
    df_item["월"] = df_item["날짜"].dt.to_period("M").astype(str)

    if item_sel in METALS:
        val_col, label = "당일Closing", "USD/ton"
    elif item_sel in OILS:
        val_col, label = "당일Closing", "USD/bbl"
    else:
        val_col, label = "당일Closing", "현물종가(KRW)"

    monthly = df_item.groupby("월")[val_col].mean().reset_index()
    monthly.columns = ["월", f"월평균({label})"]
    monthly[f"월평균({label})"] = monthly[f"월평균({label})"].round(2)
    monthly["전월대비(%)"] = (
        monthly[f"월평균({label})"].pct_change().mul(100).round(2)
    )

    if not monthly.empty:
        st.bar_chart(
            monthly.set_index("월")[f"월평균({label})"],
            use_container_width=True
        )
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
    "📌 LME Closing 기준 | 출처: 하이메탈(highmetal.co.kr) | "
    "🛢️ 원유: 네이버 금융 | 💱 환율: 네이버 금융 | 비상업적 참고용"
)
