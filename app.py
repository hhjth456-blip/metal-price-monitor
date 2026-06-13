import streamlit as st
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
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

# ══════════════════════════════════════════════════════════
#  야후 파이낸스 티커 매핑 및 단위 정의
#
#  [티커 검증 결과 - 2026.06 기준]
#  HG=F  : 구리   COMEX  → USD/lb    → ×2204.62 = USD/ton ✅
#  ALI=F : 알루미늄 COMEX → USD/ton  → 그대로 사용 ✅
#  PB=F  : 납     COMEX  → USD/lb    → ×2204.62 = USD/ton ✅
#  NI=F  : 니켈   CME    → USD/lb    → ×2204.62 = USD/ton ✅
#
#  [야후 파이낸스에 직접 선물 없는 품목 → 대체 ETF/인덱스 활용]
#  ZN=F  : ❌ 10년 국채 선물 (아연 아님!)
#           → ZINC.L (WisdomTree Zinc ETP, USD/ton 기준)
#  SN=F  : ❌ 존재하지 않는 티커
#  SB=F  : ❌ Sugar #11 선물 (주석 아님!)
#           → 주석은 야후 선물 없음 → 하이메탈 HTML 보조 크롤링으로 수집
#
#  단위(unit):
#    "lb"      → USD/lb  → ×2204.62 → USD/ton
#    "ton"     → USD/ton → 그대로
#    "p_ton"   → 영국 펜스/ton (GBX) → ÷100 → GBP/ton → ×USD/GBP 환율
#    "no_data" → 야후 데이터 없음, 별도 크롤링으로 보완
# ══════════════════════════════════════════════════════════
METAL_TICKERS = {
    "구리":    ("HG=F",  "lb"),       # COMEX USD/lb → ×2204.62 → USD/ton
    "알루미늄": ("ALI=F", "ton"),      # COMEX USD/ton 직접 사용
    "납":      ("PB=F",  "lb"),       # COMEX USD/lb → ×2204.62 → USD/ton
    "니켈":    ("NI=F",  "lb"),       # CME   USD/lb → ×2204.62 → USD/ton
    "아연":    ("ZINC.L","p_ton"),    # LSE WisdomTree ETP GBX/ton → ÷100 → GBP/ton → ×GBPUSD
    "주석":    (None,    "no_data"),  # 야후 선물 없음 → 하이메탈 보조 크롤링
}

# GBP/USD 환율 티커
GBPUSD_TICKER = "GBPUSD=X"

NAVER_OIL_URLS = {
    "WTI":    "https://finance.naver.com/marketindex/worldDailyQuote.naver"
              "?marketindexCd=OIL_CL&fdtc=2",
    "브렌트유": "https://finance.naver.com/marketindex/worldDailyQuote.naver"
              "?marketindexCd=OIL_BRT&fdtc=2",
}
NAVER_FX_URL = (
    "https://finance.naver.com/marketindex/exchangeDetail.naver"
    "?marketindexCd=FX_USDKRW"
)
HIGHMETAL_URL = "https://highmetal.co.kr/"

NAVER_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://finance.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}
HIGHMETAL_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://highmetal.co.kr/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

# ══════════════════════════════════════════════════════════
#  단위 환산 헬퍼
# ══════════════════════════════════════════════════════════
def _lb_to_ton(price: float) -> float:
    """USD/lb → USD/metric ton (×2204.62)"""
    return round(price * 2204.62, 2)


def _gbx_to_usd(price_gbx: float, gbpusd: float) -> float:
    """
    GBX(펜스)/ton → USD/ton
    GBX ÷ 100 → GBP/ton → × GBP/USD 환율 → USD/ton
    """
    return round((price_gbx / 100) * gbpusd, 2)


@st.cache_data(ttl=3600)
def _fetch_gbpusd(days: int = 40) -> pd.Series:
    """GBP/USD 환율 시계열 (날짜별)"""
    end_dt   = datetime.today()
    start_dt = end_dt - timedelta(days=days + 10)
    try:
        df = yf.download(
            GBPUSD_TICKER,
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_dt.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
        if df.empty:
            return pd.Series(dtype=float)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index).tz_localize(None)
        return df["Close"].sort_index()
    except Exception:
        return pd.Series(dtype=float)


# ══════════════════════════════════════════════════════════
#  주석(Sn) 보조 크롤링: 하이메탈
# ══════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def fetch_tin_highmetal() -> dict:
    """
    하이메탈에서 주석(Sn) 가격만 추출
    반환: {"YYYYMMDD": {"당일Closing": ..., "전일Closing": ..., "전일대비": ...}}
    """
    import copy
    try:
        resp = requests.get(
            HIGHMETAL_URL, headers=HIGHMETAL_HEADERS, timeout=15, verify=False
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 날짜 파싱
        dates = []
        for th in soup.find_all("th"):
            m = re.search(r"(\d{2})/(\d{2})", th.get_text(strip=True))
            if m:
                year = datetime.now().strftime("%Y")
                dates.append(f"{year}{m.group(1)}{m.group(2)}")
        today_date = dates[0] if dates else datetime.now().strftime("%Y%m%d")

        def _parse_price(td):
            try:
                for p in td.find_all("p"):
                    p.decompose()
                raw = td.get_text(strip=True).replace(",", "")
                return float(raw)
            except Exception:
                return None

        def _parse_pct(td):
            try:
                p_tag = td.find("p")
                if not p_tag:
                    return None
                raw = p_tag.get_text(strip=True)
                m   = re.search(r"(-?[\d.]+)%", raw)
                return float(m.group(1)) if m else None
            except Exception:
                return None

        result = {}
        for tr in soup.find_all("tr"):
            name_td   = tr.find("td", class_="name")
            price_tds = tr.find_all("td", class_="price")
            if not name_td or len(price_tds) < 2:
                continue
            lan_ko = name_td.find("p", class_="lan_Ko")
            if not lan_ko:
                continue
            if lan_ko.get_text(strip=True) != "주석":
                continue

            td1 = copy.copy(price_tds[0])
            td2 = copy.copy(price_tds[1])

            today_p   = _parse_price(td1)
            today_pct = _parse_pct(td1)
            prev_p    = _parse_price(td2)

            if today_p:
                result[today_date] = {
                    "당일Closing":  today_p,
                    "전일Closing":  prev_p,
                    "전일대비":     today_pct,
                }
            break  # 주석 행만 필요

        return result
    except Exception as e:
        st.warning(f"주석 하이메탈 크롤링 오류: {e}")
        return {}


# ══════════════════════════════════════════════════════════
#  yfinance 비철금속 가격 수집 (단위 환산 포함)
# ══════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def fetch_metals_yfinance(days: int = 30) -> dict:
    """
    야후 파이낸스로 비철금속 선물 가격 수집 + 단위 환산
    반환:
      {
        "YYYYMMDD": {
          "구리":    {"당일Official": ..., "당일Closing": ...,
                      "전일Closing": ..., "전일대비": ..., ...},
          "알루미늄": {...},
          ...
        },
        ...
      }

    단위 환산 기준:
      - HG=F  (구리)   : USD/lb  × 2204.62 → USD/ton
      - PB=F  (납)     : USD/lb  × 2204.62 → USD/ton
      - NI=F  (니켈)   : USD/lb  × 2204.62 → USD/ton
      - ALI=F (알루미늄): USD/ton 그대로
      - ZINC.L(아연)   : GBX/ton ÷ 100 × GBP/USD → USD/ton
      - 주석            : 하이메탈 보조 크롤링 (LME USD/ton)
    """
    end_dt   = datetime.today()
    start_dt = end_dt - timedelta(days=days + 10)

    result: dict[str, dict] = {}

    # GBP/USD 환율 미리 수집 (아연 환산에 필요)
    gbpusd_series = _fetch_gbpusd(days=days + 10)

    for metal, (ticker, unit) in METAL_TICKERS.items():
        # 주석은 야후 데이터 없음 → 별도 처리
        if unit == "no_data" or ticker is None:
            continue

        try:
            df = yf.download(
                ticker,
                start=start_dt.strftime("%Y-%m-%d"),
                end=end_dt.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=True,
            )
            if df.empty:
                st.warning(f"⚠️ {metal}({ticker}): 야후 데이터 없음")
                continue

            # MultiIndex 정리
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df[["Close"]].copy()
            df.index = pd.to_datetime(df.index).tz_localize(None)
            df = df.sort_index()

            # ── 단위 환산 ──────────────────────────────────
            if unit == "lb":
                # USD/lb → USD/ton
                df["Close"] = df["Close"].apply(_lb_to_ton)

            elif unit == "p_ton":
                # GBX/ton → USD/ton (날짜별 GBP/USD 적용)
                def _convert_gbx(row):
                    date_ts = row.name
                    # 해당 날짜 GBP/USD 조회 (없으면 가장 가까운 이전 날짜)
                    if gbpusd_series.empty:
                        return None
                    available = gbpusd_series[gbpusd_series.index <= date_ts]
                    if available.empty:
                        return None
                    gbpusd = float(available.iloc[-1])
                    return _gbx_to_usd(float(row["Close"]), gbpusd)

                df["Close"] = df.apply(_convert_gbx, axis=1)
                df = df.dropna(subset=["Close"])

            # unit == "ton" → 그대로 사용

            df["Prev"]   = df["Close"].shift(1)
            df["ChgPct"] = (
                (df["Close"] - df["Prev"]) / df["Prev"] * 100
            ).round(2)

            for ts, row in df.iterrows():
                date_str = ts.strftime("%Y%m%d")
                price    = row["Close"]
                prev     = row["Prev"]
                chg      = row["ChgPct"]

                if pd.isna(price):
                    continue

                if date_str not in result:
                    result[date_str] = {}

                result[date_str][metal] = {
                    "전월평균":     None,
                    "전주평균":     None,
                    "전일Official": None,
                    "전일Closing":  None if pd.isna(prev) else round(float(prev), 2),
                    "당일Official": round(float(price), 2),
                    "당일Closing":  round(float(price), 2),
                    "전일대비":     None if pd.isna(chg) else float(chg),
                }

        except Exception as e:
            st.warning(f"yfinance {metal}({ticker}) 오류: {e}")
            continue

    # ── 주석 보조 데이터 병합 ──────────────────────────────
    tin_data = fetch_tin_highmetal()
    for date_str, tin_vals in tin_data.items():
        if date_str not in result:
            result[date_str] = {}
        result[date_str]["주석"] = {
            "전월평균":     None,
            "전주평균":     None,
            "전일Official": None,
            "전일Closing":  tin_vals.get("전일Closing"),
            "당일Official": tin_vals.get("당일Closing"),
            "당일Closing":  tin_vals.get("당일Closing"),
            "전일대비":     tin_vals.get("전일대비"),
        }

    return result


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
                            if any(k in cls.lower() for k in ["down", "fall", "minus"]):
                                is_down = True
                    img = tds[1].find("img")
                    if img:
                        alt = img.get("alt", "")
                        src = img.get("src", "")
                        if "하락" in alt or "down" in src.lower():
                            is_down = True
                    try:
                        pct     = float(pct_str.replace(",", "").replace("%", ""))
                        is_down = pct < 0 if pct != 0 else is_down
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

    def _parse_split_number(tag):
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
            rate = _parse_split_number(em.find("em") or em)

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
        st.warning(f"네이버 환율 파싱 실패 (rate={rate})")
        return None

    chg = None
    no_exday = soup.find("p", class_="no_exday")
    if no_exday:
        chg = _parse_split_number(no_exday.find("em"))
        ico = no_exday.find("span", class_="ico")
        if ico and "down" in ico.get("class", []):
            chg = -abs(chg) if chg is not None else chg

    return {"당일Official": None, "당일Closing": rate, "전일대비": chg}


# ══════════════════════════════════════════════════════════
#  Google Sheets
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
                    return df_existing

        new_rows = []
        for item, vals in data.items():
            row = {"날짜": price_date, "품목": item}
            row.update(vals)
            new_rows.append(row)

        if not new_rows:
            return df_existing

        df_new = pd.DataFrame(new_rows)
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

        price_col = "당일Official" if item in METALS else "당일Closing"
        if price_col not in sub.columns:
            continue

        sub["월"]  = sub["날짜"].dt.to_period("M")
        this_m     = sub[sub["월"] == this_month][price_col].dropna()
        last_m     = sub[sub["월"] == last_month][price_col].dropna()
        avg_this   = round(this_m.mean(), 2) if not this_m.empty else None
        avg_last   = round(last_m.mean(), 2) if not last_m.empty else None
        chg_pct    = None
        if avg_this and avg_last and avg_last != 0:
            chg_pct = round((avg_this - avg_last) / avg_last * 100, 2)

        latest       = sub.sort_values("날짜").iloc[-1]
        latest_price = latest.get(price_col)
        chg_val      = latest.get("전일대비")
        if isinstance(chg_val, float) and pd.isna(chg_val):
            chg_val = None

        # 가격기준 표시 (출처 명시)
        if item == "구리":
            basis = "COMEX HG=F (USD/lb→ton)"
        elif item == "알루미늄":
            basis = "COMEX ALI=F (USD/ton)"
        elif item == "납":
            basis = "COMEX PB=F (USD/lb→ton)"
        elif item == "니켈":
            basis = "CME NI=F (USD/lb→ton)"
        elif item == "아연":
            basis = "LSE ZINC.L (GBX→USD/ton)"
        elif item == "주석":
            basis = "LME via 하이메탈 (USD/ton)"
        elif item in OILS:
            basis = "USD/bbl (네이버금융)"
        else:
            basis = "현물종가 (네이버금융)"

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
        fx_s  = f"{float(fx['최신가']):,.2f}"          if pd.notna(fx['최신가'])          else "-"
        chg_s = f"{float(fx['전일대비(%)']):+.2f}%"    if pd.notna(fx['전일대비(%)'])    else "-"
        mom_s = f"{float(fx['전월대비변동(%)']):+.2f}%" if pd.notna(fx['전월대비변동(%)']) else "-"
        lines.append(
            f"\n💱 **환율(KRW/USD):** {fx_s} "
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
    "비철금속: Yahoo Finance COMEX/CME/LSE 선물 (USD/ton 환산) + 주석: 하이메탈(LME) | "
    "원유/환율: 네이버 금융 | Google Sheets 누적 저장"
)

# 단위 환산 안내 뱃지
with st.expander("📐 가격 단위 환산 기준 안내", expanded=False):
    st.markdown("""
| 품목 | 티커 | 원래 단위 | 환산 방식 | 최종 단위 |
|------|------|-----------|-----------|-----------|
| 구리 | HG=F (COMEX) | USD/lb | × 2,204.62 | **USD/ton** |
| 알루미늄 | ALI=F (COMEX) | USD/ton | 그대로 | **USD/ton** |
| 납 | PB=F (COMEX) | USD/lb | × 2,204.62 | **USD/ton** |
| 니켈 | NI=F (CME) | USD/lb | × 2,204.62 | **USD/ton** |
| 아연 | ZINC.L (LSE) | GBX/ton | ÷ 100 × GBP/USD | **USD/ton** |
| 주석 | 하이메탈 (LME) | USD/ton | 그대로 | **USD/ton** |
| WTI | 네이버금융 | USD/bbl | 그대로 | **USD/bbl** |
| 브렌트유 | 네이버금융 | USD/bbl | 그대로 | **USD/bbl** |
""")

col_btn, col_upd, col_info = st.columns([1, 1, 4])
with col_btn:
    refresh = st.button("🔄 오늘 데이터 수집", use_container_width=True)
with col_upd:
    bulk = st.button("📥 전체 데이터 수집", use_container_width=True)
with col_info:
    st.info(f"현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ── 데이터 수집 ───────────────────────────────────────────
if refresh or bulk:
    # 캐시 초기화
    fetch_metals_yfinance.clear()
    _fetch_gbpusd.clear()
    fetch_tin_highmetal.clear()

    days = 365 if bulk else 7

    with st.spinner("Yahoo Finance에서 비철금속 선물 데이터 수집 중..."):
        parsed = fetch_metals_yfinance(days=days)

    if not parsed:
        st.error("❌ Yahoo Finance 데이터 수집 실패. 잠시 후 다시 시도하세요.")
    else:
        # 수집 결과 미리보기
        latest_date = max(parsed.keys())
        preview_data = parsed.get(latest_date, {})
        if preview_data:
            with st.expander(f"📋 비철금속 수신 데이터 미리보기 ({latest_date})", expanded=True):
                preview_rows = []
                for metal in METALS:
                    vals = preview_data.get(metal, {})
                    ticker_info = METAL_TICKERS.get(metal, (None, None))
                    preview_rows.append({
                        "품목":        metal,
                        "티커":        ticker_info[0] if ticker_info[0] else "하이메탈",
                        "당일Closing": vals.get("당일Closing", "-"),
                        "전일Closing": vals.get("전일Closing", "-"),
                        "전일대비(%)": vals.get("전일대비", "-"),
                        "단위":        "USD/ton",
                    })
                st.table(pd.DataFrame(preview_rows))

        st.success(f"✅ {len(parsed)}일치 비철금속 데이터 수신")

        with st.spinner("원유 가격 수집 중 (네이버 금융)..."):
            oil_pages = 3 if bulk else 1
            oil_rows  = fetch_oil_prices(pages=oil_pages)

        with st.spinner("환율 수집 중 (네이버 금융)..."):
            hana = fetch_hana_usd_rate()

        progress    = st.progress(0)
        date_list   = sorted(parsed.keys(), reverse=True)
        saved_count = 0

        for i, date_str in enumerate(date_list):
            data = parsed[date_str]

            # 원유 추가
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

            # 환율 추가 (최신 날짜만)
            if i == 0 and hana and "환율" not in data:
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
            progress.progress(
                (i + 1) / len(date_list),
                text=f"저장 중... {date_str} ({i+1}/{len(date_list)})"
            )
            time.sleep(0.05)

        progress.empty()
        st.success(f"✅ {saved_count}일치 Google Sheets 저장 완료!")
        st.cache_data.clear()


# ── Sheets 로드 ───────────────────────────────────────────
df_all = load_gsheet()

if df_all.empty:
    st.warning("⚠️ 저장된 데이터가 없습니다. 상단 버튼으로 데이터를 수집하세요!")
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

    st.subheader("💡 비철금속 가격 (USD/ton 환산 통일)")
    metal_stats = stats_df[stats_df["품목"].isin(METALS)]
    cols = st.columns(len(METALS))
    for i, metal_name in enumerate(METALS):
        row_df = metal_stats[metal_stats["품목"] == metal_name]
        if row_df.empty:
            cols[i].metric(label=metal_name, value="-", delta="-", delta_color="off")
            continue
        row   = row_df.iloc[0]
        price = row.get("최신가")
        chg   = row.get("전일대비(%)")
        try:
            delta_str   = f"{float(chg):+.2f}%" if chg is not None and pd.notna(chg) else "-"
            delta_color = "normal" if chg is not None and pd.notna(chg) else "off"
        except Exception:
            delta_str, delta_color = "-", "off"
        cols[i].metric(
            label=f"{metal_name}",
            value=f"${float(price):,.0f}" if pd.notna(price) and price is not None else "-",
            delta=delta_str,
            delta_color=delta_color,
            help=row.get("가격기준", ""),   # 툴팁으로 출처 표시
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
# TAB 2 – 추이 차트
# ════════════════════════════════
with tab2:
    st.subheader("📈 비철금속 가격 추이 (USD/ton)")
    selected = st.multiselect(
        "품목 선택", options=METALS, default=["구리", "알루미늄", "니켈"]
    )
    period   = st.radio(
        "기간", ["1개월", "3개월", "전체"], horizontal=True, key="period_metal"
    )
    today_dt = df_all["날짜"].max()

    if selected:
        df_chart = df_all[df_all["품목"].isin(selected)].copy()
        if period == "1개월":
            df_chart = df_chart[df_chart["날짜"] >= today_dt - pd.Timedelta(days=30)]
        elif period == "3개월":
            df_chart = df_chart[df_chart["날짜"] >= today_dt - pd.Timedelta(days=90)]
        pivot = df_chart.pivot_table(
            index="날짜", columns="품목", values="당일Official"
        )
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
        val_col, label = "당일Official", "USD/ton"
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
    "📌 구리/납/니켈: COMEX/CME USD/lb × 2,204.62 → USD/ton | "
    "알루미늄: COMEX ALI=F USD/ton 직접 사용 | "
    "아연: LSE ZINC.L GBX÷100×GBP/USD → USD/ton | "
    "주석: 하이메탈 LME USD/ton | "
    "🛢️ 원유/💱 환율: 네이버 금융 | 비상업적 참고용"
)
