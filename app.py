# ============================================================
#  app.py  ─  조달청 비철금속 & 국제유가 모니터
#  데이터 소스:
#    ① 비철금속 판매가격 → 공공데이터포털 Open API (무료, 안정)
#    ② LME 국제가격(USD) → 한국비철금속협회 테이블 크롤링
#    ③ WTI / 브렌트유    → 네이버 금융 크롤링
#    ④ 환율(KRW/USD)     → 네이버 금융 크롤링
#    ⑤ 누적 저장소       → Google Sheets
# ============================================================

import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time
import gspread
from google.oauth2.service_account import Credentials

# ── 페이지 설정 ───────────────────────────────────────────
st.set_page_config(
    page_title="비철금속·원유 시황 모니터",
    page_icon="📊",
    layout="wide"
)

# ── 상수 ─────────────────────────────────────────────────
METALS = ["알루미늄", "납", "아연", "구리", "주석", "니켈"]
OILS   = ["WTI", "브렌트유"]

# 공공데이터포털 API
PPS_API_URL = (
    "https://api.data.go.kr"
    "/15151568/v1/uddi:18394309-5202-4567-9914-ab9b3a05712c"
)

# 네이버 금융 원유 URL
NAVER_OIL_URLS = {
    "WTI":    "https://finance.naver.com/marketindex/worldDailyQuote.naver"
              "?marketindexCd=OIL_CL&fdtc=2",
    "브렌트유": "https://finance.naver.com/marketindex/worldDailyQuote.naver"
               "?marketindexCd=OIL_BRT&fdtc=2",
}

# 네이버 금융 환율 URL
NAVER_FX_URL = (
    "https://finance.naver.com/marketindex/exchangeDetail.naver"
    "?marketindexCd=FX_USDKRW"
)

# 한국비철금속협회 LME 시세 (USD/ton)
NONFERROUS_LME_URL = "https://www.nonferrous.or.kr/stats/?act=sub3"

# ── 공통 헤더 ─────────────────────────────────────────────
_HDR = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
}


# ══════════════════════════════════════════════════════════
#  1. 공공데이터포털 API ─ 조달청 원자재 판매가격(원화)
# ══════════════════════════════════════════════════════════
def fetch_pps_api(total_pages: int = 5) -> pd.DataFrame:
    """
    공공데이터포털 조달청 비축물자 원자재 일일가격 API
    반환: 날짜, 품목, 판매가격(원/톤), 판매지방청 포함 DataFrame
    """
    api_key = st.secrets["data_go_kr"]["api_key"]
    all_rows = []

    for page in range(1, total_pages + 1):
        try:
            res = requests.get(
                PPS_API_URL,
                params={
                    "serviceKey": api_key,
                    "page":       page,
                    "perPage":    100,
                    "returnType": "JSON",
                },
                timeout=15,
            )
            res.raise_for_status()
            body       = res.json()
            data       = body.get("data", [])
            total_cnt  = body.get("totalCount", 0)

            if not data:
                break
            all_rows.extend(data)
            if len(all_rows) >= total_cnt:
                break

        except Exception as e:
            st.warning(f"공공데이터 API 오류 (page={page}): {e}")
            break
        time.sleep(0.2)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # ── 첫 실행 시 컬럼 확인용 디버그 (자동으로 열림) ──
    if "api_cols_shown" not in st.session_state:
        with st.expander("🔍 [최초 1회] API 응답 원본 컬럼 확인", expanded=True):
            st.write("**컬럼 목록:**", df.columns.tolist())
            st.dataframe(df.head(5))
        st.session_state["api_cols_shown"] = True

    return _parse_pps_df(df)


# ── API 응답 → 내부 포맷 변환 ─────────────────────────────
# ※ 아래 FIELD_MAP의 키는 API 최초 실행 후 실제 컬럼명으로 수정하세요
FIELD_MAP = {
    # 실제 API 컬럼명       →  내부 키
    "품명":     "품목",       # 예) 알루미늄(서구산)
    "판매가격(부가세 포함)": "판매가격",   # 예) 3,410,000원/톤
    "판매지방청": "지역",
    "판매기간":  "날짜",       # 예) 2023.12.20  또는 20231220
    # 혹시 다른 필드명이면 아래 후보들도 추가
    # "prceDate": "날짜",
    # "prdlstNm": "품목",
    # "slPrc":    "판매가격",
    # "slRgn":    "지역",
    # "stdDt":    "날짜",
}

METAL_ALIAS = {
    "알루미늄": "알루미늄",
    "구리":     "구리",
    "아연":     "아연",
    "납":       "납",
    "주석":     "주석",
    "니켈":     "니켈",
}


def _parse_pps_df(df: pd.DataFrame) -> pd.DataFrame:
    """API 원본 DataFrame → [날짜, 품목, 판매가격_원] 정규화"""
    # 컬럼명 리네임 (있는 것만)
    rename = {k: v for k, v in FIELD_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    required = {"날짜", "품목", "판매가격"}
    missing  = required - set(df.columns)
    if missing:
        st.error(
            f"❌ API 필드 매핑 실패! 누락 컬럼: {missing}\n"
            "위 '컬럼 확인' 펼침에서 실제 컬럼명을 확인 후 FIELD_MAP을 수정하세요."
        )
        return pd.DataFrame()

    # 날짜 정규화 → YYYYMMDD 문자열
    def norm_date(v):
        s = str(v).replace(".", "").replace("-", "").strip()
        if len(s) == 8 and s.isdigit():
            return s
        return None

    df["날짜"] = df["날짜"].apply(norm_date)
    df = df.dropna(subset=["날짜"])

    # 품목명 정규화 → METALS 기준
    def norm_item(v):
        s = str(v).strip()
        for k in METAL_ALIAS:
            if k in s:
                return METAL_ALIAS[k]
        return s  # 매핑 안 되면 원본 유지

    df["품목"] = df["품목"].apply(norm_item)

    # 판매가격 숫자 추출 (예: "3,410,000원/톤" → 3410000.0)
    def norm_price(v):
        s = re.sub(r"[^\d.]", "", str(v))
        try:
            return float(s)
        except Exception:
            return None

    df["판매가격_원"] = df["판매가격"].apply(norm_price)

    return df[["날짜", "품목", "판매가격_원"] + (["지역"] if "지역" in df.columns else [])]


# ══════════════════════════════════════════════════════════
#  2. 한국비철금속협회 ─ LME 시세(USD/ton)
# ══════════════════════════════════════════════════════════
def fetch_lme_usd() -> pd.DataFrame:
    """
    한국비철금속협회 LME 시세 테이블 크롤링
    반환: [날짜, Cu, Al, Zn, Pb, Ni, Sn]  (USD/ton)
    """
    try:
        res = requests.get(
            NONFERROUS_LME_URL,
            headers={**_HDR, "Referer": "https://www.nonferrous.or.kr/"},
            timeout=15,
        )
        res.raise_for_status()
        soup  = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table", summary=lambda s: s and "LME" in s) or \
                soup.find("table")
        if not table:
            return pd.DataFrame()

        rows = []
        for tr in table.select("tbody tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 7:
                continue
            date_raw = tds[0].replace(".", "").replace(" ", "")
            if len(date_raw) != 8 or not date_raw.isdigit():
                continue

            def sf(v):
                try:
                    return float(v.replace(",", ""))
                except Exception:
                    return None

            rows.append({
                "날짜": date_raw,
                "Cu":   sf(tds[1]),
                "Al":   sf(tds[2]),
                "Zn":   sf(tds[3]),
                "Pb":   sf(tds[4]),
                "Ni":   sf(tds[5]),
                "Sn":   sf(tds[6]),
            })

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as e:
        st.warning(f"LME 시세 크롤링 실패: {e}")
        return pd.DataFrame()


# LME 컬럼 → 한글 품목 매핑
LME_COL_TO_METAL = {
    "Cu": "구리", "Al": "알루미늄", "Zn": "아연",
    "Pb": "납",   "Ni": "니켈",     "Sn": "주석",
}


# ══════════════════════════════════════════════════════════
#  3. 네이버 금융 ─ 원유 가격
# ══════════════════════════════════════════════════════════
def fetch_oil_prices(pages: int = 1) -> list:
    results = []
    for oil_name, base_url in NAVER_OIL_URLS.items():
        for page in range(1, pages + 1):
            url = f"{base_url}&page={page}"
            try:
                res  = requests.get(url, headers={**_HDR, "Referer": "https://finance.naver.com/"}, timeout=10)
                soup = BeautifulSoup(res.text, "html.parser")
                tbl  = soup.find("table") 
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

                    date_clean = date_str.replace(".", "").strip()
                    if len(date_clean) != 8:
                        continue

                    try:
                        price = float(price_str.replace(",", ""))
                    except Exception:
                        continue

                    # 등락 방향 판별
                    is_down = False
                    try:
                        pct = float(pct_str.replace(",", "").replace("%", ""))
                        is_down = pct < 0
                    except Exception:
                        pct = None
                    # img alt 보조
                    img = tds[1].find("img")
                    if img:
                        alt = img.get("alt", "")
                        if "하락" in alt:
                            is_down = True

                    try:
                        chg = float(chg_str.replace(",", ""))
                        chg = -abs(chg) if is_down else abs(chg)
                    except Exception:
                        chg = None

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
#  4. 네이버 금융 ─ 환율(KRW/USD)
# ══════════════════════════════════════════════════════════
def fetch_usd_rate() -> dict | None:
    try:
        res  = requests.get(NAVER_FX_URL, headers={**_HDR, "Referer": "https://finance.naver.com/"}, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        rate = None

        # 방법 1: no_today p 태그
        no_today = soup.find("p", class_="no_today")
        if no_today:
            spans = [s.get_text(strip=True) for s in no_today.find_all("span")]
            num   = "".join(s for s in spans if re.fullmatch(r"[\d.]+", s))
            try:
                rate = float(num)
            except Exception:
                pass

        # 방법 2: 텍스트 정규식
        if not rate or rate < 100:
            for m in re.finditer(r"(\d{1,4}\.\d{2})", soup.get_text()):
                v = float(m.group(1))
                if 900 < v < 2000:
                    rate = v
                    break

        if not rate or rate < 100:
            return None

        # 전일대비
        chg = None
        exday = soup.find("p", class_="no_exday")
        if exday:
            spans = [s.get_text(strip=True) for s in exday.find_all("span")]
            num   = "".join(s for s in spans if re.fullmatch(r"[\d.]+", s))
            try:
                chg = float(num)
            except Exception:
                pass
            ico = exday.find("span", class_="ico")
            if ico and "down" in " ".join(ico.get("class", [])):
                chg = -abs(chg) if chg else chg

        return {"당일Closing": rate, "전일대비": chg}

    except Exception as e:
        st.warning(f"환율 조회 실패: {e}")
        return None


# ══════════════════════════════════════════════════════════
#  5. Google Sheets 연동
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
            rows=20000, cols=20
        )
    return ws


# Sheets 컬럼 정의
SHEET_COLS = ["날짜", "품목", "판매가격_원", "LME_USD", "당일Closing",
              "전일대비", "전일대비pct", "지역"]


def load_gsheet() -> pd.DataFrame:
    try:
        ws   = get_gsheet()
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
        df = df.dropna(subset=["날짜"])
        for col in ["판매가격_원", "LME_USD", "당일Closing", "전일대비", "전일대비pct"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Sheets 로드 오류: {e}")
        return pd.DataFrame()


def _sv(v):
    """None / NaN → 빈 문자열"""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return v


def save_rows_to_gsheet(rows: list[dict]):
    """rows: [{"날짜":"YYYYMMDD","품목":..., ...}, ...]"""
    if not rows:
        return
    ws   = get_gsheet()
    data = ws.get_all_records()
    if not data:
        ws.append_row(SHEET_COLS)

    # 중복 방지: 이미 있는 (날짜, 품목) 세트
    existing = set()
    if data:
        df_ex = pd.DataFrame(data)
        for _, r in df_ex.iterrows():
            existing.add((str(r.get("날짜", ""))[:10], str(r.get("품목", ""))))

    new_rows = []
    for r in rows:
        date_str = str(r.get("날짜", ""))
        # YYYYMMDD → YYYY-MM-DD
        if len(date_str) == 8 and date_str.isdigit():
            date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        else:
            date_fmt = date_str

        key = (date_fmt, str(r.get("품목", "")))
        if key in existing:
            continue

        new_rows.append([
            date_fmt,
            _sv(r.get("품목")),
            _sv(r.get("판매가격_원")),
            _sv(r.get("LME_USD")),
            _sv(r.get("당일Closing")),
            _sv(r.get("전일대비")),
            _sv(r.get("전일대비pct")),
            _sv(r.get("지역", "")),
        ])

    for row_data in new_rows:
        ws.append_row(row_data)
        time.sleep(0.08)

    return len(new_rows)


# ══════════════════════════════════════════════════════════
#  6. 통계 계산
# ══════════════════════════════════════════════════════════
def calc_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    today      = df["날짜"].max()
    this_month = today.to_period("M")
    last_month = this_month - 1
    results    = []

    all_items = METALS + OILS + ["환율"]
    for item in all_items:
        sub = df[df["품목"] == item].copy()
        if sub.empty:
            continue

        # 가격 컬럼 선택
        if item in METALS:
            price_col = "판매가격_원"
            basis     = "원/톤(판매가)"
        elif item in OILS:
            price_col = "당일Closing"
            basis     = "USD/bbl"
        else:  # 환율
            price_col = "당일Closing"
            basis     = "KRW/USD"

        sub["월"] = sub["날짜"].dt.to_period("M")
        this_m   = sub[sub["월"] == this_month][price_col].dropna()
        last_m   = sub[sub["월"] == last_month][price_col].dropna()

        avg_this = round(this_m.mean(), 2) if not this_m.empty else None
        avg_last = round(last_m.mean(), 2) if not last_m.empty else None
        chg_pct  = None
        if avg_this and avg_last and avg_last != 0:
            chg_pct = round((avg_this - avg_last) / avg_last * 100, 2)

        latest       = sub.sort_values("날짜").iloc[-1]
        latest_price = latest.get(price_col)
        chg_val      = latest.get("전일대비")

        # LME USD 최신값 (비철금속만)
        lme_usd = latest.get("LME_USD") if item in METALS else None

        results.append({
            "품목":            item,
            "최신가":          latest_price,
            "LME(USD/ton)":   lme_usd,
            "가격기준":        basis,
            "전일대비":        chg_val,
            "당월누적평균":    avg_this,
            "전월평균":        avg_last,
            "전월대비변동(%)": chg_pct,
            "기준일":          latest["날짜"].strftime("%Y-%m-%d"),
        })

    out = pd.DataFrame(results)
    for col in ["최신가", "LME(USD/ton)", "전일대비", "당월누적평균",
                "전월평균", "전월대비변동(%)"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


# ══════════════════════════════════════════════════════════
#  7. 시황 코멘트 생성
# ══════════════════════════════════════════════════════════
def generate_comment(stats_df: pd.DataFrame) -> str:
    if stats_df.empty:
        return "데이터 없음"

    today_str  = datetime.now().strftime("%Y년 %m월 %d일")
    lines      = [f"**📋 {today_str} 비철금속·원유 시황 요약**\n"]
    up, dn, flat, big = [], [], [], []

    for _, row in stats_df.iterrows():
        item = row["품목"]
        if item == "환율":
            continue

        chg = row.get("전일대비")
        mom = row.get("전월대비변동(%)")

        if chg is None or pd.isna(chg):
            flat.append(item)
        elif float(chg) > 0:
            up.append(f"{item}(+{float(chg):,.0f})")
        elif float(chg) < 0:
            dn.append(f"{item}({float(chg):,.0f})")
        else:
            flat.append(item)

        if mom is not None and not pd.isna(mom) and abs(float(mom)) >= 3:
            direction = "상승" if float(mom) > 0 else "하락"
            big.append(f"{item} 전월대비 {abs(float(mom)):.1f}% {direction}")

    if up:
        lines.append(f"🔴 **상승:** {', '.join(up)}")
    if dn:
        lines.append(f"🔵 **하락:** {', '.join(dn)}")
    if flat:
        lines.append(f"⬜ **보합:** {', '.join(flat)}")
    if big:
        lines.append(f"\n📌 **월간 주요 변동:** {' / '.join(big)}")

    fx_row = stats_df[stats_df["품목"] == "환율"]
    if not fx_row.empty:
        fx     = fx_row.iloc[0]
        fx_v   = fx.get("최신가")
        fx_c   = fx.get("전일대비")
        fx_m   = fx.get("전월대비변동(%)")
        lines.append(
            f"\n💱 **환율(KRW/USD):** "
            f"{float(fx_v):,.2f}" if pd.notna(fx_v) else "-"
            f"  (전일대비 {float(fx_c):+.2f}" if pd.notna(fx_c) else ""
            f" / 전월대비 {float(fx_m):+.2f}%)" if pd.notna(fx_m) else ")"
        )

    return "\n\n".join(lines)


# ── 색상 스타일 ───────────────────────────────────────────
def color_val(val):
    try:
        v = float(str(val).replace(",", "").replace("%", ""))
        if v > 0:   return "color:#e74c3c; font-weight:bold"
        elif v < 0: return "color:#2980b9; font-weight:bold"
    except Exception:
        pass
    return "color:gray"


# ══════════════════════════════════════════════════════════
#  UI
# ══════════════════════════════════════════════════════════
st.title("📊 비철금속·원유 시황 모니터")
st.caption(
    "비철금속 판매가격: **공공데이터포털 (조달청 비축물자)** · "
    "LME USD 시세: **한국비철금속협회** · "
    "원유/환율: **네이버 금융** · Google Sheets 누적 저장"
)

col1, col2, col3 = st.columns([1, 1, 4])
with col1:
    btn_refresh = st.button("🔄 오늘 데이터 수집", use_container_width=True)
with col2:
    btn_bulk    = st.button("📥 전체 데이터 수집", use_container_width=True)
with col3:
    st.info(f"현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ── 데이터 수집 처리 ──────────────────────────────────────
if btn_refresh or btn_bulk:
    total_pages = 20 if btn_bulk else 2
    all_save_rows = []

    with st.spinner("📡 공공데이터포털 API 수집 중..."):
        df_pps = fetch_pps_api(total_pages=total_pages)
        st.success(f"✅ 판매가격 API: {len(df_pps)}건 수신")

    with st.spinner("📡 LME USD 시세 수집 중 (한국비철금속협회)..."):
        df_lme = fetch_lme_usd()
        st.success(f"✅ LME 시세: {len(df_lme)}건 수신")

    # 비철금속 판매가격 행 생성
    if not df_pps.empty:
        for _, r in df_pps.iterrows():
            row = {
                "날짜":      r["날짜"],
                "품목":      r["품목"],
                "판매가격_원": r.get("판매가격_원"),
                "지역":      r.get("지역", ""),
            }
            # 같은 날짜의 LME USD 매핑
            if not df_lme.empty:
                lme_row = df_lme[df_lme["날짜"] == r["날짜"]]
                if not lme_row.empty:
                    col_map_rev = {v: k for k, v in LME_COL_TO_METAL.items()}
                    lme_col = col_map_rev.get(r["품목"])
                    if lme_col:
                        row["LME_USD"] = lme_row.iloc[0].get(lme_col)
            all_save_rows.append(row)

    # 원유 행 생성
    with st.spinner("🛢️ 원유 가격 수집 중..."):
        oil_pages = 10 if btn_bulk else 1
        oil_rows  = fetch_oil_prices(pages=oil_pages)
        for r in oil_rows:
            all_save_rows.append({
                "날짜":        r["날짜"],
                "품목":        r["품목"],
                "당일Closing": r["당일Closing"],
                "전일대비":    r["전일대비"],
                "전일대비pct": r["전일대비pct"],
            })
        st.success(f"✅ 원유: {len(oil_rows)}건 수신")

    # 환율 행 생성
    with st.spinner("💱 환율 수집 중..."):
        fx = fetch_usd_rate()
        if fx:
            today_str = datetime.now().strftime("%Y%m%d")
            all_save_rows.append({
                "날짜":        today_str,
                "품목":        "환율",
                "당일Closing": fx["당일Closing"],
                "전일대비":    fx["전일대비"],
            })
            st.success(f"✅ 환율: {fx['당일Closing']:,.2f} KRW/USD")

    # Google Sheets 저장
    with st.spinner("💾 Google Sheets 저장 중..."):
        saved_n = save_rows_to_gsheet(all_save_rows)
        st.success(f"✅ 신규 {saved_n}건 저장 완료!")

    st.cache_data.clear()


# ── Sheets 로드 ───────────────────────────────────────────
df_all = load_gsheet()

if df_all.empty:
    st.warning("⚠️ 저장된 데이터가 없습니다. **[오늘 데이터 수집]** 버튼을 눌러주세요.")
    st.stop()

stats_df = calc_stats(df_all)


# ══════════════════════════════════════════════════════════
#  탭 구성
# ══════════════════════════════════════════════════════════
tab1, tab2, tab3 = st.tabs(["📌 오늘 시황", "📈 추이 차트", "📊 월별 분석"])


# ── TAB 1 : 오늘 시황 ─────────────────────────────────────
with tab1:
    # 시황 코멘트
    st.markdown(generate_comment(stats_df))
    st.divider()

    # 비철금속 메트릭 카드
    st.subheader("🔩 비철금속 판매가격 (원/톤, 부가세 포함)")
    metal_stats = stats_df[stats_df["품목"].isin(METALS)]
    cols = st.columns(len(METALS))
    for i, (_, row) in enumerate(metal_stats.iterrows()):
        price  = row.get("최신가")
        chg    = row.get("전일대비")
        lme    = row.get("LME(USD/ton)")
        delta  = f"{float(chg):+,.0f}원" if pd.notna(chg) and chg is not None else "-"
        d_clr  = "normal" if pd.notna(chg) and chg is not None else "off"
        p_str  = f"₩{float(price):,.0f}" if pd.notna(price) and price is not None else "-"
        lme_str = f"LME ${float(lme):,.1f}" if pd.notna(lme) and lme is not None else ""
        cols[i].metric(
            label=f"{row['품목']}",
            value=p_str,
            delta=delta,
            delta_color=d_clr,
            help=lme_str,
        )

    st.divider()

    # 원유 메트릭 카드
    st.subheader("🛢️ 국제유가 (USD/bbl)")
    oil_live = fetch_oil_latest()
    oc = st.columns(len(OILS))
    for i, oil_name in enumerate(OILS):
        if oil_name in oil_live:
            o = oil_live[oil_name]
            p = o.get("당일Closing")
            pct = o.get("전일대비pct")
            chg = o.get("전일대비")
            if pct is not None:
                d_str, d_clr = f"{float(pct):+.2f}%", "normal"
            elif chg is not None:
                d_str, d_clr = f"{float(chg):+.2f}", "normal"
            else:
                d_str, d_clr = "-", "off"
            oc[i].metric(
                label=oil_name,
                value=f"${float(p):,.2f}" if p else "-",
                delta=d_str,
                delta_color=d_clr,
            )
        else:
            oc[i].metric(label=oil_name, value="-", delta="-", delta_color="off")

    st.divider()

    # 전월대비 분석 테이블
    st.subheader("📋 전월대비 분석 테이블")
    disp_cols = [
        "품목", "최신가", "LME(USD/ton)", "가격기준",
        "전일대비", "당월누적평균", "전월평균", "전월대비변동(%)", "기준일"
    ]
    fmt = {
        "최신가":          "{:,.0f}",
        "LME(USD/ton)":   "{:,.1f}",
        "전일대비":        "{:+,.0f}",
        "당월누적평균":    "{:,.0f}",
        "전월평균":        "{:,.0f}",
        "전월대비변동(%)": "{:+.2f}%",
    }
    st.dataframe(
        stats_df[disp_cols].style
        .map(color_val, subset=["전일대비", "전월대비변동(%)"])
        .format(fmt, na_rep="-"),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    # 환율
    st.subheader("💱 환율 (KRW/USD)")
    fx_live = fetch_usd_rate()
    fx_row  = stats_df[stats_df["품목"] == "환율"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "실시간 환율",
        f"{fx_live['당일Closing']:,.2f}" if fx_live else "-",
        delta=f"{fx_live['전일대비']:+.2f}" if fx_live and fx_live.get("전일대비") else None,
    )
    if not fx_row.empty:
        fx = fx_row.iloc[0]
        c2.metric("당월 누적평균", f"{float(fx['당월누적평균']):,.2f}" if pd.notna(fx.get("당월누적평균")) else "-")
        c3.metric("전월 평균",     f"{float(fx['전월평균']):,.2f}"     if pd.notna(fx.get("전월평균"))     else "-")
        c4.metric("전월대비 변동", f"{float(fx['전월대비변동(%)']):+.2f}%" if pd.notna(fx.get("전월대비변동(%)")) else "-")


# ── TAB 2 : 추이 차트 ─────────────────────────────────────
with tab2:
    today_dt = df_all["날짜"].max()

    st.subheader("🔩 비철금속 판매가격 추이 (원/톤)")
    sel_metals = st.multiselect(
        "품목 선택", METALS, default=["구리", "알루미늄", "니켈"], key="sel_m"
    )
    period = st.radio("기간", ["1개월", "3개월", "전체"], horizontal=True, key="p_
