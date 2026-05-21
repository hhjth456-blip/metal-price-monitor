# ============================================================
#  app.py  -  비철금속 & 원유 시황 모니터
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

# ── 페이지 설정
st.set_page_config(
    page_title="비철금속 원유 시황 모니터",
    page_icon="📊",
    layout="wide"
)

# ── 상수
METALS = ["알루미늄", "납", "아연", "구리", "주석", "니켈"]
OILS   = ["WTI", "브렌트유"]

PPS_API_URL = (
    "https://api.data.go.kr"
    "/15151568/v1/uddi:18394309-5202-4567-9914-ab9b3a05712c"
)

NAVER_OIL_URLS = {
    "WTI": (
        "https://finance.naver.com/marketindex/worldDailyQuote.naver"
        "?marketindexCd=OIL_CL&fdtc=2"
    ),
    "브렌트유": (
        "https://finance.naver.com/marketindex/worldDailyQuote.naver"
        "?marketindexCd=OIL_BRT&fdtc=2"
    ),
}

NAVER_FX_URL = (
    "https://finance.naver.com/marketindex/exchangeDetail.naver"
    "?marketindexCd=FX_USDKRW"
)

NONFERROUS_LME_URL = "https://www.nonferrous.or.kr/stats/?act=sub3"

_HDR = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
}

LME_COL_TO_METAL = {
    "Cu": "구리",
    "Al": "알루미늄",
    "Zn": "아연",
    "Pb": "납",
    "Ni": "니켈",
    "Sn": "주석",
}

SHEET_COLS = [
    "날짜", "품목", "판매가격_원", "LME_USD",
    "당일Closing", "전일대비", "전일대비pct", "지역"
]

# API 필드 매핑 - 최초 실행 후 실제 컬럼명 확인 후 수정
FIELD_MAP = {
    "품명":               "품목",
    "판매가격(부가세 포함)": "판매가격",
    "판매지방청":         "지역",
    "판매기간":           "날짜",
}

METAL_ALIAS = {
    "알루미늄": "알루미늄",
    "구리":     "구리",
    "아연":     "아연",
    "납":       "납",
    "주석":     "주석",
    "니켈":     "니켈",
}


# ══════════════════════════════════════════════════════════
#  1. 공공데이터포털 API
# ══════════════════════════════════════════════════════════
def fetch_pps_api(total_pages=5):
    api_key  = st.secrets["data_go_kr"]["api_key"]
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
            body      = res.json()
            data      = body.get("data", [])
            total_cnt = body.get("totalCount", 0)
            if not data:
                break
            all_rows.extend(data)
            if len(all_rows) >= total_cnt:
                break
        except Exception as e:
            st.warning("공공데이터 API 오류 page=" + str(page) + " : " + str(e))
            break
        time.sleep(0.2)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    if "api_cols_shown" not in st.session_state:
        with st.expander("최초 1회 - API 응답 원본 컬럼 확인", expanded=True):
            st.write("컬럼 목록:", df.columns.tolist())
            st.dataframe(df.head(5))
        st.session_state["api_cols_shown"] = True

    return _parse_pps_df(df)


def _parse_pps_df(df):
    rename = {k: v for k, v in FIELD_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    required = {"날짜", "품목", "판매가격"}
    missing  = required - set(df.columns)
    if missing:
        st.error("API 필드 매핑 실패 누락=" + str(missing) + " FIELD_MAP을 수정하세요.")
        return pd.DataFrame()

    def norm_date(v):
        s = str(v).replace(".", "").replace("-", "").strip()
        if len(s) == 8 and s.isdigit():
            return s
        return None

    def norm_item(v):
        s = str(v).strip()
        for k in METAL_ALIAS:
            if k in s:
                return METAL_ALIAS[k]
        return s

    def norm_price(v):
        s = re.sub(r"[^\d.]", "", str(v))
        try:
            return float(s)
        except Exception:
            return None

    df["날짜"]      = df["날짜"].apply(norm_date)
    df             = df.dropna(subset=["날짜"])
    df["품목"]      = df["품목"].apply(norm_item)
    df["판매가격_원"] = df["판매가격"].apply(norm_price)

    keep = ["날짜", "품목", "판매가격_원"]
    if "지역" in df.columns:
        keep.append("지역")
    return df[keep]


# ══════════════════════════════════════════════════════════
#  2. 한국비철금속협회 LME 시세 (USD/ton)
# ══════════════════════════════════════════════════════════
def fetch_lme_usd():
    try:
        headers = dict(_HDR)
        headers["Referer"] = "https://www.nonferrous.or.kr/"
        res  = requests.get(NONFERROUS_LME_URL, headers=headers, timeout=15)
        res.raise_for_status()
        soup  = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table")
        if not table:
            return pd.DataFrame()

        def sf(v):
            try:
                return float(v.replace(",", ""))
            except Exception:
                return None

        rows = []
        for tr in table.select("tbody tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 7:
                continue
            date_raw = tds[0].replace(".", "").replace(" ", "")
            if len(date_raw) != 8 or not date_raw.isdigit():
                continue
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
        st.warning("LME 시세 크롤링 실패: " + str(e))
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════
#  3. 네이버 금융 - 원유
# ══════════════════════════════════════════════════════════
def fetch_oil_prices(pages=1):
    results = []
    for oil_name, base_url in NAVER_OIL_URLS.items():
        for page in range(1, pages + 1):
            url = base_url + "&page=" + str(page)
            try:
                headers = dict(_HDR)
                headers["Referer"] = "https://finance.naver.com/"
                res  = requests.get(url, headers=headers, timeout=10)
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

                    is_down = False
                    try:
                        pct     = float(pct_str.replace(",", "").replace("%", ""))
                        is_down = pct < 0
                    except Exception:
                        pct = None

                    img = tds[1].find("img")
                    if img and "하락" in img.get("alt", ""):
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


def fetch_oil_latest():
    rows   = fetch_oil_prices(pages=1)
    latest = {}
    for r in rows:
        if r["품목"] not in latest:
            latest[r["품목"]] = r
    return latest


# ══════════════════════════════════════════════════════════
#  4. 네이버 금융 - 환율
# ══════════════════════════════════════════════════════════
def fetch_usd_rate():
    try:
        headers = dict(_HDR)
        headers["Referer"] = "https://finance.naver.com/"
        res  = requests.get(NAVER_FX_URL, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        rate = None

        no_today = soup.find("p", class_="no_today")
        if no_today:
            spans = [s.get_text(strip=True) for s in no_today.find_all("span")]
            num   = "".join(s for s in spans if re.fullmatch(r"[\d.]+", s))
            try:
                rate = float(num)
            except Exception:
                pass

        if not rate or rate < 100:
            for m in re.finditer(r"(\d{1,4}\.\d{2})", soup.get_text()):
                v = float(m.group(1))
                if 900 < v < 2000:
                    rate = v
                    break

        if not rate or rate < 100:
            return {}

        chg   = None
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
                if chg is not None:
                    chg = -abs(chg)

        return {"당일Closing": rate, "전일대비": chg}

    except Exception as e:
        st.warning("환율 조회 실패: " + str(e))
        return {}


# ══════════════════════════════════════════════════════════
#  5. Google Sheets
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


def load_gsheet():
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
        st.error("Sheets 로드 오류: " + str(e))
        return pd.DataFrame()


def _sv(v):
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return v


def save_rows_to_gsheet(rows):
    if not rows:
        return 0
    ws   = get_gsheet()
    data = ws.get_all_records()
    if not data:
        ws.append_row(SHEET_COLS)

    existing = set()
    if data:
        df_ex = pd.DataFrame(data)
        for _, r in df_ex.iterrows():
            existing.add((str(r.get("날짜", ""))[:10], str(r.get("품목", ""))))

    new_rows = []
    for r in rows:
        date_str = str(r.get("날짜", ""))
        if len(date_str) == 8 and date_str.isdigit():
            date_fmt = date_str[:4] + "-" + date_str[4:6] + "-" + date_str[6:]
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
def calc_stats(df):
    if df.empty:
        return pd.DataFrame()

    today      = df["날짜"].max()
    this_month = today.to_period("M")
    last_month = (today.to_period("M") - 1)

    results = []
    for item in METALS + OILS + ["환율"]:
        # ★ .copy() 명시적으로 추가
        sub = df[df["품목"] == item].copy()
        if sub.empty:
            continue

        price_col = "당일Official" if item in METALS else "당일Closing"

        # ★ price_col이 실제로 존재하는지 확인
        if price_col not in sub.columns:
            continue

        # ★ 월 컬럼 추가 시 copy된 df에 직접 할당
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

        chg_val = latest.get("전일대비")
        if chg_val is None or (isinstance(chg_val, float) and pd.isna(chg_val)):
            chg_val = latest.get("전일대비(%)")
        if isinstance(chg_val, float) and pd.isna(chg_val):
            chg_val = None

        if item in METALS:
            basis = "Official"
        elif item in OILS:
            basis = "USD/bbl"
        else:
            basis = "현물종가"

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
#  7. 시황 코멘트
# ══════════════════════════════════════════════════════════
def generate_comment(stats_df):
    if stats_df.empty:
        return "데이터 없음"

    today_str = datetime.now().strftime("%Y년 %m월 %d일")
    lines     = ["**📋 " + today_str + " 비철금속·원유 시황 요약**\n"]
    up        = []
    dn        = []
    flat      = []
    big       = []

    for _, row in stats_df.iterrows():
        item = row["품목"]
        if item == "환율":
            continue

        chg = row.get("전일대비")
        mom = row.get("전월대비변동(%)")

        if chg is None or pd.isna(chg):
            flat.append(item)
        elif float(chg) > 0:
            up.append(item + "(+" + str(int(float(chg))) + ")")
        elif float(chg) < 0:
            dn.append(item + "(" + str(int(float(chg))) + ")")
        else:
            flat.append(item)

        if mom is not None and not pd.isna(mom) and abs(float(mom)) >= 3:
            direction = "상승" if float(mom) > 0 else "하락"
            big.append(item + " 전월대비 " + str(round(abs(float(mom)), 1)) + "% " + direction)

    if up:
        lines.append("🔴 **상승:** " + ", ".join(up))
    if dn:
        lines.append("🔵 **하락:** " + ", ".join(dn))
    if flat:
        lines.append("⬜ **보합:** " + ", ".join(flat))
    if big:
        lines.append("\n📌 **월간 주요 변동:** " + " / ".join(big))

    fx_row = stats_df[stats_df["품목"] == "환율"]
    if not fx_row.empty:
        fx    = fx_row.iloc[0]
        fx_v  = fx.get("최신가")
        fx_c  = fx.get("전일대비")
        fx_m  = fx.get("전월대비변동(%)")
        fx_vs = str(round(float(fx_v), 2)) if pd.notna(fx_v) and fx_v is not None else "-"
        fx_cs = ("+" if float(fx_c) > 0 else "") + str(round(float(fx_c), 2)) if pd.notna(fx_c) and fx_c is not None else "-"
        fx_ms = ("+" if float(fx_m) > 0 else "") + str(round(float(fx_m), 2)) + "%" if pd.notna(fx_m) and fx_m is not None else "-"
        lines.append("\n💱 **환율(KRW/USD):** " + fx_vs + "  (전일대비 " + fx_cs + " / 전월대비 " + fx_ms + ")")

    return "\n\n".join(lines)


# ── 색상 스타일
def color_val(val):
    try:
        v = float(str(val).replace(",", "").replace("%", ""))
        if v > 0:
            return "color:#e74c3c; font-weight:bold"
        elif v < 0:
            return "color:#2980b9; font-weight:bold"
    except Exception:
        pass
    return "color:gray"


# ══════════════════════════════════════════════════════════
#  UI
# ══════════════════════════════════════════════════════════
st.title("📊 비철금속·원유 시황 모니터")
st.caption(
    "비철금속 판매가: 공공데이터포털(조달청 비축물자) | "
    "LME USD: 한국비철금속협회 | "
    "원유/환율: 네이버 금융 | Google Sheets 누적 저장"
)

col1, col2, col3 = st.columns([1, 1, 4])
with col1:
    btn_refresh = st.button("오늘 데이터 수집", use_container_width=True)
with col2:
    btn_bulk = st.button("전체 데이터 수집", use_container_width=True)
with col3:
    st.info("현재 시각: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


# ── 데이터 수집
if btn_refresh or btn_bulk:
    total_pages   = 20 if btn_bulk else 2
    all_save_rows = []

    with st.spinner("공공데이터포털 API 수집 중..."):
        df_pps = fetch_pps_api(total_pages=total_pages)
        st.success("판매가격 API: " + str(len(df_pps)) + "건 수신")

    with st.spinner("LME USD 시세 수집 중..."):
        df_lme = fetch_lme_usd()
        st.success("LME 시세: " + str(len(df_lme)) + "건 수신")

    if not df_pps.empty:
        metal_to_lme = {v: k for k, v in LME_COL_TO_METAL.items()}
        for _, r in df_pps.iterrows():
            row = {
                "날짜":        r["날짜"],
                "품목":        r["품목"],
                "판매가격_원": r.get("판매가격_원"),
                "지역":        r.get("지역", ""),
            }
            if not df_lme.empty:
                lme_rows = df_lme[df_lme["날짜"] == r["날짜"]]
                if not lme_rows.empty:
                    lme_col = metal_to_lme.get(r["품목"])
                    if lme_col and lme_col in lme_rows.columns:
                        row["LME_USD"] = lme_rows.iloc[0][lme_col]
            all_save_rows.append(row)

    with st.spinner("원유 가격 수집 중..."):
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
        st.success("원유: " + str(len(oil_rows)) + "건 수신")

    with st.spinner("환율 수집 중..."):
        fx = fetch_usd_rate()
        if fx:
            today_key = datetime.now().strftime("%Y%m%d")
            all_save_rows.append({
                "날짜":        today_key,
                "품목":        "환율",
                "당일Closing": fx.get("당일Closing"),
                "전일대비":    fx.get("전일대비"),
            })
            rate_val = fx.get("당일Closing")
            st.success("환율: " + str(round(rate_val, 2)) + " KRW/USD")

    with st.spinner("Google Sheets 저장 중..."):
        saved_n = save_rows_to_gsheet(all_save_rows)
        st.success("신규 " + str(saved_n) + "건 저장 완료!")

    st.cache_data.clear()


# ── Sheets 로드
df_all = load_gsheet()

if df_all.empty:
    st.warning("저장된 데이터가 없습니다. 오늘 데이터 수집 버튼을 눌러주세요.")
    st.stop()

stats_df = calc_stats(df_all)


# ══════════════════════════════════════════════════════════
#  탭
# ══════════════════════════════════════════════════════════
tab1, tab2, tab3 = st.tabs(["📌 오늘 시황", "📈 추이 차트", "📊 월별 분석"])


# ── TAB 1
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
            avg_this = row.get("당월누적평균")
            avg_last = row.get("전월평균")
            mom      = row.get("전월대비변동(%)")
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

    # ★ 실제 존재하는 컬럼만 필터링
    display_cols = [c for c in display_cols if c in stats_df.columns]

    # ★ 스타일 적용할 컬럼도 존재 여부 확인 후 적용
    style_cols = [c for c in ["전일대비(%)", "전월대비변동(%)"] if c in display_cols]

    # ★ 포맷 딕셔너리도 존재하는 컬럼만 적용
    fmt_dict = {}
    if "최신가"          in display_cols: fmt_dict["최신가"]          = "{:,.2f}"
    if "전일대비(%)"     in display_cols: fmt_dict["전일대비(%)"]     = "{:+.2f}%"
    if "당월누적평균"    in display_cols: fmt_dict["당월누적평균"]    = "{:,.2f}"
    if "전월평균"        in display_cols: fmt_dict["전월평균"]        = "{:,.2f}"
    if "전월대비변동(%)" in display_cols: fmt_dict["전월대비변동(%)"] = "{:+.2f}%"

    styled = stats_df[display_cols].style.format(fmt_dict, na_rep="-")
    if style_cols:
        styled = styled.map(color_val, subset=style_cols)

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
    )

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

# ── TAB 2
with tab2:
    today_dt = df_all["날짜"].max()

    st.subheader("비철금속 판매가격 추이 (원/톤)")
    sel_metals = st.multiselect(
        "품목 선택",
        options=METALS,
        default=["구리", "알루미늄", "니켈"],
        key="sel_m"
    )
    period_metal = st.radio(
        "기간",
        options=["1개월", "3개월", "전체"],
        horizontal=True,
        key="p_m"
    )
    if sel_metals:
        df_c = df_all[df_all["품목"].isin(sel_metals)].copy()
        if period_metal == "1개월":
            df_c = df_c[df_c["날짜"] >= today_dt - pd.Timedelta(days=30)]
        elif period_metal == "3개월":
            df_c = df_c[df_c["날짜"] >= today_dt - pd.Timedelta(days=90)]
        pivot = df_c.pivot_table(index="날짜", columns="품목", values="판매가격_원")
        st.line_chart(pivot, use_container_width=True)

    st.divider()

    st.subheader("국제유가 추이 (USD/bbl)")
    period_oil = st.radio(
        "기간",
        options=["1개월", "3개월", "전체"],
        horizontal=True,
        key="p_o"
    )
    df_oil = df_all[df_all["품목"].isin(OILS)].copy()
    if not df_oil.empty:
        if period_oil == "1개월":
            df_oil = df_oil[df_oil["날짜"] >= today_dt - pd.Timedelta(days=30)]
        elif period_oil == "3개월":
            df_oil = df_oil[df_oil["날짜"] >= today_dt - pd.Timedelta(days=90)]
        pivot_oil = df_oil.pivot_table(index="날짜", columns="품목", values="당일Closing")
        st.line_chart(pivot_oil, use_container_width=True)
    else:
        st.info("원유 데이터가 없습니다. 데이터 수집 후 다시 확인하세요.")

    st.divider()

    st.subheader("환율 추이 (KRW/USD)")
    df_fx = df_all[df_all["품목"] == "환율"][["날짜", "당일Closing"]].set_index("날짜")
    df_fx.columns = ["KRW/USD"]
    st.line_chart(df_fx, use_container_width=True)


# ── TAB 3
with tab3:
    st.subheader("월별 평균가 비교")
    item_sel = st.selectbox("품목 선택", options=METALS + OILS + ["환율"])
    df_item  = df_all[df_all["품목"] == item_sel].copy()
    df_item["월"] = df_item["날짜"].dt.to_period("M").astype(str)

    if item_sel in METALS:
        val_col = "판매가격_원"
        label   = "원/톤"
    elif item_sel in OILS:
        val_col = "당일Closing"
        label   = "USD/bbl"
    else:
        val_col = "당일Closing"
        label   = "KRW/USD"

    col_label = "월평균(" + label + ")"
    monthly   = (
        df_item.groupby("월")[val_col]
        .mean()
        .reset_index()
        .rename(columns={val_col: col_label})
    )
    monthly[col_label]    = monthly[col_label].round(2)
    monthly["전월대비(%)"] = monthly[col_label].pct_change().mul(100).round(2)

    st.bar_chart(monthly.set_index("월")[col_label], use_container_width=True)
    st.dataframe(
        monthly.style
        .map(color_val, subset=["전월대비(%)"])
        .format({
            col_label:      "{:,.2f}",
            "전월대비(%)": lambda x: ("+" if x > 0 else "") + str(round(x, 2)) + "%" if pd.notna(x) else "-",
        }),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("데이터 다운로드 (CSV)")
    export        = df_all.copy()
    export["날짜"] = export["날짜"].dt.strftime("%Y-%m-%d")
    st.download_button(
        label="CSV 다운로드",
        data=export.to_csv(index=False).encode("utf-8-sig"),
        file_name="metal_prices_" + datetime.now().strftime("%Y%m%d") + ".csv",
        mime="text/csv",
    )

st.divider()
st.caption(
    "비철금속 판매가: 조달청 비축물자 공공데이터포털 API (원화 부가세포함) | "
    "LME USD: 한국비철금속협회 | "
    "원유/환율: 네이버 금융 | 비상업적 참고용"
)
