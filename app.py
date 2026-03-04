import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(
    page_title="조달청 비철금속 국제가격",
    page_icon="📊",
    layout="wide"
)

BASE_URL = "https://www.pps.go.kr/bichuk/bbs"
LIST_URL = f"{BASE_URL}/list.do"
VIEW_URL = f"{BASE_URL}/view.do"
METALS   = ["알루미늄", "납", "아연", "구리", "주석", "니켈"]

# ── Google Sheets 연결 ────────────────────────────────────
@st.cache_resource
def get_gsheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=scopes
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(st.secrets["sheets"]["spreadsheet_id"])
    try:
        ws = sheet.worksheet(st.secrets["sheets"]["worksheet_name"])
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(
            title=st.secrets["sheets"]["worksheet_name"],
            rows=10000, cols=20
        )
    return ws

def load_gsheet():
    try:
        ws = get_gsheet()
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
        df = df.dropna(subset=["날짜"])
        # 숫자 컬럼 변환
        for col in ["전월평균", "전주평균", "전일Official", "전일Closing",
                    "당일Official", "당일Closing", "전일대비"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Google Sheets 로드 오류: {e}")
        return pd.DataFrame()

def save_to_gsheet(price_date, data):
    try:
        df_existing = load_gsheet()

        # 중복 날짜 체크
        if not df_existing.empty:
            existing_dates = df_existing["날짜"].dt.strftime("%Y%m%d").tolist()
            if price_date in existing_dates:
                return df_existing

        # 새 데이터 행 구성
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

        ws = get_gsheet()

        # 시트가 비어있으면 헤더 먼저 작성
        existing_data = ws.get_all_records()
        cols = ["날짜", "품목", "전월평균", "전주평균", "전일Official",
                "전일Closing", "당일Official", "당일Closing", "전일대비"]

        if not existing_data:
            ws.append_row(cols)

        # 새 행 추가
        for _, row in df_new.iterrows():
            row_data = [
                row["날짜"].strftime("%Y-%m-%d"),
                str(row.get("품목", "")),
                _safe_val(row.get("전월평균")),
                _safe_val(row.get("전주평균")),
                _safe_val(row.get("전일Official")),
                _safe_val(row.get("전일Closing")),
                _safe_val(row.get("당일Official")),
                _safe_val(row.get("당일Closing")),
                _safe_val(row.get("전일대비")),
            ]
            ws.append_row(row_data)
            time.sleep(0.1)  # API 호출 제한 방지

        # 전체 다시 로드해서 반환
        return load_gsheet()

    except Exception as e:
        st.error(f"Google Sheets 저장 오류: {e}")
        return df_existing if not df_existing.empty else pd.DataFrame()

def _safe_val(v):
    """None/NaN → 빈 문자열로 변환 (Sheets 저장용)"""
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return v

# ── 세션 ────────────────────────────────────────────────
def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Connection": "keep-alive",
    })
    try:
        s.get("https://www.pps.go.kr/bichuk/index.do", timeout=10)
        time.sleep(1)
    except:
        pass
    return s

# ── 목록 크롤링 ──────────────────────────────────────────
def crawl_list(session, pages=1):
    items = []
    for page in range(1, pages + 1):
        try:
            res = session.get(LIST_URL, params={
                "key": "00823", "pageIndex": page,
                "orderBy": "bbsOrdr desc", "sc": "", "sw": ""
            }, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")
            for a in soup.select("table tbody tr td a"):
                onclick = a.get("onclick", "")
                m = re.search(r"goView\('(\w+)'", onclick)
                if not m:
                    m = re.search(r"fn_view\('(\w+)'", onclick)
                if m:
                    items.append(m.group(1))
        except:
            pass
        time.sleep(0.5)
    return items

# ── 상세 크롤링 ──────────────────────────────────────────
def crawl_detail(session, bbs_sn):
    try:
        res = session.get(VIEW_URL, params={
            "bbsSn": bbs_sn, "key": "00823",
            "pageIndex": 1, "orderBy": "bbsOrdr desc",
            "sc": "", "sw": ""
        }, timeout=15)
        res.raise_for_status()
    except Exception as e:
        return None

    soup = BeautifulSoup(res.text, "html.parser")

    # ── 가격일자 추출 ──
    price_date = None
    for span in soup.find_all("span"):
        t = span.get_text(strip=True)
        if "가격일자:" in t:
            price_date = t.replace("가격일자:", "").strip()
            break
    if not price_date:
        full_text = soup.get_text()
        m = re.search(r"가격일자[:\s]*(\d{8})", full_text)
        if m:
            price_date = m.group(1)
    if not price_date:
        m = re.match(r"(\d{8})", bbs_sn)
        if m:
            price_date = m.group(1)

    # ── 가격 테이블 파싱 ──
    content_div = soup.find("div", id="brdContent")
    if not content_div:
        return {"price_date": price_date, "data": {}}
    tbl = content_div.find("table")
    if not tbl:
        return {"price_date": price_date, "data": {}}
    tbody = tbl.find("tbody")
    if not tbody:
        return {"price_date": price_date, "data": {}}

    def safe_float(v):
        try:
            return float(str(v).replace(",", ""))
        except:
            return None

    rows = {}
    current_item = ""
    for tr in tbody.find_all("tr"):
        th = tr.find("th")
        tds = tr.find_all("td")
        if th:
            current_item = th.get_text(strip=True)
        cells = [td.get_text(strip=True) for td in tds]
        if not cells:
            continue

        if current_item in METALS:
            구분 = cells[0] if cells else ""
            if 구분 == "CASH":
                rows[current_item] = {
                    "전월평균":     safe_float(cells[1]) if len(cells) > 1 else None,
                    "전주평균":     safe_float(cells[2]) if len(cells) > 2 else None,
                    "전일Official": safe_float(cells[3]) if len(cells) > 3 else None,
                    "전일Closing":  safe_float(cells[4]) if len(cells) > 4 else None,
                    "당일Official": safe_float(cells[5]) if len(cells) > 5 else None,
                    "당일Closing":  safe_float(cells[6]) if len(cells) > 6 else None,
                    "전일대비":     safe_float(cells[7]) if len(cells) > 7 else None,
                }
        elif current_item == "환율":
            rows["환율"] = {
                "전월평균":     safe_float(cells[1]) if len(cells) > 1 else None,
                "전주평균":     safe_float(cells[2]) if len(cells) > 2 else None,
                "전일Official": None,
                "전일Closing":  safe_float(cells[3]) if len(cells) > 3 else None,
                "당일Official": None,
                "당일Closing":  safe_float(cells[4]) if len(cells) > 4 else None,
                "전일대비":     safe_float(cells[5]) if len(cells) > 5 else None,
            }

    return {"price_date": price_date, "data": rows}

# ── 통계 계산 ─────────────────────────────────────────────
def calc_stats(df):
    if df.empty:
        return pd.DataFrame()

    today      = df["날짜"].max()
    this_month = today.to_period("M")
    last_month = (today.to_period("M") - 1)

    results = []
    for item in METALS + ["환율"]:
        sub = df[df["품목"] == item].copy()
        if sub.empty:
            continue

        sub["월"] = sub["날짜"].dt.to_period("M")
        this_m = sub[sub["월"] == this_month]["당일Closing"].dropna()
        last_m = sub[sub["월"] == last_month]["당일Closing"].dropna()

        avg_this = round(this_m.mean(), 2) if not this_m.empty else None
        avg_last = round(last_m.mean(), 2) if not last_m.empty else None
        chg_pct  = None
        # ✅ 0.0 falsy 버그 수정 — is not None 명시 체크
        if avg_this is not None and avg_last is not None and avg_last != 0:
            chg_pct = round((avg_this - avg_last) / avg_last * 100, 2)

        latest  = sub.sort_values("날짜").iloc[-1]

        chg_val = latest.get("전일대비")
        if chg_val is None or (isinstance(chg_val, float) and pd.isna(chg_val)):
            chg_val = latest.get("전일대비(%)")
        if isinstance(chg_val, float) and pd.isna(chg_val):
            chg_val = None

        results.append({
            "품목":            item,
            "최신가(Closing)": latest.get("당일Closing"),
            "전일대비(%)":     chg_val,
            "당월누적평균":    avg_this,
            "전월평균":        avg_last,
            "전월대비변동(%)": chg_pct,
            "기준일":          latest["날짜"].strftime("%Y-%m-%d"),
        })

    result_df = pd.DataFrame(results)
    for col in ["최신가(Closing)", "전일대비(%)", "당월누적평균", "전월평균", "전월대비변동(%)"]:
        if col in result_df.columns:
            result_df[col] = pd.to_numeric(result_df[col], errors="coerce")
    return result_df

# ── AI 룰베이스 코멘트 ────────────────────────────────────
def generate_comment(stats_df):
    if stats_df.empty:
        return "데이터 없음"

    today_str = datetime.now().strftime("%Y년 %m월 %d일")
    lines = [f"**📋 {today_str} 비철금속 시황 요약**\n"]
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

    if up_items:
        lines.append(f"🔴 **상승:** {', '.join(up_items)}")
    if dn_items:
        lines.append(f"🔵 **하락:** {', '.join(dn_items)}")
    if flat_items:
        lines.append(f"⬜ **보합:** {', '.join(flat_items)}")
    if big_movers:
        lines.append(f"\n📌 **월간 주요 변동:** {' / '.join(big_movers)}")

    fx_row = stats_df[stats_df["품목"] == "환율"]
    if not fx_row.empty:
        fx      = fx_row.iloc[0]
        fx_val  = fx.get("최신가(Closing)")
        fx_chg  = fx.get("전일대비(%)")
        fx_mom  = fx.get("전월대비변동(%)")
        fx_str  = f"{float(fx_val):,.2f}" if pd.notna(fx_val) and fx_val is not None else "-"
        chg_str = f"{float(fx_chg):+.2f}%" if pd.notna(fx_chg) and fx_chg is not None else "-"
        mom_str = f"{float(fx_mom):+.2f}%" if pd.notna(fx_mom) and fx_mom is not None else "-"
        lines.append(f"\n💱 **환율(KRW/USD):** {fx_str} (전일대비 {chg_str} / 전월대비 {mom_str})")

    return "\n\n".join(lines)

# ── 색상 스타일 ───────────────────────────────────────────
def color_val(val):
    if val is None:
        return ""
    try:
        v = float(str(val).replace(",", "").replace("%", ""))
        if v > 0:   return "color:#e74c3c; font-weight:bold"
        elif v < 0: return "color:#2980b9; font-weight:bold"
        return "color:gray"
    except:
        return ""

# ══════════════════════════════════════════════════════════
#  UI
# ══════════════════════════════════════════════════════════
st.title("📊 조달청 비철금속 국제가격 모니터")
st.caption("출처: 조달청 비축물자 사이트 (pps.go.kr) · CASH 기준 · 매일 자동 누적")

col_btn, col_upd, col_info = st.columns([1, 1, 4])
with col_btn:
    refresh = st.button("🔄 최신 데이터 수집", use_container_width=True)
with col_upd:
    bulk = st.button("📥 과거 데이터 수집(30일)", use_container_width=True)
with col_info:
    st.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ── 데이터 수집 ───────────────────────────────────────────
if refresh or bulk:
    session = get_session()
    pages   = 20 if bulk else 1
    with st.spinner("데이터 수집 중..."):
        bbs_list    = crawl_list(session, pages=pages)
        progress    = st.progress(0)
        saved_count = 0
        debug_logs  = []

        for i, bbs_sn in enumerate(bbs_list):
            result     = crawl_detail(session, bbs_sn)
            price_date = result.get("price_date") if result else None
            data       = result.get("data")       if result else None
            log_msg    = f"bbsSn={bbs_sn} | price_date={price_date} | 품목수={len(data) if data else 0}"

            if price_date and data:
                save_to_gsheet(price_date, data)
                saved_count += 1
                log_msg += " ✅"
            else:
                log_msg += " ❌"

            debug_logs.append(log_msg)
            progress.progress((i + 1) / len(bbs_list))
            time.sleep(0.3)

    st.success(f"✅ {len(bbs_list)}건 시도 / {saved_count}건 저장 완료!")
    with st.expander("🔍 디버그 로그"):
        for log in debug_logs:
            st.text(log)
    st.cache_data.clear()

# ── Sheets 로드 ───────────────────────────────────────────
df_all = load_gsheet()

if df_all.empty:
    st.warning("⚠️ 저장된 데이터가 없습니다. 상단 **[최신 데이터 수집]** 버튼을 눌러주세요!")
    st.stop()

stats_df = calc_stats(df_all)

# ── 탭 구성 ───────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📌 오늘 시황", "📈 추이 차트", "📊 통계 분석"])

# ════════════════════════════════
# TAB 1 : 오늘 시황
# ════════════════════════════════
with tab1:
    st.markdown(generate_comment(stats_df))
    st.divider()

    st.subheader("💡 당일 Closing (CASH, USD/ton)")
    metal_stats = stats_df[stats_df["품목"].isin(METALS)]
    cols = st.columns(len(METALS))
    for i, (_, row) in enumerate(metal_stats.iterrows()):
        price = row.get("최신가(Closing)")
        chg   = row.get("전일대비(%)")
        try:
            if chg is not None and pd.notna(chg):
                delta_str   = f"{float(chg):+.2f}%"
                delta_color = "normal"
            else:
                delta_str, delta_color = "-", "off"
        except:
            delta_str, delta_color = "-", "off"
        cols[i].metric(
            label=row["품목"],
            value=f"${float(price):,.2f}" if pd.notna(price) and price is not None else "-",
            delta=delta_str,
            delta_color=delta_color
        )

    st.divider()
    st.subheader("📋 전월대비 분석 테이블")
    display_cols = ["품목", "최신가(Closing)", "전일대비(%)", "당월누적평균", "전월평균", "전월대비변동(%)", "기준일"]
    st.dataframe(
        stats_df[display_cols].style
        .applymap(color_val, subset=["전일대비(%)", "전월대비변동(%)"])
        .format(
            {
                "최신가(Closing)": "{:,.2f}",
                "전일대비(%)":     "{:+.2f}%",
                "당월누적평균":    "{:,.2f}",
                "전월평균":        "{:,.2f}",
                "전월대비변동(%)": "{:+.2f}%",
            },
            na_rep="-"
        ),
        use_container_width=True,
        hide_index=True
    )

    fx_row = stats_df[stats_df["품목"] == "환율"]
    if not fx_row.empty:
        st.divider()
        fx = fx_row.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(
            "💱 환율 (KRW/USD)",
            f"{float(fx['최신가(Closing)']):,.2f}" if pd.notna(fx['최신가(Closing)']) else "-",
            delta=f"{float(fx['전일대비(%)']):+.2f}%" if pd.notna(fx['전일대비(%)']) else None
        )
        c2.metric(
            "당월 누적 평균",
            f"{float(fx['당월누적평균']):,.2f}" if pd.notna(fx['당월누적평균']) else "-"
        )
        c3.metric(
            "전월 평균",
            f"{float(fx['전월평균']):,.2f}" if pd.notna(fx['전월평균']) else "-"
        )
        c4.metric(
            "전월대비 변동",
            f"{float(fx['전월대비변동(%)']):+.2f}%" if pd.notna(fx['전월대비변동(%)']) else "-"
        )

# ════════════════════════════════
# TAB 2 : 추이 차트
# ════════════════════════════════
with tab2:
    st.subheader("📈 품목별 Closing 가격 추이")

    selected = st.multiselect("품목 선택", options=METALS, default=["구리", "알루미늄", "니켈"])
    period   = st.radio("기간", ["1개월", "3개월", "전체"], horizontal=True)

    today = df_all["날짜"].max()

    if selected:
        df_chart = df_all[df_all["품목"].isin(selected)].copy()
        if period == "1개월":
            df_chart = df_chart[df_chart["날짜"] >= today - pd.Timedelta(days=30)]
        elif period == "3개월":
            df_chart = df_chart[df_chart["날짜"] >= today - pd.Timedelta(days=90)]
        pivot = df_chart.pivot_table(index="날짜", columns="품목", values="당일Closing")
        st.line_chart(pivot, use_container_width=True)

    st.divider()
    st.subheader("💱 환율 추이 (KRW/USD)")
    df_fx = df_all[df_all["품목"] == "환율"].set_index("날짜")[["당일Closing"]].copy()
    df_fx.columns = ["환율(KRW/USD)"]
    if period == "1개월":
        df_fx = df_fx[df_fx.index >= today - pd.Timedelta(days=30)]
    elif period == "3개월":
        df_fx = df_fx[df_fx.index >= today - pd.Timedelta(days=90)]
    st.line_chart(df_fx, use_container_width=True)

# ════════════════════════════════
# TAB 3 : 통계 분석
# ════════════════════════════════
with tab3:
    st.subheader("📊 월별 평균가 비교")

    item_sel = st.selectbox("품목 선택", METALS + ["환율"])
    df_item  = df_all[df_all["품목"] == item_sel].copy()
    df_item["월"] = df_item["날짜"].dt.to_period("M").astype(str)

    monthly = df_item.groupby("월")["당일Closing"].mean().reset_index()
    monthly.columns = ["월", "월평균 Closing"]
    monthly["월평균 Closing"] = monthly["월평균 Closing"].round(2)
    monthly["전월대비(%)"] = monthly["월평균 Closing"].pct_change().mul(100).round(2)

    st.bar_chart(monthly.set_index("월")["월평균 Closing"], use_container_width=True)
    st.dataframe(
        monthly.style
        .applymap(color_val, subset=["전월대비(%)"])
        .format({
            "월평균 Closing": "{:,.2f}",
            "전월대비(%)": lambda x: f"{x:+.2f}%" if pd.notna(x) else "-"
        }),
        use_container_width=True,
        hide_index=True
    )

    st.divider()
    st.subheader("📁 원본 데이터 다운로드")
    csv_export = df_all.copy()
    csv_export["날짜"] = csv_export["날짜"].dt.strftime("%Y-%m-%d")
    st.download_button(
        label="⬇️ CSV 다운로드",
        data=csv_export.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"metal_prices_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

st.divider()
st.caption("📌 CASH 기준 LME 가격 / 조달청 비축물자 사이트 자동 수집 / 비상업적 참고용")