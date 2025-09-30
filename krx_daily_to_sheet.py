# krx_daily_to_sheet.py
# 기능: 종목별 시트에 날짜별 OHLCV(시가, 고가, 저가, 종가, 거래량, 등락률)만 누적 기록
# - 종목별 워크시트 자동 생성 (제목: "종목코드 종목명")
# - 헤더: ["날짜","종목코드","종목명","시가","고가","저가","종가","거래량","등락률"]
# - 같은 날짜는 한 번만 기록(중복 방지)
# - 예외 발생 시에도 액션이 깨지지 않도록 항상 exit 0

from datetime import datetime, timedelta
import os, json, re, sys, traceback
from typing import Optional, Dict, Any, List

import pandas as pd
from pykrx import stock
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------- 환경변수 ----------
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")      # 서비스 계정 JSON(문자열)
SPREADSHEET_ID      = os.environ.get("SPREADSHEET_ID")                    # 필수
TICKERS             = [t.strip() for t in os.environ.get("TICKERS", "082270,358570,000250").split(",") if t.strip()]
RUN_DATE            = os.environ.get("RUN_DATE")  # 예: "2025-09-29" (테스트용)

# ---------- 공용 ----------
def authorize_from_json_str(json_str: str):
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    info = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    return gspread.authorize(creds)

def get_recent_trading_day(base_date: datetime) -> datetime:
    """기준일 기준 가장 가까운 거래일(과거)을 찾기 (주말/휴일 보정)"""
    d = base_date
    for _ in range(20):
        s = d.strftime("%Y%m%d")
        try:
            df = stock.get_index_ohlcv_by_date(s, s, "1001")  # 1001=KOSPI
            if df is not None and not df.empty:
                return d
        except Exception:
            pass
        d -= timedelta(days=1)
    return d

# ---------- 컬럼 매칭(견고) ----------
def _norm(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = s.replace(" ", "")
    s = re.sub(r"[\(\)\[\]{}％%원,.\-_/]", "", s)
    s = re.sub(r"\d+", "", s)
    return s

def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df is None or df.empty: return None
    norm_map = {_norm(c): c for c in df.columns}
    # 1) 정확히 일치
    for c in candidates:
        if c in df.columns: return c
    # 2) 정규화 후 일치
    for c in candidates:
        nc = _norm(c)
        if nc in norm_map: return norm_map[nc]
    # 3) 부분 포함
    for c in candidates:
        nc = _norm(c)
        for k, orig in norm_map.items():
            if nc and nc in k: return orig
    return None

# ---------- 데이터 수집 ----------
def fetch_daily_for_ticker(date_str: str, ticker: str) -> Optional[Dict[str, Any]]:
    """OHLCV 6개 컬럼만 반환. 없으면 None."""
    ohlcv = stock.get_market_ohlcv_by_date(date_str, date_str, ticker)
    if ohlcv is None or ohlcv.empty:
        print(f"[INFO] {ticker}: 해당 날짜({date_str}) 시세 없음")
        return None

    # 견고한 컬럼 탐색
    open_col   = pick_col(ohlcv, ["시가"])
    high_col   = pick_col(ohlcv, ["고가"])
    low_col    = pick_col(ohlcv, ["저가"])
    close_col  = pick_col(ohlcv, ["종가"])
    volume_col = pick_col(ohlcv, ["거래량"])
    change_col = pick_col(ohlcv, ["등락률", "등락률(%)", "등락율"])  # 등락'율' 오타 대응

    needed = [("시가", open_col), ("고가", high_col), ("저가", low_col),
              ("종가", close_col), ("거래량", volume_col), ("등락률", change_col)]
    miss = [n for n, c in needed if c is None]
    if miss:
        print(f"[WARN] {ticker}: 필수 컬럼 누락 {miss} | 보유={list(ohlcv.columns)}")
        return None

    row = ohlcv.iloc[0]

    # 종목명
    try:
        ticker_name = stock.get_market_ticker_name(ticker) or ""
    except Exception:
        ticker_name = ""

    # 값 파싱
    def to_int(x):
        try: return int(x)
        except Exception: return None

    def to_float(x):
        try: return float(x)
        except Exception: return None

    change_val = to_float(row[change_col])
    if change_val is not None:
        change_val = round(change_val, 2)  # ✅ 소수 둘째 자리까지 반올림
    
    rec: Dict[str, Any] = {
        "날짜": datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d"),
        "종목코드": ticker,
        "종목명": ticker_name,
        "시가": to_int(row[open_col]),
        "고가": to_int(row[high_col]),
        "저가": to_int(row[low_col]),
        "종가": to_int(row[close_col]),
        "거래량": to_int(row[volume_col]),
        "등락률": change_val,
    }

    return rec

# ---------- 시트 기록 (종목별 시트) ----------
KR_HEADER = [
    "날짜","종목코드","종목명",
    "시가","고가","저가","종가","거래량","등락률",
]

def ensure_ticker_sheet(sh, ticker: str, name: str, header: List[str]):
    """종목별 워크시트를 (없으면) 생성하고 헤더 정렬."""
    title = f"{ticker} {name}".strip() if name else f"{ticker}"
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(len(header)))
        ws.append_row(header)
    first = ws.row_values(1)
    if first != header:
        if first:
            ws.delete_rows(1)
        ws.insert_row(header, 1)
    return ws

def existing_dates(ws) -> set:
    """종목별 시트는 날짜만 중복 방지 키로 사용"""
    keys = set()
    data = ws.get_all_values()
    for row in data[1:]:
        if len(row) >= 1 and row[0]:
            keys.add(row[0])
    return keys

def main() -> int:
    try:
        if not SERVICE_ACCOUNT_JSON or not SPREADSHEET_ID:
            print("[ERROR] GOOGLE_SERVICE_ACCOUNT_JSON 또는 SPREADSHEET_ID 누락")
            return 0  # 실패여도 exit 0

        base = datetime.strptime(RUN_DATE, "%Y-%m-%d") if RUN_DATE else datetime.now()
        trade_day = get_recent_trading_day(base)
        date_str = trade_day.strftime("%Y%m%d")

        gc = authorize_from_json_str(SERVICE_ACCOUNT_JSON)
        sh = gc.open_by_key(SPREADSHEET_ID)

        appended_total = 0
        for t in TICKERS:
            try:
                rec = fetch_daily_for_ticker(date_str, t)
            except Exception as e:
                traceback.print_exc()
                rec = None
            if not rec:
                continue

            ws = ensure_ticker_sheet(sh, t, rec.get("종목명", ""), KR_HEADER)
            seen = existing_dates(ws)
            key = rec["날짜"]
            if key in seen:
                print(f"Skip duplicate: {t} {rec.get('종목명','')} @ {key}")
                continue

            row = [rec.get(h, "") for h in KR_HEADER]
            ws.append_row(row, value_input_option="RAW")
            appended_total += 1

        if appended_total == 0:
            print("No records to write.")
        else:
            print(f"{appended_total}개 행이 추가되었습니다. 대상 거래일: {trade_day.strftime('%Y-%m-%d')}")
        return 0

    except Exception:
        traceback.print_exc()
        return 0

if __name__ == "__main__":
    sys.exit(main())
