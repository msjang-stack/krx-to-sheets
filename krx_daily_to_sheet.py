# krx_daily_to_sheet.py  (KR: 종목별 시트, 한글 헤더, 종목명 추가 / 안정판)
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
# WORKSHEET_NAME는 사용하지 않음(종목별로 시트 생성)
TICKERS             = [t.strip() for t in os.environ.get("TICKERS", "082270,358570,000250").split(",") if t.strip()]
RUN_DATE            = os.environ.get("RUN_DATE")  # 예: "2025-09-29" (테스트용)
INCLUDE_INVESTOR    = os.environ.get("INCLUDE_INVESTOR", "1") == "1"
INCLUDE_SHORT       = os.environ.get("INCLUDE_SHORT", "1") == "1"

# ---------- 공용 유틸 ----------
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
    for c in candidates:
        if c in df.columns: return c
    for c in candidates:
        nc = _norm(c)
        if nc in norm_map: return norm_map[nc]
    for c in candidates:
        nc = _norm(c)
        for k, orig in norm_map.items():
            if nc and nc in k: return orig
    return None

# ---------- 데이터 수집 ----------
def try_fetch_investor(date_str: str, ticker: str) -> Dict[str, Optional[int]]:
    """
    투자주체(개인/외국인합계/기관합계). pykrx 버전에 따라 종목 단위 API가 없을 수 있으므로
    실패해도 None으로 반환하고 절대 예외로 터뜨리지 않음.
    """
    out = {"개인": None, "외국인합계": None, "기관합계": None}
    if not INCLUDE_INVESTOR:
        return out
    # 후보 함수(버전별 상이 가능) — 실패해도 통과
    try:
        if hasattr(stock, "get_trading_value_by_date"):
            df = stock.get_trading_value_by_date(date_str, date_str, ticker)
            if df is not None and not df.empty:
                rec = df.reset_index().iloc[0].to_dict()
                for k in out.keys():
                    if k in rec:
                        out[k] = int(rec[k])
                return out
    except Exception as e:
        print(f"[INFO] investor via get_trading_value_by_date failed: {e}")
    try:
        if hasattr(stock, "get_market_trading_value_by_date"):
            # 시장 단위 집계일 수 있어 종목별 의미가 없을 수 있음. 여기서는 사용하지 않음.
            pass
    except Exception as e:
        print(f"[INFO] investor via get_market_trading_value_by_date failed: {e}")
    print(f"[INFO] investor breakdown not available for {ticker} on {date_str}.")
    return out

def try_fetch_short(date_str: str, ticker: str) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {"공매도수량": None, "공매도거래대금": None, "공매도비중": None}
    if not INCLUDE_SHORT:
        return out
    try:
        df = stock.get_shorting_status_by_date(date_str, date_str, ticker)
    except Exception as e:
        print(f"[INFO] shorting fetch failed pre: {e}")
        df = None
    if df is None or df.empty:
        return out
    srow = df.iloc[0]
    def pick_s(cols):
        for c in cols:
            if c in df.columns:
                return c
        norm = {c.replace(" ", ""): c for c in df.columns}
        for c in cols:
            k = c.replace(" ", "")
            if k in norm: return norm[k]
        return None
    qty_col   = pick_s(["공매도 거래량", "공매도수량", "거래량"])
    amt_col   = pick_s(["공매도 거래대금", "공매도거래대금", "거래대금", "거래대금(원)"])
    ratio_col = pick_s(["공매도 비중", "공매도비중", "비중"])
    if qty_col:   out["공매도수량"]     = int(srow[qty_col])
    if amt_col:   out["공매도거래대금"] = int(srow[amt_col])
    if ratio_col:
        try: out["공매도비중"] = float(srow[ratio_col])
        except Exception: out["공매도비중"] = None
    return out

def fetch_daily_for_ticker(date_str: str, ticker: str) -> Optional[Dict[str, Any]]:
    """OHLCV 필수 + (가능 시) 투자주체/공매도. 실패해도 None 반환."""
    try:
        ohlcv = stock.get_market_ohlcv_by_date(date_str, date_str, ticker)
        if ohlcv is None or ohlcv.empty:
            print(f"[WARN] {ticker}: 해당 날짜({date_str})의 시세 데이터가 없습니다.")
            return None

        row = ohlcv.iloc[0]
        open_col  = pick_col(ohlcv, ["시가"])
        high_col  = pick_col(ohlcv, ["고가"])
        low_col   = pick_col(ohlcv, ["저가"])
        close_col = pick_col(ohlcv, ["종가"])
        vol_col   = pick_col(ohlcv, ["거래량"])
        val_col   = pick_col(ohlcv, ["거래대금", "거래대금(원)", "거래 대금", "거래대금(백만)"])

        # 필수 5개가 없으면 스킵(가격/거래량만 필수)
        needed = [("시가", open_col), ("고가", high_col), ("저가", low_col), ("종가", close_col), ("거래량", vol_col)]
        missing = [n for n,c in needed if c is None]
        if missing:
            print(f"[WARN] {ticker}: 필수 컬럼 누락 {missing} | 보유={list(ohlcv.columns)}")
            return None

        value_val = None
        if val_col is not None:
            try:
                value_val = int(row[val_col])
            except Exception:
                value_val = None

        # 종목명
        try:
            ticker_name = stock.get_market_ticker_name(ticker)
        except Exception:
            ticker_name = ""

        rec: Dict[str, Any] = {
            "날짜": datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d"),
            "종목코드": ticker,
            "종목명": ticker_name or "",
            "시가": int(row[open_col]),
            "고가": int(row[high_col]),
            "저가": int(row[low_col]),
            "종가": int(row[close_col]),
            "거래량": int(row[vol_col]),
            "거래대금": value_val,
            "개인": None,
            "외국인합계": None,
            "기관합계": None,
            "공매도수량": None,
            "공매도거래대금": None,
            "공매도비중": None,
        }

        inv = try_fetch_investor(date_str, ticker)
        rec["개인"]      = inv.get("개인")
        rec["외국인합계"] = inv.get("외국인합계")
        rec["기관합계"]   = inv.get("기관합계")

        short = try_fetch_short(date_str, ticker)
        rec.update(short)

        return rec

    except Exception as e:
        print(f"[WARN] {ticker}: {e}")
        return None

# ---------- 시트 기록 (종목별 시트) ----------
KR_HEADER = [
    "날짜","종목코드","종목명",
    "시가","고가","저가","종가",
    "거래량","거래대금",
    "개인","외국인합계","기관합계",
    "공매도수량","공매도거래대금","공매도비중",
]

def ensure_ticker_sheet(sh, ticker: str, name: str, header: List[str]):
    """종목별 워크시트를 (없으면) 생성하고, 헤더 1행을 한글로 정렬."""
    title = f"{ticker} {name}".strip() if name else f"{ticker}"
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="2000", cols=str(len(header)))
        ws.append_row(header)
    first = ws.row_values(1)
    if first != header:
        if first:
            ws.delete_row(1)
        ws.insert_row(header, 1)
    return ws

def existing_dates(ws) -> set:
    """종목별 시트는 날짜만 중복 방지 키로 사용 (같은 날짜는 1회만 기록)"""
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

        # 종목별로 개별 시트에 기록
        appended_total = 0
        for t in TICKERS:
            rec = fetch_daily_for_ticker(date_str, t)
            if not rec:
                continue

            # 시트 준비
            name = rec.get("종목명", "") or ""
            ws = ensure_ticker_sheet(sh, t, name, KR_HEADER)

            # 중복 방지: 날짜만 키
            seen = existing_dates(ws)
            key = rec["날짜"]
            if key in seen:
                print(f"Skip duplicate: {t} {name} @ {key}")
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
