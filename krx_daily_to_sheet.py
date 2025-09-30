# krx_daily_to_sheet.py  (pykrx 최신 함수명 반영 + 견고 매칭)
from datetime import datetime, timedelta
import os, json, re
from typing import Optional, Dict
from dateutil.relativedelta import relativedelta
import pandas as pd
from pykrx import stock
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---- 환경변수 ----
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")  # 서비스 계정 JSON(문자열)
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]                          # 필수
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "daily_log")
TICKERS = [t.strip() for t in os.environ.get("TICKERS", "082270,358570,000250").split(",") if t.strip()]
RUN_DATE = os.environ.get("RUN_DATE")  # 예: "2025-09-29"

# ---------------- 공통 유틸 ----------------
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

# ------------- 컬럼 매칭(견고) -------------
def _norm(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = s.replace(" ", "")
    s = re.sub(r"[\(\)\[\]{}％%원,.\-_/]", "", s)
    s = re.sub(r"\d+", "", s)
    return s

def pick_col(df: pd.DataFrame, candidates: list) -> Optional[str]:
    if df is None or df.empty: return None
    norm_map = {_norm(c): c for c in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
    for c in candidates:
        nc = _norm(c)
        if nc in norm_map:
            return norm_map[nc]
    for c in candidates:
        nc = _norm(c)
        for k, orig in norm_map.items():
            if nc and nc in k:
                return orig
    return None

# ------------- 데이터 수집 -------------
def _try_fetch_investor_value(date_str: str, ticker: str) -> Dict[str, Optional[int]]:
    """
    투자주체별 거래대금(순매수/매수/매도 중 기본은 순매수)을 조회.
    최신 pykrx는 get_market_trading_value_by_date 사용. (공식 리포 문서 확인)
    """
    out = {"개인": None, "외국인합계": None, "기관합계": None}

    try:
        # on='매수' 또는 '매도'도 가능. 기본(미지정)은 '순매수'
        df = stock.get_market_trading_value_by_date(date_str, date_str, ticker)  # ← 최신 함수명
        if df is not None and not df.empty:
            rec = df.reset_index().iloc[0].to_dict()
            for k in list(out.keys()):
                if k in rec:
                    out[k] = int(rec[k])
            return out
    except Exception as e:
        # 함수 미존재/티커 미지원 등은 None 유지
        print(f"[INFO] investor breakdown fetch failed for {ticker}: {e}")

    return out

def _try_fetch_short(date_str: str, ticker: str) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {"short_qty": None, "short_value": None, "short_ratio": None}
    try:
        df = stock.get_shorting_status_by_date(date_str, date_str, ticker)
    except Exception:
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
    if qty_col:   out["short_qty"]   = int(srow[qty_col])
    if amt_col:   out["short_value"] = int(srow[amt_col])
    if ratio_col:
        try: out["short_ratio"] = float(srow[ratio_col])
        except Exception: out["short_ratio"] = None
    return out

def fetch_daily_for_ticker(date_str: str, ticker: str) -> dict:
    """OHLCV + (가능 시) 투자주체 + (가능 시) 공매도"""
    ohlcv = stock.get_market_ohlcv_by_date(date_str, date_str, ticker)
    if ohlcv is None or ohlcv.empty:
        raise RuntimeError(f"No OHLCV for {ticker} on {date_str}")
    row = ohlcv.iloc[0]

    open_col  = pick_col(ohlcv, ["시가"])
    high_col  = pick_col(ohlcv, ["고가"])
    low_col   = pick_col(ohlcv, ["저가"])
    close_col = pick_col(ohlcv, ["종가"])
    vol_col   = pick_col(ohlcv, ["거래량"])
    val_col   = pick_col(ohlcv, ["거래대금", "거래대금(원)", "거래 대금", "거래대금(백만)"])

    missing = [(name, col) for name, col in [
        ("시가", open_col), ("고가", high_col), ("저가", low_col), ("종가", close_col), ("거래량", vol_col)
    ] if col is None]
    if missing:
        raise RuntimeError(
            f"Missing required OHLCV columns for {ticker}. "
            f"Need {[n for n,_ in missing]}, have {list(ohlcv.columns)}"
        )

    value_val = None
    if val_col is not None:
        try: value_val = int(row[val_col])
        except Exception: value_val = None

    rec = {
        "date": datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d"),
        "ticker": ticker,
        "open": int(row[open_col]),
        "high": int(row[high_col]),
        "low": int(row[low_col]),
        "close": int(row[close_col]),
        "volume": int(row[vol_col]),
        "value": value_val,
    }

    inv = _try_fetch_investor_value(date_str, ticker)
    rec["net_individual"]  = inv.get("개인")
    rec["net_foreign"]     = inv.get("외국인합계")
    rec["net_institution"] = inv.get("기관합계")

    rec.update(_try_fetch_short(date_str, ticker))
    return rec

# ------------- 시트 기록 -------------
def ensure_worksheet(sh, name: str, header):
    try:
        ws = sh.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows="100", cols=str(len(header)))
        ws.append_row(header)
    first = ws.row_values(1)
    if first != header:
        if first: ws.delete_row(1)
        ws.insert_row(header, 1)
    return ws

def existing_keys(ws) -> set:
    keys = set()
    data = ws.get_all_values()
    for row in data[1:]:
        if len(row) >= 2:
            keys.add(f"{row[0]}|{row[1]}")
    return keys

def main():
    base = datetime.strptime(RUN_DATE, "%Y-%m-%d") if RUN_DATE else datetime.now()
    trade_day = get_recent_trading_day(base)
    date_str = trade_day.strftime("%Y%m%d")

    records = []
    for t in TICKERS:
        try:
            r = fetch_daily_for_ticker(date_str, t)
            records.append(r)
        except Exception as e:
            try:
                df_dbg = stock.get_market_ohlcv_by_date(date_str, date_str, t)
                cols = list(df_dbg.columns) if df_dbg is not None else []
            except Exception:
                cols = []
            print(f"[WARN] {t}: {e} | OHLCV columns: {cols}")

    if not records:
        print("No records to write."); return

    gc = authorize_from_json_str(SERVICE_ACCOUNT_JSON)
    sh = gc.open_by_key(SPREADSHEET_ID)

    header = [
        "date","ticker","open","high","low","close",
        "volume","value",
        "net_individual","net_foreign","net_institution",
        "short_qty","short_value","short_ratio",
    ]
    ws = ensure_worksheet(sh, WORKSHEET_NAME, header)
    seen = existing_keys(ws)

    rows = []
    for r in records:
        key = f"{r['date']}|{r['ticker']}"
        if key in seen:
            print(f"Skip duplicate: {key}")
            continue
        rows.append([r.get(h, "") for h in header])
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    print(f"Appended {len(rows)} rows for {trade_day.strftime('%Y-%m-%d')}.")

if __name__ == "__main__":
    main()
