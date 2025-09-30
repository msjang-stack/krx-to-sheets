# krx_daily_to_sheet.py
from datetime import datetime, timedelta
import os, json
from typing import List
from dateutil.relativedelta import relativedelta
import pandas as pd
from pykrx import stock
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---- 환경변수 설정 ----
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")  # JSON 문자열
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]                         # 필수
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "daily_log")
TICKERS = [t.strip() for t in os.environ.get("TICKERS", "082270,358570,000250").split(",") if t.strip()]
RUN_DATE = os.environ.get("RUN_DATE")  # 예: "2025-09-29"

def authorize_from_json_str(json_str: str):
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    info = json.loads(json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    return gspread.authorize(creds)

def get_recent_trading_day(base_date: datetime) -> datetime:
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

def fetch_daily_for_ticker(date_str: str, ticker: str) -> dict:
    """Get OHLCV, investor net buys by type, and short-selling metrics for date/ticker
       - 컬럼명이 종목/시장에 따라 '거래대금(원)' 처럼 달라지는 경우를 대비해 유연 매칭
    """
    ohlcv = stock.get_market_ohlcv_by_date(date_str, date_str, ticker)
    if ohlcv is None or ohlcv.empty:
        raise RuntimeError(f"No OHLCV for {ticker} on {date_str}")

    row = ohlcv.iloc[0]

    def pick(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        # 공백 제거/괄호 등 정규화 후 매칭 시도
        norm = {col.replace(" ", "").replace("(원)", ""): col for col in df.columns}
        for c in candidates:
            k = c.replace(" ", "").replace("(원)", "")
            if k in norm:
                return norm[k]
        return None

    # 각 항목별 후보 컬럼명
    open_col  = pick(ohlcv, ["시가"])
    high_col  = pick(ohlcv, ["고가"])
    low_col   = pick(ohlcv, ["저가"])
    close_col = pick(ohlcv, ["종가"])
    vol_col   = pick(ohlcv, ["거래량"])
    val_col   = pick(ohlcv, ["거래대금", "거래대금(원)"])

    # 필수 컬럼 체크 (가격/거래량은 없으면 의미가 없으니 에러)
    for need, nm in [("시가", open_col), ("고가", high_col), ("저가", low_col), ("종가", close_col), ("거래량", vol_col)]:
        if nm is None:
            raise RuntimeError(f"Missing required column for {ticker}: {need} (available: {list(ohlcv.columns)})")

    # value(거래대금)은 ETF/특정 경우에 표기 다를 수 있어 None 허용
    value_val = None
    if val_col is not None:
        try:
            value_val = int(row[val_col])
        except Exception:
            value_val = None  # 안전하게 통과

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

    # 투자주체 순매수(거래대금) — 없으면 None
    inv = stock.get_trading_value_by_date(date_str, date_str, ticker)
    if inv is not None and not inv.empty:
        iv = inv.reset_index().iloc[0].to_dict()
        rec["net_individual"] = int(iv.get("개인", 0))
        rec["net_foreign"] = int(iv.get("외국인", 0))
        rec["net_institution"] = int(iv.get("기관합계", 0))
    else:
        rec["net_individual"] = rec["net_foreign"] = rec["net_institution"] = None

    # 공매도 — 비어있을 수 있음(ETF 등)
    short_df = stock.get_shorting_status_by_date(date_str, date_str, ticker)
    rec["short_qty"] = rec["short_value"] = rec["short_ratio"] = None
    if short_df is not None and not short_df.empty:
        srow = short_df.iloc[0]
        def pick_s(cols):
            for c in cols:
                if c in short_df.columns:
                    return c
            norm = {col.replace(" ", ""): col for col in short_df.columns}
            for c in cols:
                k = c.replace(" ", "")
                if k in norm:
                    return norm[k]
            return None
        qty_col   = pick_s(["공매도 거래량", "공매도수량", "거래량"])
        amt_col   = pick_s(["공매도 거래대금", "공매도거래대금", "거래대금"])
        ratio_col = pick_s(["공매도 비중", "공매도비중", "비중"])
        if qty_col:   rec["short_qty"] = int(srow[qty_col])
        if amt_col:   rec["short_value"] = int(srow[amt_col])
        if ratio_col:
            try:
                rec["short_ratio"] = float(srow[ratio_col])
            except Exception:
                rec["short_ratio"] = None

    return rec


def ensure_worksheet(sh, name: str, header):
    try:
        ws = sh.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows="100", cols=str(len(header)))
        ws.append_row(header)
    first = ws.row_values(1)
    if first != header:
        if first:
            ws.delete_row(1)
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
            records.append(fetch_daily_for_ticker(date_str, t))
        except Exception as e:
            print(f"[WARN] {t}: {e}")

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
            print(f"Skip duplicate: {key}"); continue
        rows.append([r.get(h, "") for h in header])
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    print(f"Appended {len(rows)} rows for {trade_day.strftime('%Y-%m-%d')}.")

if __name__ == "__main__":
    main()
