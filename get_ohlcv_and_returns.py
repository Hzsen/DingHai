#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
get_ohlcv_and_returns.py

Fetch OHLCV for a given ticker and compute:
- open, high, low, close, volume, dollar_volume for a chosen date (latest by default)
- N-day return (close_t / close_{t-N} - 1)

Backends:
  - yfinance (default; no api key required)
  - polygon  (set env POLYGON_API_KEY)

Usage:
  pip install yfinance pandas openpyxl requests
  python get_ohlcv_and_returns.py --ticker AAPL --ndays 20
"""
import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

def fetch_yfinance(ticker: str, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    import yfinance as yf
    df = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=False)
    if df.empty:
        raise RuntimeError(f"No data from yfinance for {ticker}.")
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
    df.index = pd.to_datetime(df.index).tz_localize(None)
    return df[["open", "high", "low", "close", "volume"]]

def fetch_polygon(ticker: str, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    import requests
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY environment variable not set.")
    if not start:
        start = (datetime.now(timezone.utc) - timedelta(days=730)).date().isoformat()
    if not end:
        end = datetime.now(timezone.utc).date().isoformat()
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}
    import requests
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Polygon error {r.status_code}: {r.text[:300]}")
    js = r.json()
    if js.get("status") != "OK" or not js.get("results"):
        raise RuntimeError(f"Polygon returned no results for {ticker}: {js}")
    rows = [{
        "date": datetime.utcfromtimestamp(it["t"]/1000).date().isoformat(),
        "open": it["o"],
        "high": it["h"],
        "low": it["l"],
        "close": it["c"],
        "volume": it["v"],
    } for it in js["results"]]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return df

def compute_metrics(df: pd.DataFrame, ndays: int) -> dict:
    out = {}
    last = df.iloc[-1]
    out["as_of"] = df.index[-1].date().isoformat()
    out["open"] = float(last["open"])
    out["high"] = float(last["high"])
    out["low"] = float(last["low"])
    out["close"] = float(last["close"])
    out["volume"] = int(last["volume"])
    out["dollar_volume"] = float(last["close"]) * float(last["volume"])
    if ndays is not None and ndays > 0 and len(df) > ndays:
        prev_close = float(df["close"].iloc[-1 - ndays])
        out["n_day_return"] = float(last["close"]) / prev_close - 1.0
        out["n_day_lookback"] = ndays
        out["n_day_base_date"] = df.index[-1 - ndays].date().isoformat()
    else:
        out["n_day_return"] = None
        out["n_day_lookback"] = ndays
        out["n_day_base_date"] = None
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", required=True, help="Ticker symbol, e.g., AAPL")
    ap.add_argument("--ndays", type=int, default=20, help="Lookback days for return")
    ap.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    ap.add_argument("--backend", choices=["yfinance", "polygon"], default="yfinance")
    args = ap.parse_args()

    if args.backend == "yfinance":
        df = fetch_yfinance(args.ticker, args.start, args.end)
    else:
        df = fetch_polygon(args.ticker, args.start, args.end)

    out_df = df.copy()
    out_df["dollar_volume"] = out_df["close"] * out_df["volume"]
    csv_path = f"ohlcv_{args.ticker.upper()}.csv"
    xlsx_path = f"ohlcv_{args.ticker.upper()}.xlsx"
    out_df.to_csv(csv_path)
    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xlw:
            out_df.to_excel(xlw, sheet_name="ohlcv")
    except Exception:
        pass

    metrics = compute_metrics(df, args.ndays)
    def fmt_money(x): return f"${x:,.2f}"
    def fmt_pct(x): return ("{:+.2%}".format(x)) if x is not None else "N/A"

    print(f"[{args.ticker.upper()}] as of {metrics['as_of']}: "
          f"O={fmt_money(metrics['open'])}, H={fmt_money(metrics['high'])}, "
          f"L={fmt_money(metrics['low'])}, C={fmt_money(metrics['close'])}, "
          f"Vol={metrics['volume']:,}, $Vol={fmt_money(metrics['dollar_volume'])}")
    print(f"N-day return (N={metrics['n_day_lookback']} from {metrics['n_day_base_date']}): "
          f"{fmt_pct(metrics['n_day_return'])}")
    print(f"Saved: {csv_path} and {xlsx_path}")

if __name__ == "__main__":
    main()
