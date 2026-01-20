#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
polygon_ohlcv_ndays.py

Fetch daily OHLCV from Polygon.io and compute:
- Latest day's open/high/low/close/volume/dollar_volume
- N-day return: close_t / close_{t-N} - 1

Usage
-----
# 1) Set your API key safely (do NOT hardcode in code)
# macOS/Linux:
#   export POLYGON_API_KEY="YOUR_KEY"
# Windows (Powershell):
#   setx POLYGON_API_KEY "YOUR_KEY"

# 2) Install deps
#   pip install requests pandas openpyxl

# 3) Run
#   python polygon_ohlcv_ndays.py --ticker AAPL --ndays 20
#   python polygon_ohlcv_ndays.py --ticker TSLA --start 2024-01-01 --end 2025-10-09

Notes
-----
- Uses Aggregates (Bars) endpoint: /v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}
- Automatically requests adjusted bars (?adjusted=true) and sorts ascending.
- Saves CSV and Excel to: ohlcv_<TICKER>.csv/.xlsx
"""

import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

import pandas as pd
import requests


def fetch_polygon_daily(ticker: str, start: Optional[str], end: Optional[str], adjusted: bool = True) -> pd.DataFrame:
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY environment variable is not set.")

    if not end:
        end = datetime.now(timezone.utc).date().isoformat()
    if not start:
        # default ~2y history to ensure we can compute N-day returns
        start = (datetime.now(timezone.utc) - timedelta(days=800)).date().isoformat()

    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker.upper()}/range/1/day/{start}/{end}"
    params = {
        "adjusted": "true" if adjusted else "false",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key,
    }
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Polygon HTTP {r.status_code}: {r.text[:300]}")
    js = r.json()
    if js.get("status") != "OK" or not js.get("results"):
        raise RuntimeError(f"No results for {ticker} between {start} and {end}. Response: {js}")

    rows: List[Dict[str, Any]] = []
    for it in js["results"]:
        rows.append({
            "date": datetime.utcfromtimestamp(it["t"]/1000).date(),
            "open": float(it["o"]),
            "high": float(it["h"]),
            "low": float(it["l"]),
            "close": float(it["c"]),
            "volume": int(it["v"]),
        })
    df = pd.DataFrame(rows).set_index("date").sort_index()
    return df


def compute_summary(df: pd.DataFrame, ndays: int) -> Dict[str, Any]:
    last = df.iloc[-1]
    out = {
        "as_of": df.index[-1].isoformat(),
        "open": float(last["open"]),
        "high": float(last["high"]),
        "low": float(last["low"]),
        "close": float(last["close"]),
        "volume": int(last["volume"]),
        "dollar_volume": float(last["close"]) * float(last["volume"]),
        "n_day_lookback": ndays,
        "n_day_base_date": None,
        "n_day_return": None,
    }
    if ndays and ndays > 0 and len(df) > ndays:
        base_close = float(df["close"].iloc[-1 - ndays])
        out["n_day_base_date"] = df.index[-1 - ndays].isoformat()
        out["n_day_return"] = float(last["close"]) / base_close - 1.0
    return out


def main():
    ap = argparse.ArgumentParser(description="Fetch OHLCV from Polygon.io and compute N-day return")
    ap.add_argument("--ticker", required=True, help="Symbol, e.g., AAPL")
    ap.add_argument("--ndays", type=int, default=20, help="Lookback days for return (trading days)")
    ap.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    ap.add_argument("--unadjusted", action="store_true", help="Use unadjusted bars (default adjusted)")
    args = ap.parse_args()

    df = fetch_polygon_daily(args.ticker, args.start, args.end, adjusted=not args.unadjusted)

    # Save table with computed dollar_volume
    out = df.copy()
    out["dollar_volume"] = out["close"] * out["volume"]
    csv_path = f"ohlcv_{args.ticker.upper()}.csv"
    xlsx_path = f"ohlcv_{args.ticker.upper()}.xlsx"
    out.to_csv(csv_path, index=True)
    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xlw:
            out.to_excel(xlw, sheet_name="ohlcv", index=True)
    except Exception:
        pass

    # Summary
    s = compute_summary(df, args.ndays)
    def money(x): return f"${x:,.2f}"
    def pct(x): return "N/A" if x is None else f"{x:+.2%}"

    print(f"[{args.ticker.upper()}] as of {s['as_of']}: "
          f"O={money(s['open'])}, H={money(s['high'])}, L={money(s['low'])}, "
          f"C={money(s['close'])}, Vol={s['volume']:,}, $Vol={money(s['dollar_volume'])}")
    print(f"N-day return (N={s['n_day_lookback']} from {s['n_day_base_date']}): {pct(s['n_day_return'])}")
    print(f"Saved: {csv_path} and {xlsx_path}")


if __name__ == "__main__":
    main()
