#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
finviz_elite_export_all_us_tickers_with_sector.py

Use ONE Finviz Elite export URL (must include &auth=) to fetch and merge
NASDAQ + NYSE + AMEX (optional OTC) and export tickers **with Sector**.

Defaults:
  - Requests columns: ticker,sector
  - De-duplicates by ticker (case-insensitive)
  - Saves CSV and Excel

Examples:
  python finviz_elite_export_all_us_tickers_with_sector.py --url "https://elite.finviz.com/export.ashx?v=111&auth=YOUR_TOKEN" --out-xlsx all_with_sector.xlsx

Advanced (custom columns):
  python finviz_elite_export_all_us_tickers_with_sector.py --url "<url>" --columns "ticker,sector,industry,marketCap"
"""

import argparse
import io
import sys
import time
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import requests

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

EXCH_FILTERS = {
    "nasdaq": "exch_nasd",
    "nyse": "exch_nyse",
    "amex": "exch_amex",
    "otc": "exch_otc",
}

def ensure_export_endpoint(url: str) -> str:
    parts = urlparse(url)
    path = parts.path.replace("/screener.ashx", "/export.ashx")
    return urlunparse((parts.scheme, parts.netloc, path, parts.params, parts.query, parts.fragment))

def ensure_filter(url: str, exch_filter: str) -> str:
    """Append an exchange filter to &f= if missing."""
    parts = urlparse(url)
    q = parse_qs(parts.query, keep_blank_values=True)
    existing = q.get("f", [])
    current = existing[0] if existing else ""
    tokens = [t.strip() for t in current.split(",") if t.strip()]
    if exch_filter not in tokens:
        tokens.append(exch_filter)
    q["f"] = [",".join(tokens)] if tokens else [exch_filter]
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, urlencode(q, doseq=True), parts.fragment))

def set_columns(url: str, columns_csv: str) -> str:
    """Force &c= to requested columns (e.g., 'ticker,sector')."""
    parts = urlparse(url)
    q = parse_qs(parts.query, keep_blank_values=True)
    q["c"] = [columns_csv]
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, urlencode(q, doseq=True), parts.fragment))

def fetch_csv(url: str, retries: int = 3, backoff: float = 1.5, timeout: int = 30) -> bytes:
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            if r.status_code == 200:
                content = r.content
                if b"<html" in content.lower():
                    raise RuntimeError("Received HTML instead of CSV (check token/login).")
                return content
            elif r.status_code in (429, 403, 503):
                time.sleep(backoff ** (attempt + 1))
            else:
                r.raise_for_status()
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(backoff ** (attempt + 1))
            else:
                raise
    if last_exc:
        raise last_exc
    raise RuntimeError("Failed to fetch CSV")

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to TitleCase (Ticker, Sector, Industry, etc.)."""
    new_cols = {}
    for c in df.columns:
        new_cols[c] = c.strip().title()
    return df.rename(columns=new_cols)

def main():
    ap = argparse.ArgumentParser(description="Export ALL US tickers **with Sector** from Finviz Elite")
    ap.add_argument("--url", required=True, help="Elite export URL (must include &auth=...)")
    ap.add_argument("--include-otc", action="store_true", help="Also include OTC")
    ap.add_argument("--sleep", type=float, default=1.0, help="Sleep seconds between requests")
    ap.add_argument("--columns", default="ticker,sector",
                    help="Columns to request via &c= (default: 'ticker,sector'). "
                         "You can add 'industry,marketCap,price' etc.")
    ap.add_argument("--out-csv", default="all_us_tickers_with_sector.csv", help="Output CSV path")
    ap.add_argument("--out-xlsx", default="all_us_tickers_with_sector.xlsx", help="Output Excel path")
    args = ap.parse_args()

    base = ensure_export_endpoint(args.url)
    exchanges = ["nasdaq", "nyse", "amex"] + (["otc"] if args.include_otc else [])

    frames: List[pd.DataFrame] = []
    for ex in exchanges:
        url_ex = ensure_filter(base, EXCH_FILTERS[ex])
        url_cols = set_columns(url_ex, args.columns)

        csv_bytes = fetch_csv(url_cols)
        try:
            df = pd.read_csv(io.BytesIO(csv_bytes))
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(csv_bytes), encoding="latin-1")

        df = normalize_columns(df)

        # Basic sanity: Ensure we have Ticker column
        if "Ticker" not in df.columns:
            # Sometimes the column might be 'Symbol'
            if "Symbol" in df.columns:
                df = df.rename(columns={"Symbol": "Ticker"})
            else:
                print(f"[warn] Missing 'Ticker' column for {ex}. Columns: {list(df.columns)}", file=sys.stderr)
                continue

        # Tag the exchange for reference (optional but useful)
        df["Exchange"] = ex.upper()
        frames.append(df)
        print(f"[ok] {ex.upper()} rows: {len(df)}")
        time.sleep(args.sleep)

    if not frames:
        print("[error] No data fetched.", file=sys.stderr)
        sys.exit(2)

    merged = pd.concat(frames, ignore_index=True)

    # De-duplicate by Ticker (case-insensitive) and prefer non-null Sector
    merged["Ticker_upper"] = merged["Ticker"].astype(str).str.upper().str.strip()
    merged.sort_values(["Ticker_upper", "Sector"], ascending=[True, True], inplace=True, na_position="last")
    dedup = merged.drop_duplicates(subset=["Ticker_upper"], keep="first")

    # Final tidy
    # If user requested extra columns, keep them; otherwise keep Ticker, Sector, Exchange
    if args.columns:
        req_cols = [c.strip().title() for c in args.columns.split(",")]
        keep_cols = ["Ticker", "Sector", "Exchange"] + [c for c in req_cols if c not in ("Ticker", "Sector")]
        keep_cols = [c for c in keep_cols if c in dedup.columns]
    else:
        keep_cols = ["Ticker", "Sector", "Exchange"]

    out_df = dedup[keep_cols].reset_index(drop=True)

    # Save
    out_df.to_csv(args.out_csv, index=False)
    out_df.to_excel(args.out_xlsx, sheet_name="tickers", index=False)

    print(f"Saved {len(out_df)} unique tickers with sector to:")
    print(f"  - CSV : {args.out_csv}")
    print(f"  - XLSX: {args.out_xlsx}")


if __name__ == "__main__":
    main()
