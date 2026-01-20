#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
finviz_elite_export_all_us_tickers.py

Given ONE Finviz Elite export URL (containing your &auth= token), this script:
  - Builds export URLs for NASDAQ, NYSE, AMEX (and optionally OTC)
  - Requests each CSV
  - Extracts the ticker column
  - De-duplicates
  - Saves combined tickers to CSV and Excel

Examples:
  python finviz_elite_export_all_us_tickers.py --url "https://elite.finviz.com/export.ashx?v=111&auth=YOUR_TOKEN" --out-xlsx all_tickers.xlsx
  python finviz_elite_export_all_us_tickers.py --url "https://elite.finviz.com/export.ashx?v=111&f=sec_technology&auth=YOUR_TOKEN" --include-otc

Notes:
  * Your URL may already include &f=... filters. We will **append** exchange filters
    to ensure we fetch each exchange. If &c= is not present, we request all columns
    and then auto-detect the ticker column ("Ticker" or "Symbol").
  * Keep your token secret.
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

def update_query_param(url: str, new_params: Dict[str, str | List[str]], append: bool = False) -> str:
    """Update or append query parameters in a URL."""
    parts = urlparse(url)
    q = parse_qs(parts.query, keep_blank_values=True)
    if append:
        for k, v in new_params.items():
            if isinstance(v, list):
                q.setdefault(k, [])
                q[k].extend(v)
            else:
                q.setdefault(k, [])
                q[k].append(v)
    else:
        for k, v in new_params.items():
            q[k] = v if isinstance(v, list) else [v]
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def ensure_filter(url: str, exch_filter: str) -> str:
    """
    Ensure &f= contains the given exchange filter by appending it (comma-separated).
    We do not remove existing filters; we only append if missing.
    """
    parts = urlparse(url)
    q = parse_qs(parts.query, keep_blank_values=True)
    f_vals = q.get("f", [])
    existing = f_vals[0] if f_vals else ""
    tokens = [t.strip() for t in existing.split(",") if t.strip()]
    if exch_filter not in tokens:
        tokens.append(exch_filter)
    new_f = ",".join(tokens) if tokens else exch_filter
    q["f"] = [new_f]
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def fetch_csv(url: str, retries: int = 3, backoff: float = 1.5, timeout: int = 30) -> bytes:
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
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(backoff ** (attempt + 1))
    raise RuntimeError("Failed to fetch CSV")


def get_ticker_column(df: pd.DataFrame) -> Optional[str]:
    for cand in ["Ticker", "Symbol", "ticker", "symbol"]:
        if cand in df.columns:
            return cand
    # sometimes first column is ticker for minimal exports
    if df.shape[1] >= 1 and df.columns[0].lower() in ("", "unnamed: 0"):
        return df.columns[1] if df.shape[1] > 1 else df.columns[0]
    return None


def main():
    ap = argparse.ArgumentParser(description="Export ALL US tickers from Finviz Elite (NASDAQ+NYSE+AMEX, optional OTC)")
    ap.add_argument("--url", required=True, help="Your Elite export URL (must include &auth=...)")
    ap.add_argument("--include-otc", action="store_true", help="Also include OTC (exch_otc)")
    ap.add_argument("--sleep", type=float, default=1.0, help="Sleep seconds between requests")
    ap.add_argument("--out-csv", default="all_us_tickers.csv", help="Output CSV path for tickers (one per line)")
    ap.add_argument("--out-xlsx", default="all_us_tickers.xlsx", help="Output Excel path")
    args = ap.parse_args()

    exchanges = ["nasdaq", "nyse", "amex"]
    if args.include_otc:
        exchanges.append("otc")

    tickers: List[str] = []
    for ex in exchanges:
        exch_filter = EXCH_FILTERS[ex]

        # Ensure we are calling the export endpoint
        parts = urlparse(args.url)
        path = parts.path.replace("/screener.ashx", "/export.ashx")
        url = urlunparse((parts.scheme, parts.netloc, path, parts.params, parts.query, parts.fragment))

        # Ensure f includes the exchange
        url_with_ex = ensure_filter(url, exch_filter)

        # (Optional) request only ticker column by appending &c=ticker if not present
        parts2 = urlparse(url_with_ex)
        q2 = parse_qs(parts2.query, keep_blank_values=True)
        if "c" not in q2:
            q2["c"] = ["ticker"]
        new_query = urlencode(q2, doseq=True)
        final_url = urlunparse((parts2.scheme, parts2.netloc, parts2.path, parts2.params, new_query, parts2.fragment))

        csv_bytes = fetch_csv(final_url)
        try:
            df = pd.read_csv(io.BytesIO(csv_bytes))
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(csv_bytes), encoding="latin-1")

        col = get_ticker_column(df)
        if not col:
            print(f"[warn] Could not detect ticker column for {ex}. Columns: {list(df.columns)}", file=sys.stderr)
            continue
        ex_tickers = df[col].astype(str).str.upper().str.strip().tolist()
        tickers.extend(ex_tickers)

        print(f"[ok] {ex.upper()}: {len(ex_tickers)} tickers")
        time.sleep(args.sleep)

    # De-duplicate and clean
    uniq = sorted(set([t for t in tickers if t and t != "nan"]))

    # Save CSV
    with open(args.out_csv, "w", encoding="utf-8") as f:
        for t in uniq:
            f.write(t + "\n")

    # Save Excel
    out_df = pd.DataFrame({"ticker": uniq})
    out_df.to_excel(args.out_xlsx, sheet_name="tickers", index=False)

    print(f"Saved {len(uniq)} unique tickers to:")
    print(f"  - CSV : {args.out_csv}")
    print(f"  - XLSX: {args.out_xlsx}")


if __name__ == "__main__":
    main()
