from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd


def load_theme_price_file(path: Path | str) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"theme price file does not exist: {path}")
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
    elif path.suffix.lower() in {".parquet", ".pq"}:
        frame = pd.read_parquet(path)
    else:
        raise ValueError("theme price input must be CSV or Parquet")
    required = {"date", "symbol", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"theme price file missing columns: {sorted(missing)}")
    return frame


def fetch_live_theme_prices(
    symbols: Iterable[str],
    as_of: date,
    *,
    lookback_days: int = 550,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Fetch US daily close/volume through the project's optional AkShare boundary."""
    try:
        import akshare as ak
    except ImportError as exc:
        raise RuntimeError("akshare is required; install the research extra") from exc
    start = as_of - timedelta(days=lookback_days)
    rows: list[pd.DataFrame] = []
    errors: list[dict[str, str]] = []
    for symbol in sorted(set(symbols)):
        try:
            frame = ak.stock_us_daily(symbol=symbol, adjust="")
            required = {"date", "close", "volume"}
            missing = required - set(frame.columns)
            if missing:
                raise ValueError(f"provider columns missing: {sorted(missing)}")
            normalized = frame.loc[:, ["date", "close", "volume"]].copy()
            normalized["date"] = pd.to_datetime(normalized["date"])
            normalized = normalized.loc[
                (normalized["date"].dt.date >= start) & (normalized["date"].dt.date <= as_of)
            ]
            normalized["symbol"] = symbol
            if normalized.empty:
                raise ValueError("empty history")
            rows.append(normalized)
        except Exception as exc:  # One invalid symbol does not block the whole monitor.
            errors.append({"symbol": symbol, "error_type": type(exc).__name__})
    if not rows:
        raise RuntimeError("all theme market data requests failed")
    output = pd.concat(rows, ignore_index=True, sort=False)
    return output.loc[:, ["date", "symbol", "close", "volume"]], errors
