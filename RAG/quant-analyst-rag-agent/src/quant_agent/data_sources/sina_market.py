from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import pandas as pd


def normalize_sina_spot(raw: pd.DataFrame, as_of: date) -> pd.DataFrame:
    """Normalize Sina's all-A-share snapshot without inventing historical fields."""
    required = {"代码", "名称", "最新价", "昨收", "今开", "最高", "最低", "成交量", "成交额", "时间戳"}
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Sina spot missing columns: {sorted(missing)}")
    frame = raw.rename(
        columns={
            "代码": "source_symbol", "名称": "name", "最新价": "close", "昨收": "prev_close",
            "今开": "open", "最高": "high", "最低": "low", "成交量": "volume",
            "成交额": "amount", "时间戳": "source_timestamp",
        }
    ).copy()
    frame = frame.loc[frame["source_symbol"].str.match(r"^(sh|sz)\d{6}$", na=False)].copy()
    suffix = frame["source_symbol"].str[:2].map({"sh": "SH", "sz": "SZ"})
    frame["ticker"] = frame["source_symbol"].str[2:] + "." + suffix
    numeric = ["close", "prev_close", "open", "high", "low", "volume", "amount"]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["date"] = pd.Timestamp(as_of)
    frame["return_1d"] = frame["close"] / frame["prev_close"] - 1
    spread = frame["high"] - frame["low"]
    frame["intraday_close_location"] = ((frame["close"] - frame["low"]) / spread).where(spread > 0, 0.5)
    frame["close_vs_open"] = frame["close"] / frame["open"] - 1
    frame["amount_rank_market"] = frame["amount"].rank(method="min", ascending=False)
    frame["is_st"] = frame["name"].astype(str).str.upper().str.contains(r"(?:^|\*)ST", regex=True)
    frame["is_new_listing_name"] = frame["name"].astype(str).str.startswith(("N", "C"))
    return frame.sort_values("ticker").reset_index(drop=True)


def prefilter_repair_universe(spot: pd.DataFrame, max_symbols: int = 500) -> pd.DataFrame:
    """Cheap current-day prefilter before expensive historical requests.

    This is intentionally broad: it removes untradeable/illiquid names and weak
    closes, then keeps a union-like ranking of liquidity, recovery and close quality.
    """
    valid = spot.loc[
        (~spot["is_st"])
        & (~spot["is_new_listing_name"])
        & (spot["close"] > 0)
        & (spot["volume"] > 0)
        & (spot["amount"] >= 50_000_000)
        & (spot["return_1d"] >= -0.01)
        & (spot["intraday_close_location"] >= 0.55)
    ].copy()
    if valid.empty:
        return valid
    valid["liquidity_pct"] = valid["amount"].rank(pct=True)
    valid["recovery_pct"] = valid["return_1d"].rank(pct=True)
    valid["prefilter_score"] = (
        0.45 * valid["liquidity_pct"]
        + 0.35 * valid["recovery_pct"]
        + 0.20 * valid["intraday_close_location"].clip(0, 1)
    )
    # Preserve a capacity core so large institutional names are not displaced by
    # hundreds of small caps with a larger one-day bounce.
    capacity_slots = min(len(valid), max_symbols, max(100, max_symbols // 2))
    capacity_core = valid.nsmallest(capacity_slots, "amount_rank_market")
    remaining = valid.loc[~valid.index.isin(capacity_core.index)].nlargest(
        max(0, max_symbols - len(capacity_core)), "prefilter_score"
    )
    return pd.concat([capacity_core, remaining]).sort_values("prefilter_score", ascending=False).reset_index(drop=True)


def fetch_sina_histories(
    candidates: pd.DataFrame,
    as_of: date,
    lookback_days: int = 430,
    workers: int = 8,
    retries: int = 2,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Fetch Tencent qfq OHLCV for a bounded universe and append Sina spot day.

    Tencent's returned ``amount`` field is share volume in lots, not monetary
    turnover. It is normalized to shares and intentionally not presented as
    historical transaction amount. Current cross-sectional amount rank still comes
    from the full Sina snapshot.
    """
    import akshare as ak

    start = (as_of - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end = as_of.strftime("%Y%m%d")

    def fetch_one(row: object) -> tuple[pd.DataFrame | None, dict[str, str] | None]:
        source_symbol = str(getattr(row, "source_symbol"))
        ticker = str(getattr(row, "ticker"))
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                history = ak.stock_zh_a_hist_tx(symbol=source_symbol, start_date=start, end_date=end, adjust="qfq")
                if history.empty:
                    raise ValueError("empty history")
                history = history.rename(columns={"amount": "volume"}).copy()
                history["volume"] = pd.to_numeric(history["volume"], errors="coerce") * 100.0
                history["amount"] = pd.NA
                history["turnover_rate"] = 0.0
                history["date"] = pd.to_datetime(history["date"])
                history["ticker"] = ticker
                history["name"] = str(getattr(row, "name"))
                history["amount_rank_market"] = float(getattr(row, "amount_rank_market"))
                if history["date"].max().date() < as_of:
                    today = pd.DataFrame(
                        [{
                            "date": pd.Timestamp(as_of), "open": float(getattr(row, "open")),
                            "high": float(getattr(row, "high")), "low": float(getattr(row, "low")),
                            "close": float(getattr(row, "close")), "volume": float(getattr(row, "volume")),
                            "amount": float(getattr(row, "amount")), "turnover_rate": 0.0,
                            "ticker": ticker, "name": str(getattr(row, "name")),
                            "amount_rank_market": float(getattr(row, "amount_rank_market")),
                        }]
                    )
                    history = pd.concat([history, today], ignore_index=True, sort=False)
                return history, None
            except Exception as exc:  # the caller receives bounded error metadata
                last_error = exc
                if attempt < retries:
                    time.sleep(0.25 * (2**attempt))
        return None, {"ticker": ticker, "error_type": type(last_error).__name__}

    frames: list[pd.DataFrame] = []
    errors: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one, row): row.ticker for row in candidates.itertuples(index=False)}
        for future in as_completed(futures):
            frame, error = future.result()
            if frame is not None:
                frames.append(frame)
            if error is not None:
                errors.append(error)
    if not frames:
        return pd.DataFrame(), errors
    return pd.concat(frames, ignore_index=True, sort=False), sorted(errors, key=lambda item: item["ticker"])
