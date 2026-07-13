from __future__ import annotations

import time
from collections.abc import Callable

import pandas as pd


PRICE_COLUMN_MAP = {
    "日期": "date",
    "股票代码": "source_ticker",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "振幅": "amplitude_pct",
    "涨跌幅": "pct_change",
    "涨跌额": "price_change",
    "换手率": "turnover_rate_pct",
}
INDEX_COLUMN_MAP = {
    "日期": "date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
}


def _retry(call: Callable[[], pd.DataFrame], attempts: int = 3) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            result = call()
            if result.empty:
                raise ValueError("data source returned an empty frame")
            return result
        except Exception as exc:  # third-party adapters raise several exception types
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(2**attempt)
    assert last_error is not None
    raise RuntimeError(f"market data request failed after {attempts} attempts") from last_error


def _yyyymmdd(value: str | pd.Timestamp) -> str:
    return pd.Timestamp(value).strftime("%Y%m%d")


def _year_chunks(start_date: str, end_date: str) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if start > end:
        raise ValueError("start_date must not be after end_date")
    chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(pd.Timestamp(year=cursor.year, month=12, day=31), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + pd.Timedelta(days=1)
    return chunks


def fetch_phase0_prices(labels: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        import akshare as ak
    except ImportError as exc:  # pragma: no cover - depends on optional environment
        raise RuntimeError('Install research dependencies with: pip install -e ".[research]"') from exc

    frames: list[pd.DataFrame] = []
    stocks = labels[["ticker", "name"]].drop_duplicates().sort_values("ticker")
    for stock in stocks.itertuples(index=False):
        code, exchange = stock.ticker.split(".", maxsplit=1)
        if exchange not in {"SH", "SZ"}:
            raise ValueError(f"Phase 0 Sina adapter does not support exchange: {exchange}")
        try:
            raw = _retry(
                lambda code=code, exchange=exchange: ak.stock_zh_a_daily(
                    symbol=f"{exchange.lower()}{code}",
                    start_date=_yyyymmdd(start_date),
                    end_date=_yyyymmdd(end_date),
                    adjust="qfq",
                )
            )
            normalized = raw.copy()
            required = {"date", "open", "high", "low", "close", "volume", "amount", "turnover"}
            missing = required - set(normalized.columns)
            if missing:
                raise ValueError(f"AkShare Sina response for {stock.ticker} is missing: {sorted(missing)}")
            normalized["turnover_rate"] = pd.to_numeric(normalized["turnover"], errors="raise")
            normalized["data_source"] = "akshare.stock_zh_a_daily"
            normalized["adjustment"] = "qfq_sina_current"
        except RuntimeError:
            raw_chunks = []
            for chunk_start, chunk_end in _year_chunks(start_date, end_date):
                raw_chunks.append(
                    _retry(
                        lambda code=code, chunk_start=chunk_start, chunk_end=chunk_end: ak.stock_zh_a_hist(
                            symbol=code,
                            period="daily",
                            start_date=_yyyymmdd(chunk_start),
                            end_date=_yyyymmdd(chunk_end),
                            adjust="qfq",
                            timeout=20,
                        )
                    )
                )
            raw = pd.concat(raw_chunks, ignore_index=True).drop_duplicates("日期", keep="last")
            normalized = raw.rename(columns=PRICE_COLUMN_MAP)
            missing = set(PRICE_COLUMN_MAP.values()) - set(normalized.columns)
            if missing:
                raise ValueError(f"AkShare Eastmoney response for {stock.ticker} is missing: {sorted(missing)}")
            normalized["turnover_rate"] = pd.to_numeric(normalized["turnover_rate_pct"], errors="raise") / 100
            normalized["data_source"] = "akshare.stock_zh_a_hist"
            normalized["adjustment"] = "qfq_eastmoney_current"

        normalized["ticker"] = stock.ticker
        normalized["stock_name"] = stock.name
        frames.append(normalized)

    prices = pd.concat(frames, ignore_index=True)
    prices["date"] = pd.to_datetime(prices["date"])
    return prices.sort_values(["ticker", "date"]).reset_index(drop=True)


def fetch_csi300(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        import akshare as ak
    except ImportError as exc:  # pragma: no cover - depends on optional environment
        raise RuntimeError('Install research dependencies with: pip install -e ".[research]"') from exc

    try:
        raw = _retry(lambda: ak.stock_zh_index_daily(symbol="sh000300"))
        normalized = raw.copy()
        normalized["date"] = pd.to_datetime(normalized["date"])
        normalized = normalized.loc[
            (normalized["date"] >= pd.Timestamp(start_date)) & (normalized["date"] <= pd.Timestamp(end_date))
        ].copy()
        if normalized.empty:
            raise ValueError("Sina CSI 300 response does not cover the requested dates")
        data_source = "akshare.stock_zh_index_daily"
    except RuntimeError:
        raw_chunks = []
        for chunk_start, chunk_end in _year_chunks(start_date, end_date):
            raw_chunks.append(
                _retry(
                    lambda chunk_start=chunk_start, chunk_end=chunk_end: ak.index_zh_a_hist(
                        symbol="000300",
                        period="daily",
                        start_date=_yyyymmdd(chunk_start),
                        end_date=_yyyymmdd(chunk_end),
                    )
                )
            )
        raw = pd.concat(raw_chunks, ignore_index=True).drop_duplicates("日期", keep="last")
        normalized = raw.rename(columns=INDEX_COLUMN_MAP)
        data_source = "akshare.index_zh_a_hist"
    missing = {"date", "close"} - set(normalized.columns)
    if missing:
        raise ValueError(f"AkShare CSI 300 response is missing: {sorted(missing)}")
    normalized["date"] = pd.to_datetime(normalized["date"])
    normalized["ticker"] = "000300.SH"
    normalized["data_source"] = data_source
    return normalized.sort_values("date").reset_index(drop=True)
