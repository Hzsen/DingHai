from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from quant_agent.data_sources.base import DataBatch, DataRequest, SchemaValidationError, SourceRecord


SHANGHAI = ZoneInfo("Asia/Shanghai")


class PilotParquetMarketSource:
    """Adapter over the real AkShare-backed Phase 0 cache.

    The cache is an auditable local source for the Phase 1 vertical slice; later it
    can be replaced by a live adapter without changing the warehouse contract.
    """

    name = "akshare.phase0_parquet_cache"

    def __init__(self, prices_path: Path, benchmark_path: Path) -> None:
        self.prices_path = prices_path
        self.benchmark_path = benchmark_path

    def fetch(self, request: DataRequest) -> DataBatch:
        prices = pd.read_parquet(self.prices_path)
        benchmark = pd.read_parquet(self.benchmark_path).copy()
        benchmark["ticker"] = "000300.SH"
        benchmark["stock_name"] = "沪深300"
        benchmark["turnover_rate"] = 0.0
        combined = pd.concat([prices, benchmark], ignore_index=True, sort=False)
        required = {"date", "ticker", "stock_name", "open", "high", "low", "close", "volume", "amount"}
        missing = required - set(combined.columns)
        if missing:
            raise SchemaValidationError(f"pilot market cache missing columns: {sorted(missing)}")
        combined["date"] = pd.to_datetime(combined["date"])
        mask = (
            combined["ticker"].isin(request.symbols)
            & (combined["date"].dt.date >= request.start_date)
            & (combined["date"].dt.date <= request.end_date)
        )
        records: list[SourceRecord] = []
        for row in combined.loc[mask].sort_values(["ticker", "date"]).itertuples(index=False):
            event_time = datetime.combine(row.date.date(), datetime.min.time(), tzinfo=SHANGHAI)
            payload = {
                "ticker": row.ticker,
                "name": row.stock_name,
                "trade_date": row.date.date().isoformat(),
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "volume": float(row.volume) if pd.notna(row.volume) else 0.0,
                "amount": float(row.amount) if pd.notna(row.amount) else 0.0,
                "turnover_rate": float(getattr(row, "turnover_rate", 0.0) or 0.0),
                "adjustment": str(getattr(row, "adjustment", "index_unadjusted") or "index_unadjusted"),
            }
            records.append(SourceRecord(row.ticker, event_time, event_time, payload))
        return DataBatch.create(dataset=request.dataset, source=self.name, records=records)
