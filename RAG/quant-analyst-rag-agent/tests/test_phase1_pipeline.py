from __future__ import annotations

from datetime import datetime, timezone
import math

import pytest

from quant_agent.data_sources.base import DataBatch, DataQualityError, SourceRecord, TransientSourceError, with_retry
from quant_agent.pipeline.warehouse import PhaseWarehouse


def _record(close: float = 10.5) -> SourceRecord:
    now = datetime(2026, 1, 5, tzinfo=timezone.utc)
    return SourceRecord(
        "000001.SZ",
        now,
        now,
        {
            "ticker": "000001.SZ",
            "name": "测试股票",
            "trade_date": "2026-01-05",
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": close,
            "volume": 1000,
            "amount": 10000,
            "turnover_rate": 0.02,
            "adjustment": "qfq",
        },
    )


def test_retry_only_retries_transient_errors() -> None:
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TransientSourceError("temporary")
        return "ok"

    assert with_retry(operation, sleep=lambda _: None) == "ok"
    assert calls == 3


def test_repeated_ingest_is_idempotent(tmp_path) -> None:
    warehouse = PhaseWarehouse(tmp_path / "warehouse.db")
    warehouse.ingest_batch(DataBatch.create(dataset="cn_daily", source="fixture", records=[_record()]))
    warehouse.ingest_batch(DataBatch.create(dataset="cn_daily", source="fixture", records=[_record()]))

    assert warehouse.table_count("bronze_records") == 1
    assert warehouse.table_count("silver_cn_daily") == 1
    assert warehouse.table_count("gold_cn_prices") == 1
    assert warehouse.watermark("cn_daily") == "2026-01-05"


def test_bad_batch_does_not_replace_last_good_gold(tmp_path) -> None:
    warehouse = PhaseWarehouse(tmp_path / "warehouse.db")
    warehouse.ingest_batch(DataBatch.create(dataset="cn_daily", source="fixture", records=[_record(10.5)]))
    bad = DataBatch.create(dataset="cn_daily", source="fixture", records=[_record(-1.0)])

    with pytest.raises(DataQualityError):
        warehouse.ingest_batch(bad)

    with warehouse.connect() as conn:
        close = conn.execute("SELECT close FROM gold_cn_prices WHERE ticker='000001.SZ'").fetchone()[0]
        status = conn.execute("SELECT status FROM ingestion_runs WHERE run_id=?", (bad.batch_id,)).fetchone()[0]
    assert close == 10.5
    assert status == "failed"


def test_non_finite_value_is_rejected_before_publish(tmp_path) -> None:
    warehouse = PhaseWarehouse(tmp_path / "warehouse.db")
    with pytest.raises(DataQualityError, match="non-finite"):
        warehouse.ingest_batch(DataBatch.create(dataset="cn_daily", source="fixture", records=[_record(math.nan)]))
    assert warehouse.table_count("gold_cn_prices") == 0
