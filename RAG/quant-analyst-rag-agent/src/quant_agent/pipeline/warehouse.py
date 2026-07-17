from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quant_agent.data_sources.base import DataBatch, DataQualityError, SourceRecord


WAREHOUSE_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS ingestion_runs (
        run_id TEXT PRIMARY KEY, dataset TEXT NOT NULL, source TEXT NOT NULL,
        status TEXT NOT NULL, requested_at TEXT NOT NULL, fetched_at TEXT NOT NULL,
        completed_at TEXT, record_count INTEGER NOT NULL, error_count INTEGER NOT NULL,
        error_json TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS bronze_records (
        record_hash TEXT PRIMARY KEY, run_id TEXT NOT NULL, dataset TEXT NOT NULL,
        source TEXT NOT NULL, symbol TEXT NOT NULL, event_time TEXT NOT NULL,
        available_at TEXT NOT NULL, payload_json TEXT NOT NULL, fetched_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS quality_checks (
        run_id TEXT NOT NULL, check_name TEXT NOT NULL, status TEXT NOT NULL,
        severity TEXT NOT NULL, details TEXT NOT NULL, checked_at TEXT NOT NULL,
        PRIMARY KEY (run_id, check_name)
    )""",
    """CREATE TABLE IF NOT EXISTS silver_cn_daily (
        ticker TEXT NOT NULL, trade_date TEXT NOT NULL, name TEXT NOT NULL,
        open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
        volume REAL NOT NULL, amount REAL NOT NULL, turnover_rate REAL NOT NULL,
        adjustment TEXT NOT NULL, available_at TEXT NOT NULL, source TEXT NOT NULL,
        source_run_id TEXT NOT NULL, content_hash TEXT NOT NULL,
        PRIMARY KEY (ticker, trade_date, adjustment)
    )""",
    """CREATE TABLE IF NOT EXISTS silver_macro_observations (
        series_id TEXT NOT NULL, observation_date TEXT NOT NULL, value REAL NOT NULL,
        unit TEXT NOT NULL, frequency TEXT NOT NULL, available_at TEXT NOT NULL,
        source TEXT NOT NULL, source_run_id TEXT NOT NULL, content_hash TEXT NOT NULL,
        PRIMARY KEY (series_id, observation_date)
    )""",
    """CREATE TABLE IF NOT EXISTS gold_cn_prices (
        ticker TEXT NOT NULL, trade_date TEXT NOT NULL, name TEXT NOT NULL,
        open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
        volume REAL NOT NULL, amount REAL NOT NULL, turnover_rate REAL NOT NULL,
        adjustment TEXT NOT NULL, available_at TEXT NOT NULL, source_run_id TEXT NOT NULL,
        PRIMARY KEY (ticker, trade_date)
    )""",
    """CREATE TABLE IF NOT EXISTS gold_macro_observations (
        series_id TEXT NOT NULL, observation_date TEXT NOT NULL,
        value_millions_usd REAL NOT NULL, available_at TEXT NOT NULL,
        source_run_id TEXT NOT NULL, PRIMARY KEY (series_id, observation_date)
    )""",
    """CREATE TABLE IF NOT EXISTS dataset_versions (
        dataset TEXT PRIMARY KEY, run_id TEXT NOT NULL, published_at TEXT NOT NULL,
        row_count INTEGER NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_bronze_run ON bronze_records(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_cn_date ON gold_cn_prices(trade_date, ticker)",
    "CREATE INDEX IF NOT EXISTS idx_macro_available ON gold_macro_observations(available_at)",
]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _content_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _record_hash(batch: DataBatch, record: SourceRecord) -> str:
    return _content_hash(
        {
            "dataset": batch.dataset,
            "source": batch.source,
            "symbol": record.symbol,
            "event_time": record.event_time.isoformat(),
            "available_at": record.available_at.isoformat(),
            "payload": dict(record.payload),
        }
    )


def _market_errors(records: tuple[SourceRecord, ...]) -> list[str]:
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    required = {"ticker", "name", "trade_date", "open", "high", "low", "close", "volume", "amount"}
    for record in records:
        payload = record.payload
        missing = required - set(payload)
        if missing:
            errors.append(f"{record.symbol}: missing {sorted(missing)}")
            continue
        key = (str(payload["ticker"]), str(payload["trade_date"]))
        if key in seen:
            errors.append(f"duplicate market key: {key}")
        seen.add(key)
        try:
            open_, high, low, close = (float(payload[name]) for name in ("open", "high", "low", "close"))
            volume, amount = float(payload["volume"]), float(payload["amount"])
        except (TypeError, ValueError):
            errors.append(f"{record.symbol}: non-numeric OHLCV")
            continue
        if high < max(open_, low, close) or low > min(open_, high, close):
            errors.append(f"{record.symbol} {payload['trade_date']}: invalid OHLC")
        if not all(math.isfinite(value) for value in (open_, high, low, close, volume, amount)):
            errors.append(f"{record.symbol} {payload['trade_date']}: non-finite market value")
        elif min(open_, high, low, close, volume, amount) < 0:
            errors.append(f"{record.symbol} {payload['trade_date']}: negative market value")
    return errors


def _macro_errors(records: tuple[SourceRecord, ...]) -> list[str]:
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    required = {"series_id", "observation_date", "value", "unit", "frequency"}
    for record in records:
        payload = record.payload
        missing = required - set(payload)
        if missing:
            errors.append(f"{record.symbol}: missing {sorted(missing)}")
            continue
        key = (str(payload["series_id"]), str(payload["observation_date"]))
        if key in seen:
            errors.append(f"duplicate macro key: {key}")
        seen.add(key)
        try:
            float(payload["value"])
        except (TypeError, ValueError):
            errors.append(f"{record.symbol}: non-numeric value")
    return errors


class PhaseWarehouse:
    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            for statement in WAREHOUSE_SCHEMA:
                conn.execute(statement)
            abandoned = conn.execute("SELECT run_id FROM ingestion_runs WHERE status='staged'").fetchall()
            now = datetime.now(timezone.utc).isoformat()
            for row in abandoned:
                conn.execute(
                    "INSERT OR REPLACE INTO quality_checks VALUES (?,?,?,?,?,?)",
                    (row[0], "atomic_publish", "failed", "error", '["abandoned staged run recovered"]', now),
                )
            conn.execute(
                "UPDATE ingestion_runs SET status='failed', completed_at=? WHERE status='staged'",
                (now,),
            )

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _record_run_and_bronze(self, batch: DataBatch) -> None:
        errors = [asdict(error) for error in batch.errors]
        with self.connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO ingestion_runs
                (run_id,dataset,source,status,requested_at,fetched_at,completed_at,record_count,error_count,error_json)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    batch.batch_id,
                    batch.dataset,
                    batch.source,
                    "staged",
                    batch.requested_at.isoformat(),
                    batch.fetched_at.isoformat(),
                    None,
                    len(batch.records),
                    len(batch.errors),
                    _canonical_json(errors),
                ),
            )
            for record in batch.records:
                conn.execute(
                    """INSERT OR IGNORE INTO bronze_records
                    (record_hash,run_id,dataset,source,symbol,event_time,available_at,payload_json,fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        _record_hash(batch, record),
                        batch.batch_id,
                        batch.dataset,
                        batch.source,
                        record.symbol,
                        record.event_time.isoformat(),
                        record.available_at.isoformat(),
                        _canonical_json(dict(record.payload)),
                        batch.fetched_at.isoformat(),
                    ),
                )

    def ingest_batch(self, batch: DataBatch) -> None:
        """Stage raw data, validate, and atomically publish one dataset."""
        self._record_run_and_bronze(batch)
        if batch.errors:
            self._mark_failed(batch, "source_errors", [error.message for error in batch.errors])
            raise DataQualityError("batch contains source errors")
        if not batch.records:
            self._mark_failed(batch, "non_empty", ["batch has no records"])
            raise DataQualityError("batch has no records")
        if batch.dataset == "cn_daily":
            errors = _market_errors(batch.records)
        elif batch.dataset == "us_liquidity":
            errors = _macro_errors(batch.records)
        else:
            errors = [f"unsupported dataset: {batch.dataset}"]
        if errors:
            self._mark_failed(batch, "dataset_quality", errors)
            raise DataQualityError("; ".join(errors[:3]))
        try:
            self._publish(batch)
        except Exception as exc:
            self._mark_failed(batch, "atomic_publish", [type(exc).__name__])
            raise

    def _mark_failed(self, batch: DataBatch, check_name: str, errors: list[str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO quality_checks VALUES (?,?,?,?,?,?)",
                (batch.batch_id, check_name, "failed", "error", _canonical_json(errors), now),
            )
            conn.execute(
                "UPDATE ingestion_runs SET status='failed', completed_at=? WHERE run_id=?",
                (now, batch.batch_id),
            )

    def _publish(self, batch: DataBatch) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if batch.dataset == "cn_daily":
                for record in batch.records:
                    p = record.payload
                    values = (
                        p["ticker"], p["trade_date"], p["name"], p["open"], p["high"], p["low"], p["close"],
                        p["volume"], p["amount"], p.get("turnover_rate", 0.0), p.get("adjustment", "unknown"),
                        record.available_at.isoformat(), batch.source, batch.batch_id, _content_hash(dict(p)),
                    )
                    conn.execute(
                        """INSERT INTO silver_cn_daily VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(ticker,trade_date,adjustment) DO UPDATE SET
                        name=excluded.name,open=excluded.open,high=excluded.high,low=excluded.low,
                        close=excluded.close,volume=excluded.volume,amount=excluded.amount,
                        turnover_rate=excluded.turnover_rate,available_at=excluded.available_at,
                        source=excluded.source,source_run_id=excluded.source_run_id,content_hash=excluded.content_hash""",
                        values,
                    )
                    conn.execute(
                        """INSERT INTO gold_cn_prices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(ticker,trade_date) DO UPDATE SET
                        name=excluded.name,open=excluded.open,high=excluded.high,low=excluded.low,
                        close=excluded.close,volume=excluded.volume,amount=excluded.amount,
                        turnover_rate=excluded.turnover_rate,adjustment=excluded.adjustment,
                        available_at=excluded.available_at,source_run_id=excluded.source_run_id""",
                        values[:11] + (values[11], values[13]),
                    )
            else:
                for record in batch.records:
                    p = record.payload
                    content_hash = _content_hash(dict(p))
                    silver_values = (
                        p["series_id"], p["observation_date"], p["value"], p["unit"], p["frequency"],
                        record.available_at.isoformat(), batch.source, batch.batch_id, content_hash,
                    )
                    conn.execute(
                        """INSERT INTO silver_macro_observations VALUES (?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(series_id,observation_date) DO UPDATE SET
                        value=excluded.value,unit=excluded.unit,frequency=excluded.frequency,
                        available_at=excluded.available_at,source=excluded.source,
                        source_run_id=excluded.source_run_id,content_hash=excluded.content_hash""",
                        silver_values,
                    )
                    normalized = float(p["value"]) * (1000.0 if p["unit"] == "billions_usd" else 1.0)
                    conn.execute(
                        """INSERT INTO gold_macro_observations VALUES (?,?,?,?,?)
                        ON CONFLICT(series_id,observation_date) DO UPDATE SET
                        value_millions_usd=excluded.value_millions_usd,
                        available_at=excluded.available_at,source_run_id=excluded.source_run_id""",
                        (p["series_id"], p["observation_date"], normalized, record.available_at.isoformat(), batch.batch_id),
                    )
            row_count = conn.execute(
                "SELECT COUNT(*) FROM gold_cn_prices" if batch.dataset == "cn_daily" else "SELECT COUNT(*) FROM gold_macro_observations"
            ).fetchone()[0]
            conn.execute(
                "INSERT OR REPLACE INTO quality_checks VALUES (?,?,?,?,?,?)",
                (batch.batch_id, "dataset_quality", "passed", "error", "[]", now),
            )
            conn.execute(
                "UPDATE ingestion_runs SET status='published', completed_at=? WHERE run_id=?",
                (now, batch.batch_id),
            )
            conn.execute(
                """INSERT INTO dataset_versions VALUES (?,?,?,?)
                ON CONFLICT(dataset) DO UPDATE SET run_id=excluded.run_id,
                published_at=excluded.published_at,row_count=excluded.row_count""",
                (batch.dataset, batch.batch_id, now, row_count),
            )

    def table_count(self, table: str) -> int:
        allowed = {
            "ingestion_runs", "bronze_records", "quality_checks", "silver_cn_daily",
            "silver_macro_observations", "gold_cn_prices", "gold_macro_observations",
        }
        if table not in allowed:
            raise ValueError("unsupported table")
        with self.connect() as conn:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def watermark(self, dataset: str) -> str | None:
        table_and_column = {
            "cn_daily": ("gold_cn_prices", "trade_date"),
            "us_liquidity": ("gold_macro_observations", "observation_date"),
        }
        if dataset not in table_and_column:
            raise ValueError("unsupported dataset")
        table, column = table_and_column[dataset]
        with self.connect() as conn:
            return conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()[0]
