from __future__ import annotations

import json
import sqlite3
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from quant_agent.theme_rotation.engine import ThemeRotationEvaluation
from quant_agent.theme_rotation.serialization import jsonable


SCHEMA = (
    """CREATE TABLE IF NOT EXISTS theme_rotation_runs (
        run_id TEXT PRIMARY KEY,as_of TEXT NOT NULL,status TEXT NOT NULL,source TEXT NOT NULL,
        price_rows INTEGER NOT NULL,error_count INTEGER NOT NULL,metadata_json TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS theme_rotation_price_observations (
        symbol TEXT NOT NULL,observation_date TEXT NOT NULL,available_at TEXT NOT NULL,
        close REAL NOT NULL,volume REAL NOT NULL,source TEXT NOT NULL,run_id TEXT NOT NULL,
        PRIMARY KEY(symbol,observation_date,source)
    )""",
    """CREATE TABLE IF NOT EXISTS theme_rotation_snapshots (
        snapshot_id TEXT PRIMARY KEY,as_of TEXT NOT NULL,valid_until TEXT NOT NULL,
        benchmark_symbol TEXT NOT NULL,selected_theme_id TEXT NOT NULL,data_coverage REAL NOT NULL,
        model_version TEXT NOT NULL,reference_model_version TEXT NOT NULL,payload_json TEXT NOT NULL,
        run_id TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS theme_rotation_metrics (
        snapshot_id TEXT NOT NULL,theme_id TEXT NOT NULL,label TEXT NOT NULL,proxy_symbol TEXT NOT NULL,
        relative_5d REAL,relative_20d REAL,relative_60d REAL,absolute_5d REAL,volume_ratio REAL,
        breadth REAL,score REAL,state TEXT NOT NULL,reason TEXT NOT NULL,data_coverage REAL NOT NULL,
        payload_json TEXT NOT NULL,PRIMARY KEY(snapshot_id,theme_id)
    )""",
    """CREATE TABLE IF NOT EXISTS theme_rotation_attributions (
        snapshot_id TEXT NOT NULL,attribution_id TEXT NOT NULL,label TEXT NOT NULL,score REAL NOT NULL,
        core_signal TEXT NOT NULL,invalidation TEXT NOT NULL,payload_json TEXT NOT NULL,
        PRIMARY KEY(snapshot_id,attribution_id)
    )""",
    """CREATE TABLE IF NOT EXISTS theme_rotation_alerts (
        snapshot_id TEXT NOT NULL,alert_id TEXT NOT NULL,label TEXT NOT NULL,direction TEXT NOT NULL,
        threshold REAL NOT NULL,previous_value REAL NOT NULL,current_value REAL NOT NULL,
        PRIMARY KEY(snapshot_id,alert_id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_theme_rotation_snapshot_asof ON theme_rotation_snapshots(as_of)",
)


def publish_theme_rotation(
    db_path: Path | str,
    evaluation: ThemeRotationEvaluation,
    prices: pd.DataFrame,
    *,
    source: str,
    source_errors: list[dict[str, Any]] | None = None,
) -> str:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    source_errors = source_errors or []
    snapshot = evaluation.snapshot
    run_id = f"theme-rotation-run/{snapshot.as_of.date().isoformat()}/{snapshot.snapshot_id.rsplit('/', 1)[-1]}"
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        for statement in SCHEMA:
            conn.execute(statement)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT OR REPLACE INTO theme_rotation_runs VALUES (?,?,?,?,?,?,?,?)",
            (
                run_id, snapshot.as_of.isoformat(), "PUBLISHED_WITH_WARNINGS" if source_errors else "PUBLISHED",
                source, len(prices), len(source_errors),
                json.dumps({"errors": source_errors}, ensure_ascii=False, sort_keys=True), now,
            ),
        )
        normalized = prices.loc[:, ["date", "symbol", "close", "volume"]].copy()
        normalized["date"] = pd.to_datetime(normalized["date"])
        normalized = normalized.dropna(subset=["date", "symbol", "close"]).sort_values(["date", "symbol"])
        for row in normalized.itertuples(index=False):
            observed = row.date.date()
            available_at = datetime.combine(observed, time(21, 0), tzinfo=timezone.utc).isoformat()
            volume = 0.0 if pd.isna(row.volume) else float(row.volume)
            conn.execute(
                """INSERT INTO theme_rotation_price_observations VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(symbol,observation_date,source) DO UPDATE SET
                available_at=excluded.available_at,close=excluded.close,volume=excluded.volume,run_id=excluded.run_id""",
                (str(row.symbol), observed.isoformat(), available_at, float(row.close), volume, source, run_id),
            )
        payload = json.dumps(jsonable(evaluation), ensure_ascii=False, sort_keys=True, allow_nan=False)
        conn.execute(
            "INSERT OR REPLACE INTO theme_rotation_snapshots VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                snapshot.snapshot_id, snapshot.as_of.isoformat(), snapshot.valid_until.isoformat(),
                snapshot.benchmark_symbol, snapshot.selected_theme_id, snapshot.data_coverage,
                snapshot.model_version, snapshot.reference_model_version, payload, run_id,
            ),
        )
        conn.execute("DELETE FROM theme_rotation_metrics WHERE snapshot_id=?", (snapshot.snapshot_id,))
        conn.execute("DELETE FROM theme_rotation_attributions WHERE snapshot_id=?", (snapshot.snapshot_id,))
        conn.execute("DELETE FROM theme_rotation_alerts WHERE snapshot_id=?", (snapshot.snapshot_id,))
        for item in snapshot.themes:
            item_payload = json.dumps(jsonable(item), ensure_ascii=False, sort_keys=True, allow_nan=False)
            conn.execute(
                "INSERT INTO theme_rotation_metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    snapshot.snapshot_id, item.theme_id, item.label, item.proxy_symbol, item.relative_5d,
                    item.relative_20d, item.relative_60d, item.absolute_5d, item.volume_ratio, item.breadth,
                    item.score, item.state.value, item.reason.value, item.data_coverage, item_payload,
                ),
            )
        for item in snapshot.attributions:
            conn.execute(
                "INSERT INTO theme_rotation_attributions VALUES (?,?,?,?,?,?,?)",
                (
                    snapshot.snapshot_id, item.attribution_id, item.label, item.score, item.core_signal,
                    item.invalidation, json.dumps(jsonable(item), ensure_ascii=False, sort_keys=True),
                ),
            )
        for item in snapshot.alerts:
            conn.execute(
                "INSERT INTO theme_rotation_alerts VALUES (?,?,?,?,?,?,?)",
                (
                    snapshot.snapshot_id, item.alert_id, item.label, item.direction.value, item.threshold,
                    item.previous_value, item.current_value,
                ),
            )
    return run_id
