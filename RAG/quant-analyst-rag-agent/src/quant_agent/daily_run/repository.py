"""Persistence for daily-run snapshots (SQLite, stdlib only)."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from domain.daily_run import DailyRunSnapshot
from quant_agent.daily_run.serialization import jsonable

_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_run_snapshots (
    run_id TEXT PRIMARY KEY,
    as_of TEXT NOT NULL,
    mode TEXT NOT NULL,
    overall_status TEXT NOT NULL,
    synthetic INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""


class DailyRunRepository:
    """Stores rendered snapshots so past runs remain auditable."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def save(self, snapshot: DailyRunSnapshot) -> None:
        payload = json.dumps(jsonable(snapshot), ensure_ascii=False, allow_nan=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_run_snapshots
                    (run_id, as_of, mode, overall_status, synthetic, snapshot_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    as_of=excluded.as_of,
                    mode=excluded.mode,
                    overall_status=excluded.overall_status,
                    synthetic=excluded.synthetic,
                    snapshot_json=excluded.snapshot_json,
                    created_at=excluded.created_at
                """,
                (
                    snapshot.run_id,
                    snapshot.as_of.isoformat(),
                    snapshot.mode.value,
                    snapshot.overall_status.value,
                    int(snapshot.synthetic),
                    payload,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def load_latest(self) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT snapshot_json FROM daily_run_snapshots ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        return json.loads(row[0]) if row else None

    def count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM daily_run_snapshots").fetchone()[0])
