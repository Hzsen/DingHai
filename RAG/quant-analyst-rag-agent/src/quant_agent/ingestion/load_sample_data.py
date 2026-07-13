from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

TABLE_FILES = {
    "prices": "prices_sample.csv",
    "factors": "factors_sample.csv",
    "backtest_results": "backtests_sample.csv",
    "regime_performance": "regimes_sample.csv",
    "anomaly_logs": "anomaly_logs_sample.csv",
    "factor_definitions": "factor_definitions_sample.csv",
}


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_table_rows(raw_dir: Path, table_name: str) -> list[dict[str, str]]:
    if table_name not in TABLE_FILES:
        raise ValueError(f"Unknown sample table: {table_name}")
    path = raw_dir / TABLE_FILES[table_name]
    if not path.exists():
        raise FileNotFoundError(f"Missing sample data file: {path}")
    return load_csv_rows(path)


def iter_table_rows(raw_dir: Path) -> Iterable[tuple[str, list[dict[str, str]]]]:
    for table_name in TABLE_FILES:
        yield table_name, load_table_rows(raw_dir, table_name)
