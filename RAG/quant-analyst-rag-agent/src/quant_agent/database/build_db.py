from __future__ import annotations

import sqlite3
from pathlib import Path

from quant_agent.config import Paths
from quant_agent.database.schema import SCHEMA_STATEMENTS, TABLE_INSERT_COLUMNS
from quant_agent.ingestion.load_sample_data import iter_table_rows
from quant_agent.ingestion.validate_data import validate_rows


def create_schema(conn: sqlite3.Connection) -> None:
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)


def insert_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict[str, str]]) -> None:
    columns = TABLE_INSERT_COLUMNS[table_name]
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    conn.executemany(sql, [tuple(row[column] for column in columns) for row in rows])


def build_database(db_path: Path | None = None, raw_dir: Path | None = None, overwrite: bool = True) -> Path:
    paths = Paths()
    target = db_path or paths.db_path
    source_dir = raw_dir or paths.raw_data_dir
    target.parent.mkdir(parents=True, exist_ok=True)
    if overwrite and target.exists():
        target.unlink()
    with sqlite3.connect(target) as conn:
        create_schema(conn)
        for table_name, rows in iter_table_rows(source_dir):
            validate_rows(table_name, rows)
            insert_rows(conn, table_name, rows)
        conn.commit()
    return target


if __name__ == "__main__":
    print(build_database())
