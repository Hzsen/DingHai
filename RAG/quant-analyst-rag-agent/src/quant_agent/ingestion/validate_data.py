from __future__ import annotations

from datetime import datetime

REQUIRED_COLUMNS = {
    "prices": {"ticker", "date", "open", "high", "low", "close", "adjusted_close", "volume", "sector"},
    "factors": {"ticker", "date", "factor_name", "factor_value", "factor_rank", "sector"},
    "backtest_results": {"strategy_name", "factor_name", "start_date", "end_date", "annual_return", "sharpe", "max_drawdown", "volatility", "turnover", "transaction_cost_bps", "benchmark_return"},
    "regime_performance": {"strategy_name", "factor_name", "regime", "annual_return", "sharpe", "max_drawdown", "hit_rate"},
    "anomaly_logs": {"date", "ticker", "anomaly_type", "description", "severity"},
    "factor_definitions": {"factor_name", "definition", "formula", "interpretation", "common_failure_modes"},
}
DATE_FIELDS = {"prices": ["date"], "factors": ["date"], "backtest_results": ["start_date", "end_date"], "regime_performance": [], "anomaly_logs": ["date"], "factor_definitions": []}


def validate_date(value: str) -> None:
    datetime.strptime(value, "%Y-%m-%d")


def validate_rows(table_name: str, rows: list[dict[str, str]]) -> None:
    if table_name not in REQUIRED_COLUMNS:
        raise ValueError(f"Unknown table for validation: {table_name}")
    if not rows:
        raise ValueError(f"No rows found for table: {table_name}")
    missing = REQUIRED_COLUMNS[table_name] - set(rows[0])
    if missing:
        raise ValueError(f"{table_name} is missing columns: {sorted(missing)}")
    for row in rows:
        for field in DATE_FIELDS[table_name]:
            validate_date(row[field])
