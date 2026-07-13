from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from quant_agent.config import Paths

FACTOR_ALIASES = {"momentum": "momentum_60d", "60-day momentum": "momentum_60d", "60 day momentum": "momentum_60d", "momentum_60d": "momentum_60d", "volatility": "volatility_20d", "20-day volatility": "volatility_20d", "20 day volatility": "volatility_20d", "volatility_20d": "volatility_20d", "liquidity": "liquidity_dollar_volume", "liquidity anomaly": "liquidity_dollar_volume", "liquidity_dollar_volume": "liquidity_dollar_volume", "sector rotation": "sector_relative_strength", "sector relative strength": "sector_relative_strength", "sector_relative_strength": "sector_relative_strength"}
REGIME_ALIASES = {"high volatility": "high_volatility", "high-volatility": "high_volatility", "high_volatility": "high_volatility", "low volatility": "low_volatility", "low-volatility": "low_volatility", "low_volatility": "low_volatility", "drawdown": "drawdown", "recovery": "recovery", "bull trend": "bull_trend", "bull-trend": "bull_trend", "bull_trend": "bull_trend"}
STRATEGY_ALIASES = {"momentum": "momentum_long_short", "momentum strategy": "momentum_long_short", "momentum_long_short": "momentum_long_short", "volatility": "low_volatility_strategy", "low volatility": "low_volatility_strategy", "low_volatility_strategy": "low_volatility_strategy", "liquidity": "liquidity_quality_strategy", "liquidity_quality_strategy": "liquidity_quality_strategy", "sector rotation": "sector_rotation_strategy", "sector-rotation": "sector_rotation_strategy", "sector_rotation_strategy": "sector_rotation_strategy"}
ALLOWED_METRICS = {"annual_return", "sharpe", "max_drawdown", "volatility", "turnover", "transaction_cost_bps", "benchmark_return", "hit_rate"}


def normalize_date(value: str | None) -> str | None:
    if not value:
        return None
    datetime.strptime(value, "%Y-%m-%d")
    return value


def normalize_factor_name(value: str | None) -> str | None:
    if not value:
        return None
    key = value.strip().lower().replace("_", " ")
    return FACTOR_ALIASES.get(key) or FACTOR_ALIASES.get(value.strip().lower())


def normalize_regime(value: str | None) -> str | None:
    if not value:
        return None
    key = value.strip().lower().replace("_", " ")
    return REGIME_ALIASES.get(key) or REGIME_ALIASES.get(value.strip().lower())


def normalize_strategy_name(value: str | None) -> str | None:
    if not value:
        return None
    key = value.strip().lower().replace("_", " ")
    return STRATEGY_ALIASES.get(key) or STRATEGY_ALIASES.get(value.strip().lower())


def extract_year_range(query: str) -> tuple[str | None, str | None]:
    years = [int(match) for match in re.findall(r"\b(20\d{2})\b", query)]
    if not years:
        return None, None
    if len(years) == 1:
        return f"{years[0]}-01-01", f"{years[0]}-12-31"
    return f"{min(years)}-01-01", f"{max(years)}-12-31"


class QueryService:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or Paths().db_path
        if not self.db_path.exists():
            raise FileNotFoundError(f"SQLite database not found: {self.db_path}. Run build_indexes --build-db first.")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    def _validate_metric(self, metric: str) -> str:
        if metric not in ALLOWED_METRICS:
            raise ValueError(f"Unsupported metric: {metric}")
        return metric

    def get_best_factor_by_regime(self, regime: str, metric: str = "sharpe") -> list[dict[str, Any]]:
        normalized_regime = normalize_regime(regime)
        if not normalized_regime:
            raise ValueError(f"Unsupported regime: {regime}")
        metric = self._validate_metric(metric)
        return self._rows(f"""SELECT strategy_name, factor_name, regime, annual_return, sharpe, max_drawdown, hit_rate, 'regime_performance' AS source_table FROM regime_performance WHERE regime = ? ORDER BY {metric} DESC""", (normalized_regime,))

    def compare_factors(self, factor_names: list[str], start_date: str | None = None, end_date: str | None = None) -> list[dict[str, Any]]:
        factors = [name for name in (normalize_factor_name(name) for name in factor_names) if name]
        if not factors:
            raise ValueError("No supported factor names were provided")
        start_date = normalize_date(start_date)
        end_date = normalize_date(end_date)
        placeholders = ", ".join("?" for _ in factors)
        params: list[Any] = list(factors)
        filters = [f"factor_name IN ({placeholders})"]
        if start_date:
            filters.append("end_date >= ?")
            params.append(start_date)
        if end_date:
            filters.append("start_date <= ?")
            params.append(end_date)
        return self._rows(f"""SELECT strategy_name, factor_name, start_date, end_date, annual_return, sharpe, max_drawdown, volatility, turnover, transaction_cost_bps, benchmark_return, 'backtest_results' AS source_table FROM backtest_results WHERE {' AND '.join(filters)} ORDER BY sharpe DESC""", tuple(params))

    def get_strategy_metrics(self, strategy_name: str | None = None, factor_name: str | None = None) -> list[dict[str, Any]]:
        strategy = normalize_strategy_name(strategy_name) if strategy_name else None
        factor = normalize_factor_name(factor_name) if factor_name else None
        filters: list[str] = []
        params: list[Any] = []
        if strategy:
            filters.append("strategy_name = ?")
            params.append(strategy)
        if factor:
            filters.append("factor_name = ?")
            params.append(factor)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        return self._rows(f"""SELECT strategy_name, factor_name, start_date, end_date, annual_return, sharpe, max_drawdown, volatility, turnover, transaction_cost_bps, benchmark_return, 'backtest_results' AS source_table FROM backtest_results {where} ORDER BY end_date DESC, sharpe DESC""", tuple(params))

    def get_factor_definition(self, factor_name: str) -> dict[str, Any]:
        factor = normalize_factor_name(factor_name)
        if not factor:
            raise ValueError(f"Unsupported factor name: {factor_name}")
        rows = self._rows("""SELECT factor_name, definition, formula, interpretation, common_failure_modes, 'factor_definitions' AS source_table FROM factor_definitions WHERE factor_name = ?""", (factor,))
        return rows[0] if rows else {}

    def compare_factor_across_regimes(self, factor_name: str, regimes: list[str]) -> list[dict[str, Any]]:
        factor = normalize_factor_name(factor_name)
        final_regimes = [regime for regime in (normalize_regime(regime) for regime in regimes) if regime]
        if not factor or not final_regimes:
            raise ValueError("Supported factor and regime values are required")
        placeholders = ", ".join("?" for _ in final_regimes)
        return self._rows(f"""SELECT strategy_name, factor_name, regime, annual_return, sharpe, max_drawdown, hit_rate, 'regime_performance' AS source_table FROM regime_performance WHERE factor_name = ? AND regime IN ({placeholders}) ORDER BY sharpe DESC""", tuple([factor] + final_regimes))

    def get_anomalies(self, start_date: str | None = None, end_date: str | None = None, ticker: str | None = None, anomaly_type: str | None = None) -> list[dict[str, Any]]:
        filters: list[str] = []
        params: list[Any] = []
        if start_date:
            filters.append("date >= ?")
            params.append(normalize_date(start_date))
        if end_date:
            filters.append("date <= ?")
            params.append(normalize_date(end_date))
        if ticker:
            filters.append("ticker = ?")
            params.append(ticker.upper())
        if anomaly_type:
            filters.append("anomaly_type = ?")
            params.append(anomaly_type)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        return self._rows(f"""SELECT date, ticker, anomaly_type, description, severity, 'anomaly_logs' AS source_table FROM anomaly_logs {where} ORDER BY date ASC""", tuple(params))

    def get_best_strategy(self, metric: str = "sharpe") -> list[dict[str, Any]]:
        metric = self._validate_metric(metric)
        return self._rows(f"""SELECT strategy_name, factor_name, start_date, end_date, annual_return, sharpe, max_drawdown, volatility, turnover, transaction_cost_bps, benchmark_return, 'backtest_results' AS source_table FROM backtest_results ORDER BY {metric} DESC LIMIT 5""")
