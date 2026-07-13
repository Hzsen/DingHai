from __future__ import annotations

from pathlib import Path
from typing import Any

from quant_agent.database.sql_queries import QueryService


class SQLTool:
    def __init__(self, db_path: Path | None = None):
        self.query_service = QueryService(db_path)

    def best_factor_by_regime(self, regime: str, metric: str = "sharpe") -> list[dict[str, Any]]:
        return self.query_service.get_best_factor_by_regime(regime, metric)

    def compare_factors(
        self,
        factor_names: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.query_service.compare_factors(factor_names, start_date, end_date)

    def strategy_metrics(
        self,
        strategy_name: str | None = None,
        factor_name: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.query_service.get_strategy_metrics(strategy_name, factor_name)

    def compare_factor_across_regimes(
        self,
        factor_name: str,
        regimes: list[str],
    ) -> list[dict[str, Any]]:
        return self.query_service.compare_factor_across_regimes(factor_name, regimes)

    def anomalies(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        ticker: str | None = None,
        anomaly_type: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.query_service.get_anomalies(start_date, end_date, ticker, anomaly_type)

    def best_strategy(self, metric: str = "sharpe") -> list[dict[str, Any]]:
        return self.query_service.get_best_strategy(metric)
