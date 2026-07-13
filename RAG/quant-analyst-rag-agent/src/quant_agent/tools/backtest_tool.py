from __future__ import annotations
from pathlib import Path
from typing import Any
from quant_agent.database.sql_queries import QueryService
class BacktestTool:
    def __init__(self, db_path: Path | None = None): self.query_service = QueryService(db_path)
    def compare_factors(self, factor_names: list[str], start_date: str | None = None, end_date: str | None = None) -> list[dict[str, Any]]: return self.query_service.compare_factors(factor_names, start_date, end_date)
    def strategy_metrics(self, strategy_name: str | None = None, factor_name: str | None = None) -> list[dict[str, Any]]: return self.query_service.get_strategy_metrics(strategy_name, factor_name)
