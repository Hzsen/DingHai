from __future__ import annotations
from pathlib import Path
from typing import Any
from quant_agent.database.sql_queries import QueryService
class RegimeTool:
    def __init__(self, db_path: Path | None = None): self.query_service = QueryService(db_path)
    def best_factor(self, regime: str, metric: str = "sharpe") -> list[dict[str, Any]]: return self.query_service.get_best_factor_by_regime(regime, metric)
    def compare_factor(self, factor_name: str, regimes: list[str]) -> list[dict[str, Any]]: return self.query_service.compare_factor_across_regimes(factor_name, regimes)
