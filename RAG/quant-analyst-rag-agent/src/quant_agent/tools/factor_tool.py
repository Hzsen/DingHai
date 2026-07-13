from __future__ import annotations
from pathlib import Path
from typing import Any
from quant_agent.database.sql_queries import QueryService
class FactorTool:
    def __init__(self, db_path: Path | None = None): self.query_service = QueryService(db_path)
    def definition(self, factor_name: str) -> dict[str, Any]: return self.query_service.get_factor_definition(factor_name)
