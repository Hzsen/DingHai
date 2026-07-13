from __future__ import annotations
from typing import Any, TypedDict
class AgentState(TypedDict, total=False):
    query: str; query_type: str; route: str; sql_results: list[dict[str, Any]]; retrieved_docs: list[dict[str, Any]]; calculations: dict[str, Any]; evidence: list[dict[str, Any]]; answer: str; errors: list[str]; confidence: str
