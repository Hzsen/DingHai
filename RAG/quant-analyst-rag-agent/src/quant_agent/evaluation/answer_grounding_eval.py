from __future__ import annotations
import re
from typing import Any
from quant_agent.config import Paths
from quant_agent.graph.workflow import ask
TEST_QUERIES = ["Which factor performed best during high-volatility regimes?", "What caused the momentum strategy to underperform in March 2020?", "Explain how the liquidity anomaly factor is calculated."]
def has_required_evidence(result: dict[str, Any]) -> bool:
    route = result.get("route")
    if route in {"sql_only", "factor_definition"}: return bool(result.get("sql_results"))
    if route == "retrieval_only": return bool(result.get("retrieved_docs"))
    if route == "hybrid_sql_retrieval": return bool(result.get("retrieved_docs")) and bool(result.get("sql_results"))
    return "I do not have enough" in result.get("answer", "")
def numeric_claims_are_grounded(result: dict[str, Any]) -> bool:
    answer = result.get("answer", ""); metric_like = re.findall(r"(?:Sharpe|max drawdown|turnover|annual return|return)[: ]+(-?\d+(?:\.\d+)?)%?", answer, flags=re.I)
    if not metric_like: return True
    numeric_values = []
    for row in result.get("sql_results", []):
        for value in row.values():
            if isinstance(value, (float, int)): numeric_values.extend([float(value), round(float(value) * 100, 1)])
    return all(any(abs(float(claim) - candidate) < 0.06 for candidate in numeric_values) for claim in metric_like)
def run_grounding_eval(paths: Paths | None = None) -> dict[str, Any]:
    paths = paths or Paths(); failures = []
    for query in TEST_QUERIES:
        result = ask(query, paths)
        if not has_required_evidence(result) or not numeric_claims_are_grounded(result): failures.append({"query": query, "answer": result.get("answer"), "route": result.get("route")})
    return {"passed": len(failures) == 0, "failed_cases": failures, "total": len(TEST_QUERIES)}
