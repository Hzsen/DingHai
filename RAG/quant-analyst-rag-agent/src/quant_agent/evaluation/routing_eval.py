from __future__ import annotations

from collections import defaultdict
from typing import Any

from quant_agent.graph.router import route_query

TEST_CASES = [
    {
        "query": "Which factor performed best during high-volatility regimes?",
        "expected_route": "sql_only",
    },
    {
        "query": "What caused momentum to underperform in March 2020?",
        "expected_route": "hybrid_sql_retrieval",
    },
    {
        "query": "Compare 60-day momentum and volatility factors from 2020 to 2024.",
        "expected_route": "sql_only",
    },
    {
        "query": "Find research notes related to liquidity shocks.",
        "expected_route": "retrieval_only",
    },
    {
        "query": "Explain how the liquidity anomaly factor is calculated.",
        "expected_route": "factor_definition",
    },
    {
        "query": "Show me the max drawdown and turnover of the sector-rotation strategy.",
        "expected_route": "sql_only",
    },
]


def run_routing_eval() -> dict[str, Any]:
    failed = []
    confusion: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for case in TEST_CASES:
        _query_type, route = route_query(case["query"])
        expected = case["expected_route"]
        confusion[expected][route] += 1
        if route != expected:
            failed.append(
                {
                    "query": case["query"],
                    "expected": expected,
                    "actual": route,
                }
            )

    return {
        "accuracy": (len(TEST_CASES) - len(failed)) / len(TEST_CASES),
        "failed_cases": failed,
        "confusion_matrix": {key: dict(value) for key, value in confusion.items()},
    }
