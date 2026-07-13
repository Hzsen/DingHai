from __future__ import annotations

import re

QUERY_TYPE_TO_ROUTE = {
    "structured_metric_query": "sql_only",
    "backtest_comparison_query": "sql_only",
    "regime_analysis_query": "sql_only",
    "factor_definition_query": "factor_definition",
    "research_note_query": "retrieval_only",
    "hybrid_explanation_query": "hybrid_sql_retrieval",
    "calculation_query": "calculator",
    "ambiguous_query": "clarification_or_safe_response",
}


def classify_query(query: str) -> str:
    q = query.lower()

    if re.search(r"\b(formula|definition|defined|calculated|how is .* calculated|how .* calculate)\b", q):
        return "factor_definition_query"
    if re.search(r"\b(why|caused|cause|underperform|underperformed|explain what happened)\b", q):
        return "hybrid_explanation_query"
    if re.search(r"\b(research notes|find notes|notes related|related to|documents?)\b", q):
        return "research_note_query"
    if re.search(r"\b(regime|high[- ]volatility|low[- ]volatility|recovery|drawdown|bull[- ]trend)\b", q):
        return "regime_analysis_query"
    if "compare" in q:
        return "backtest_comparison_query"
    if re.search(r"\b(sharpe|max drawdown|turnover|annual return|best strategy|best factor|metrics?)\b", q):
        return "structured_metric_query"
    if re.search(r"\b(calculate|difference|spread)\b", q):
        return "calculation_query"

    return "ambiguous_query"


def route_for_query_type(query_type: str) -> str:
    return QUERY_TYPE_TO_ROUTE.get(query_type, "clarification_or_safe_response")


def route_query(query: str) -> tuple[str, str]:
    query_type = classify_query(query)
    return query_type, route_for_query_type(query_type)
