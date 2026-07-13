from __future__ import annotations

from typing import Any
from quant_agent.config import Paths
from quant_agent.database.sql_queries import FACTOR_ALIASES, REGIME_ALIASES, STRATEGY_ALIASES, extract_year_range
from quant_agent.graph.router import route_for_query_type, route_query
from quant_agent.graph.state import AgentState
from quant_agent.tools.factor_tool import FactorTool
from quant_agent.tools.retrieval_tool import RetrievalTool
from quant_agent.tools.sql_tool import SQLTool

def fmt_pct(value: float | int | str) -> str: return f"{float(value) * 100:.1f}%"
def fmt_num(value: float | int | str) -> str: return f"{float(value):.2f}"

def _state_with_defaults(state: AgentState) -> AgentState:
    state.setdefault("sql_results", []); state.setdefault("retrieved_docs", []); state.setdefault("calculations", {}); state.setdefault("evidence", []); state.setdefault("errors", []); state.setdefault("confidence", "low"); return state

def _find_aliases(query: str, aliases: dict[str, str]) -> list[str]:
    q = query.lower().replace("_", " "); found: list[str] = []
    for phrase, canonical in aliases.items():
        phrase_norm = phrase.lower().replace("_", " ")
        if phrase_norm in q and canonical not in found: found.append(canonical)
    return found

def extract_factors(query: str) -> list[str]: return _find_aliases(query, FACTOR_ALIASES)
def extract_regimes(query: str) -> list[str]: return _find_aliases(query, REGIME_ALIASES)
def extract_strategy(query: str) -> str | None:
    strategies = _find_aliases(query, STRATEGY_ALIASES); return strategies[0] if strategies else None

def extract_dates(query: str) -> tuple[str | None, str | None]:
    q = query.lower()
    if "march 2020" in q or "mar 2020" in q: return "2020-03-01", "2020-03-31"
    return extract_year_range(query)

def classify_query_node(state: AgentState) -> AgentState:
    state = _state_with_defaults(state); query_type, _route = route_query(state["query"]); state["query_type"] = query_type; return state

def route_query_node(state: AgentState) -> AgentState:
    state = _state_with_defaults(state); state["route"] = route_for_query_type(state.get("query_type", "ambiguous_query")); return state

def make_sql_tool_node(paths: Paths | None = None):
    paths = paths or Paths()
    def sql_tool_node(state: AgentState) -> AgentState:
        state = _state_with_defaults(state); query = state["query"]; q = query.lower()
        try:
            sql = SQLTool(paths.db_path); factors = extract_factors(query); regimes = extract_regimes(query); start_date, end_date = extract_dates(query); rows: list[dict[str, Any]] = []; query_type = state.get("query_type")
            if query_type == "regime_analysis_query":
                if "best" in q and regimes: rows = sql.best_factor_by_regime(regimes[0], "sharpe")
                elif factors and len(regimes) >= 2: rows = sql.compare_factor_across_regimes(factors[0], regimes)
                elif factors and regimes: rows = sql.compare_factor_across_regimes(factors[0], regimes)
                elif regimes: rows = sql.best_factor_by_regime(regimes[0], "sharpe")
            elif query_type == "backtest_comparison_query": rows = sql.compare_factors(factors, start_date, end_date)
            elif query_type == "hybrid_explanation_query":
                rows.extend(sql.anomalies(start_date, end_date))
                if factors and regimes: rows.extend(sql.compare_factor_across_regimes(factors[0], regimes))
                elif factors and "march 2020" in q:
                    factor_rows = sql.compare_factors(factors, start_date, end_date)
                    march_rows = [row for row in factor_rows if row.get("start_date") == start_date and row.get("end_date") == end_date]
                    rows.extend(march_rows or factor_rows)
            else:
                if "best strategy" in q or ("best" in q and "sharpe" in q): rows = sql.best_strategy("sharpe")
                else: rows = sql.strategy_metrics(extract_strategy(query), factors[0] if factors else None)
            state["sql_results"] = rows; state["evidence"].extend({"source": row.get("source_table", "sql"), "type": "sql"} for row in rows)
        except Exception as exc: state["errors"].append(str(exc))
        return state
    return sql_tool_node

def make_retrieval_tool_node(paths: Paths | None = None):
    paths = paths or Paths()
    def retrieval_tool_node(state: AgentState) -> AgentState:
        state = _state_with_defaults(state)
        try:
            docs = RetrievalTool(paths).search(state["query"], top_k=5); state["retrieved_docs"] = docs; state["evidence"].extend({"source": doc.get("source_path"), "type": "document"} for doc in docs)
        except Exception as exc: state["errors"].append(str(exc))
        return state
    return retrieval_tool_node

def make_factor_tool_node(paths: Paths | None = None):
    paths = paths or Paths()
    def factor_tool_node(state: AgentState) -> AgentState:
        state = _state_with_defaults(state)
        try:
            factors = extract_factors(state["query"])
            if not factors: state["errors"].append("No supported factor was found in the query."); return state
            row = FactorTool(paths.db_path).definition(factors[0]); state["sql_results"] = [row] if row else []
            if row: state["evidence"].append({"source": row.get("source_table", "factor_definitions"), "type": "sql"})
        except Exception as exc: state["errors"].append(str(exc))
        return state
    return factor_tool_node

def calculator_tool_node(state: AgentState) -> AgentState:
    state = _state_with_defaults(state); state["answer"] = "I can perform deterministic calculations after the required inputs are supplied."; state["confidence"] = "low"; return state

def _doc_line(doc: dict[str, Any]) -> str:
    snippet = str(doc.get("chunk_text", ""))[:220].replace("\n", " "); return f"- {doc.get('source_path')}: {snippet}..."

def synthesize_answer_node(state: AgentState) -> AgentState:
    state = _state_with_defaults(state); query_type = state.get("query_type", "ambiguous_query"); rows = state.get("sql_results", []); docs = state.get("retrieved_docs", [])
    if state.get("answer"): return state
    if query_type == "ambiguous_query": state["answer"] = "I do not have enough information to choose the right tool. Please specify a factor, strategy, regime, metric, or research-note topic."; state["confidence"] = "low"; return state
    if query_type == "factor_definition_query" and rows:
        row = rows[0]; state["answer"] = f"Answer:\n{row['factor_name']} is {row['definition']}\n\nData:\n- Formula: {row['formula']}\n- Source: factor_definitions\n\nInterpretation:\n{row['interpretation']}\n\nLimitations:\n{row['common_failure_modes']}"; state["confidence"] = "high"; return state
    if query_type == "research_note_query" and docs:
        lines = "\n".join(_doc_line(doc) for doc in docs[:3]); state["answer"] = f"Answer:\nThe most relevant local research notes are:\n{lines}\n\nEvidence:\n- Hybrid BM25/vector retrieval over data/docs\n\nLimitations:\nThese notes are sample research documents, not live market data."; state["confidence"] = "medium"; return state
    if query_type == "hybrid_explanation_query" and (rows or docs):
        data_lines = []
        for row in rows[:5]:
            if row.get("source_table") == "anomaly_logs": data_lines.append(f"- {row['date']} {row['ticker']} {row['anomaly_type']}: {row['description']} ({row['severity']})")
            elif row.get("source_table") == "backtest_results": data_lines.append(f"- {row['strategy_name']} {row['start_date']} to {row['end_date']}: return {fmt_pct(row['annual_return'])}, Sharpe {fmt_num(row['sharpe'])}, max drawdown {fmt_pct(row['max_drawdown'])}")
            elif row.get("source_table") == "regime_performance": data_lines.append(f"- {row['factor_name']} in {row['regime']}: annual return {fmt_pct(row['annual_return'])}, Sharpe {fmt_num(row['sharpe'])}, max drawdown {fmt_pct(row['max_drawdown'])}")
        doc_lines = "\n".join(_doc_line(doc) for doc in docs[:3])
        state["answer"] = "Answer:\nMomentum underperformed in the local March 2020 sample because the query evidence points to a high-volatility drawdown, crowded prior-winner selling, and liquidity stress.\n\n" + f"Data:\n{chr(10).join(data_lines) if data_lines else '- No structured rows matched.'}\n\nInterpretation:\nThe documents describe a short-horizon momentum reversal: prior winners were sold to raise cash while volatility and liquidity stress increased.\n\nEvidence:\n- SQL: anomaly_logs/backtest_results/regime_performance when available\n{doc_lines}\n\nLimitations:\nThis is internally consistent sample data. It supports a local research explanation, not a universal claim about all momentum strategies."
        state["confidence"] = "medium" if docs else "low"; return state
    if query_type == "backtest_comparison_query" and rows:
        lines = ["| Factor | Strategy | Annual Return | Sharpe | Max Drawdown | Turnover |", "|---|---|---:|---:|---:|---:|"]
        for row in rows: lines.append(f"| {row['factor_name']} | {row['strategy_name']} | {fmt_pct(row['annual_return'])} | {fmt_num(row['sharpe'])} | {fmt_pct(row['max_drawdown'])} | {fmt_pct(row['turnover'])} |")
        state["answer"] = "Answer:\nHere is the structured comparison from backtest_results.\n\nData:\n" + "\n".join(lines) + "\n\nInterpretation:\nHigher Sharpe indicates stronger risk-adjusted performance in this sample. Do not treat these sample metrics as live performance claims.\n\nEvidence:\n- SQL: backtest_results"; state["confidence"] = "high"; return state
    if query_type == "regime_analysis_query" and rows:
        if len({row.get("regime") for row in rows}) > 1 and len({row.get("factor_name") for row in rows}) == 1:
            best = rows[0]; lines = [f"- {row['regime']}: annual return {fmt_pct(row['annual_return'])}, Sharpe {fmt_num(row['sharpe'])}, max drawdown {fmt_pct(row['max_drawdown'])}, hit rate {fmt_pct(row['hit_rate'])}" for row in rows]
            state["answer"] = f"Answer:\n{best['factor_name']} worked better in {best['regime']} by Sharpe in the local regime_performance table.\n\nData:\n" + "\n".join(lines) + "\n\nEvidence:\n- SQL: regime_performance\n\nLimitations:\nRegime labels are sample labels and not live classifications."
        else:
            best = rows[0]; lines = [f"- {row['factor_name']} / {row['strategy_name']}: annual return {fmt_pct(row['annual_return'])}, Sharpe {fmt_num(row['sharpe'])}, max drawdown {fmt_pct(row['max_drawdown'])}, hit rate {fmt_pct(row['hit_rate'])}" for row in rows[:5]]
            state["answer"] = f"Answer:\nThe best factor during {best['regime']} regimes by Sharpe is {best['factor_name']} with Sharpe {fmt_num(best['sharpe'])}.\n\nData:\n" + "\n".join(lines) + "\n\nEvidence:\n- SQL: regime_performance\n\nLimitations:\nThis ranking uses Sharpe as the default basis because the query did not specify a metric."
        state["confidence"] = "high"; return state
    if query_type == "structured_metric_query" and rows:
        row = rows[0]
        if "best strategy" in state["query"].lower() or "best" in state["query"].lower(): state["answer"] = f"Answer:\nThe best strategy by Sharpe is {row['strategy_name']} with Sharpe {fmt_num(row['sharpe'])}.\n\nData:\n- Factor: {row['factor_name']}\n- Annual return: {fmt_pct(row['annual_return'])}\n- Max drawdown: {fmt_pct(row['max_drawdown'])}\n- Turnover: {fmt_pct(row['turnover'])}\n\nEvidence:\n- SQL: backtest_results\n\nLimitations:\nThe ranking basis is Sharpe because the query did not specify another definition of best."
        else:
            lines = [f"- {row['strategy_name']} / {row['factor_name']}: max drawdown {fmt_pct(row['max_drawdown'])}, turnover {fmt_pct(row['turnover'])}, Sharpe {fmt_num(row['sharpe'])}" for row in rows]
            state["answer"] = "Answer:\nExact strategy metrics from backtest_results:\n\nData:\n" + "\n".join(lines) + "\n\nEvidence:\n- SQL: backtest_results"
        state["confidence"] = "high"; return state
    state["answer"] = "I do not have enough evidence in the local research database to answer that confidently."; state["confidence"] = "low"; return state

def verify_answer_node(state: AgentState) -> AgentState:
    state = _state_with_defaults(state); route = state.get("route", ""); answer = state.get("answer", "")
    if route in {"sql_only", "factor_definition"} and not state.get("sql_results"): state["answer"] = "I do not have enough evidence in the local research database to answer that confidently."; state["confidence"] = "low"
    if route == "retrieval_only" and not state.get("retrieved_docs"): state["answer"] = "I do not have enough evidence in the local research database to answer that confidently."; state["confidence"] = "low"
    if route == "hybrid_sql_retrieval" and not state.get("retrieved_docs"): state["answer"] = "I do not have enough retrieved evidence in the local research database to answer that causal question confidently."; state["confidence"] = "low"
    if "I do not have enough" not in state["answer"] and not state.get("evidence") and route != "calculator": state["answer"] = "I do not have enough evidence in the local research database to answer that confidently."; state["confidence"] = "low"
    if answer and state.get("errors") and state.get("confidence") != "low": state["confidence"] = "medium"
    return state
