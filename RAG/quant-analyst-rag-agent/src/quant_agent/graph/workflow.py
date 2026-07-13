from __future__ import annotations

from quant_agent.config import Paths
from quant_agent.database.build_db import build_database
from quant_agent.graph.nodes import (
    calculator_tool_node,
    classify_query_node,
    make_factor_tool_node,
    make_retrieval_tool_node,
    make_sql_tool_node,
    route_query_node,
    synthesize_answer_node,
    verify_answer_node,
)
from quant_agent.graph.state import AgentState


class SequentialWorkflow:
    def __init__(self, paths: Paths | None = None):
        self.paths = paths or Paths()
        self.sql_node = make_sql_tool_node(self.paths)
        self.retrieval_node = make_retrieval_tool_node(self.paths)
        self.factor_node = make_factor_tool_node(self.paths)

    def invoke(self, state: AgentState) -> AgentState:
        state = classify_query_node(state)
        state = route_query_node(state)
        route = state.get("route")

        if route == "sql_only":
            state = self.sql_node(state)
        elif route == "retrieval_only":
            state = self.retrieval_node(state)
        elif route == "hybrid_sql_retrieval":
            state = self.sql_node(state)
            state = self.retrieval_node(state)
        elif route == "factor_definition":
            state = self.factor_node(state)
        elif route == "calculator":
            state = calculator_tool_node(state)

        state = synthesize_answer_node(state)
        return verify_answer_node(state)


def _route_selector(state: AgentState) -> str:
    return state.get("route", "clarification_or_safe_response")


def create_workflow(paths: Paths | None = None):
    paths = paths or Paths()
    try:
        from langgraph.graph import END, StateGraph
    except ImportError:
        return SequentialWorkflow(paths)
    graph = StateGraph(AgentState)
    graph.add_node("classify_query_node", classify_query_node)
    graph.add_node("route_query_node", route_query_node)
    graph.add_node("sql_tool_node", make_sql_tool_node(paths))
    graph.add_node("retrieval_tool_node", make_retrieval_tool_node(paths))
    graph.add_node("factor_tool_node", make_factor_tool_node(paths))
    graph.add_node("calculator_tool_node", calculator_tool_node)
    graph.add_node("synthesize_answer_node", synthesize_answer_node)
    graph.add_node("verify_answer_node", verify_answer_node)

    graph.set_entry_point("classify_query_node")
    graph.add_edge("classify_query_node", "route_query_node")
    graph.add_conditional_edges(
        "route_query_node",
        _route_selector,
        {
            "sql_only": "sql_tool_node",
            "retrieval_only": "retrieval_tool_node",
            "hybrid_sql_retrieval": "sql_tool_node",
            "factor_definition": "factor_tool_node",
            "calculator": "calculator_tool_node",
            "clarification_or_safe_response": "synthesize_answer_node",
        },
    )
    graph.add_conditional_edges(
        "sql_tool_node",
        lambda state: "retrieval_tool_node"
        if state.get("route") == "hybrid_sql_retrieval"
        else "synthesize_answer_node",
        {
            "retrieval_tool_node": "retrieval_tool_node",
            "synthesize_answer_node": "synthesize_answer_node",
        },
    )
    graph.add_edge("retrieval_tool_node", "synthesize_answer_node")
    graph.add_edge("factor_tool_node", "synthesize_answer_node")
    graph.add_edge("calculator_tool_node", "synthesize_answer_node")
    graph.add_edge("synthesize_answer_node", "verify_answer_node")
    graph.add_edge("verify_answer_node", END)
    return graph.compile()


def ensure_runtime_artifacts(paths: Paths | None = None) -> None:
    paths = paths or Paths()
    paths.ensure_processed_dirs()
    if not paths.db_path.exists():
        build_database(paths.db_path, paths.raw_data_dir)


def ask(query: str, paths: Paths | None = None) -> AgentState:
    paths = paths or Paths()
    ensure_runtime_artifacts(paths)
    workflow = create_workflow(paths)
    return workflow.invoke(
        {
            "query": query,
            "errors": [],
            "sql_results": [],
            "retrieved_docs": [],
            "calculations": {},
            "evidence": [],
        }
    )
