from quant_agent.config import Paths
from quant_agent.database.build_db import build_database
from quant_agent.graph.workflow import ask
from quant_agent.retrieval.hybrid_retriever import build_retrievers

def test_graph_answers_regime_question():
    paths = Paths(); build_database(paths.db_path, paths.raw_data_dir); build_retrievers(paths.docs_dir, paths.bm25_index_path, paths.vector_index_path); result = ask("Which factor performed best during high-volatility regimes?", paths)
    assert result["route"] == "sql_only"; assert "sector_relative_strength" in result["answer"]; assert result["confidence"] == "high"

def test_graph_answers_hybrid_question():
    paths = Paths(); build_database(paths.db_path, paths.raw_data_dir); build_retrievers(paths.docs_dir, paths.bm25_index_path, paths.vector_index_path); result = ask("What caused the momentum strategy to underperform in March 2020?", paths)
    assert result["route"] == "hybrid_sql_retrieval"; assert result["retrieved_docs"]; assert "Momentum underperformed" in result["answer"]
