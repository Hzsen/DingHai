from __future__ import annotations
from typing import Any
from quant_agent.config import Paths
from quant_agent.retrieval.hybrid_retriever import HybridRetriever, build_retrievers
TEST_CASES = [{"query": "liquidity shock", "expected_doc": "research_notes/liquidity_shock_notes.md"}, {"query": "momentum March 2020 underperformance", "expected_doc": "research_notes/march_2020_momentum_underperformance.md"}, {"query": "high volatility regime momentum", "expected_doc": "research_notes/high_volatility_regime_notes.md"}]
def run_retrieval_eval(paths: Paths | None = None) -> dict[str, Any]:
    paths = paths or Paths(); paths.ensure_processed_dirs()
    if not paths.bm25_index_path.exists() or not paths.vector_index_path.exists(): build_retrievers(paths.docs_dir, paths.bm25_index_path, paths.vector_index_path)
    retriever = HybridRetriever.from_paths(paths.bm25_index_path, paths.vector_index_path); top1 = 0; top3 = 0; reciprocal_ranks = []; failures = []
    for case in TEST_CASES:
        results = retriever.search(case["query"], top_k=5); docs = [str(row["source_path"]) for row in results]; expected = case["expected_doc"]
        if docs and docs[0] == expected: top1 += 1
        if expected in docs[:3]: top3 += 1
        if expected in docs: reciprocal_ranks.append(1 / (docs.index(expected) + 1))
        else: reciprocal_ranks.append(0); failures.append({"query": case["query"], "expected": expected, "actual_top": docs[:3]})
    total = len(TEST_CASES); return {"top_1_accuracy": top1 / total, "top_3_accuracy": top3 / total, "mrr": sum(reciprocal_ranks) / total, "failed_cases": failures}
