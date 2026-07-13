from quant_agent.config import Paths
from quant_agent.retrieval.document_loader import load_markdown_documents
from quant_agent.retrieval.hybrid_retriever import HybridRetriever

def test_hybrid_retrieves_march_2020_momentum_note():
    paths = Paths(); docs = load_markdown_documents(paths.docs_dir); retriever = HybridRetriever.from_documents(docs); results = retriever.search("What caused momentum to underperform in March 2020?", top_k=3)
    assert any(row["source_path"] == "research_notes/march_2020_momentum_underperformance.md" for row in results); assert "hybrid_score" in results[0]
