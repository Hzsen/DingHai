from quant_agent.config import Paths
from quant_agent.retrieval.bm25_retriever import BM25Retriever
from quant_agent.retrieval.document_loader import load_markdown_documents

def test_bm25_retrieves_liquidity_shock_note():
    paths = Paths(); docs = load_markdown_documents(paths.docs_dir); retriever = BM25Retriever.from_documents(docs); results = retriever.search("liquidity shock", top_k=3)
    assert results; assert results[0]["source_path"] == "research_notes/liquidity_shock_notes.md"
