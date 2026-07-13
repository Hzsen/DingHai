from __future__ import annotations
from quant_agent.config import Paths
from quant_agent.retrieval.hybrid_retriever import HybridRetriever, build_retrievers
class RetrievalTool:
    def __init__(self, paths: Paths | None = None):
        self.paths = paths or Paths(); self.paths.ensure_processed_dirs()
        if not self.paths.bm25_index_path.exists() or not self.paths.vector_index_path.exists(): build_retrievers(self.paths.docs_dir, self.paths.bm25_index_path, self.paths.vector_index_path)
        self.retriever = HybridRetriever.from_paths(self.paths.bm25_index_path, self.paths.vector_index_path)
    def search(self, query: str, top_k: int = 5) -> list[dict[str, object]]: return self.retriever.search(query, top_k=top_k)
