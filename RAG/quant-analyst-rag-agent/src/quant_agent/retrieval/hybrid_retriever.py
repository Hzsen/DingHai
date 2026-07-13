from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from quant_agent.retrieval.bm25_retriever import BM25Retriever
from quant_agent.retrieval.document_loader import DocumentChunk, load_markdown_documents
from quant_agent.retrieval.vector_retriever import VectorRetriever


def _normalize(scores: dict[str, float]) -> dict[str, float]:
    if not scores:
        return {}
    max_score = max(scores.values()) or 1.0
    return {key: value / max_score for key, value in scores.items()}


@dataclass
class HybridRetriever:
    bm25: BM25Retriever
    vector: VectorRetriever
    alpha: float = 0.5

    @classmethod
    def from_documents(cls, documents: list[DocumentChunk], alpha: float = 0.5) -> "HybridRetriever":
        return cls(BM25Retriever.from_documents(documents), VectorRetriever.from_documents(documents), alpha)

    @classmethod
    def from_paths(cls, bm25_path: Path, vector_path: Path, alpha: float = 0.5) -> "HybridRetriever":
        return cls(BM25Retriever.load(bm25_path), VectorRetriever.load(vector_path), alpha)

    def search(self, query: str, top_k: int = 5) -> list[dict[str, object]]:
        bm25_results = self.bm25.search(query, top_k=max(top_k * 3, 10))
        vector_results = self.vector.search(query, top_k=max(top_k * 3, 10))
        bm25_scores = {str(row["document_id"]): float(row.get("bm25_score", 0.0)) for row in bm25_results}
        vector_scores = {str(row["document_id"]): float(row.get("vector_score", 0.0)) for row in vector_results}
        norm_bm25 = _normalize(bm25_scores)
        norm_vector = _normalize(vector_scores)
        merged: dict[str, dict[str, object]] = {}
        for row in bm25_results + vector_results:
            merged.setdefault(str(row["document_id"]), dict(row))
        for document_id, row in merged.items():
            row["bm25_score"] = bm25_scores.get(document_id, 0.0)
            row["vector_score"] = vector_scores.get(document_id, 0.0)
            row["hybrid_score"] = self.alpha * norm_bm25.get(document_id, 0.0) + (1 - self.alpha) * norm_vector.get(document_id, 0.0)
        results = list(merged.values())
        results.sort(key=lambda row: row["hybrid_score"], reverse=True)
        return results[:top_k]


def build_retrievers(docs_dir: Path, bm25_path: Path, vector_path: Path) -> tuple[BM25Retriever, VectorRetriever]:
    documents = load_markdown_documents(docs_dir)
    bm25 = BM25Retriever.from_documents(documents)
    vector = VectorRetriever.from_documents(documents)
    bm25.save(bm25_path)
    vector.save(vector_path)
    return bm25, vector
