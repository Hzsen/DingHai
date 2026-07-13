from __future__ import annotations

import math
import pickle
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from quant_agent.retrieval.bm25_retriever import tokenize
from quant_agent.retrieval.document_loader import DocumentChunk

SYNONYMS = {"caused": ["drivers", "driver", "because"], "cause": ["driver", "because"], "underperform": ["underperformed", "underperformance", "weak"], "shock": ["stress", "spike", "drawdown"], "calculated": ["formula", "measures"], "research": ["notes", "interpretation"]}


def expand_tokens(tokens: list[str]) -> list[str]:
    expanded = list(tokens)
    for token in tokens:
        expanded.extend(SYNONYMS.get(token, []))
    return expanded


@dataclass
class VectorRetriever:
    documents: list[dict[str, str]]
    vectors: list[dict[str, float]]
    idf: dict[str, float]

    @classmethod
    def from_documents(cls, documents: list[DocumentChunk]) -> "VectorRetriever":
        docs = [doc.to_dict() for doc in documents]
        tokenized = [expand_tokens(tokenize(f"{doc.title} {doc.source_path} {doc.chunk_text}")) for doc in documents]
        df: dict[str, int] = {}
        for tokens in tokenized:
            for token in set(tokens):
                df[token] = df.get(token, 0) + 1
        n_docs = max(len(tokenized), 1)
        idf = {token: math.log((1 + n_docs) / (1 + count)) + 1 for token, count in df.items()}
        vectors = [cls._vectorize_tokens(tokens, idf) for tokens in tokenized]
        return cls(docs, vectors, idf)

    @staticmethod
    def _vectorize_tokens(tokens: list[str], idf: dict[str, float]) -> dict[str, float]:
        counts = Counter(tokens)
        if not counts:
            return {}
        max_count = max(counts.values())
        vector = {token: (count / max_count) * idf.get(token, 1.0) for token, count in counts.items()}
        norm = math.sqrt(sum(value * value for value in vector.values())) or 1.0
        return {token: value / norm for token, value in vector.items()}

    def _vectorize_query(self, query: str) -> dict[str, float]:
        return self._vectorize_tokens(expand_tokens(tokenize(query)), self.idf)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            pickle.dump(self, handle)

    @classmethod
    def load(cls, path: Path) -> "VectorRetriever":
        with path.open("rb") as handle:
            return pickle.load(handle)

    @staticmethod
    def _cosine(left: dict[str, float], right: dict[str, float]) -> float:
        if len(left) > len(right):
            left, right = right, left
        return sum(value * right.get(token, 0.0) for token, value in left.items()) if left and right else 0.0

    def search(self, query: str, top_k: int = 5) -> list[dict[str, object]]:
        query_vector = self._vectorize_query(query)
        results = []
        for document, vector in zip(self.documents, self.vectors):
            score = self._cosine(query_vector, vector)
            if score > 0:
                item = dict(document)
                item["vector_score"] = score
                results.append(item)
        results.sort(key=lambda row: row["vector_score"], reverse=True)
        return results[:top_k]
