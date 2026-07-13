from __future__ import annotations

import math
import pickle
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from quant_agent.retrieval.document_loader import DocumentChunk

TOKEN_RE = re.compile(r"[a-z0-9_]+")


def tokenize(text: str) -> list[str]:
    normalized = text.lower().replace("-", "_")
    tokens = TOKEN_RE.findall(normalized)
    expanded = []
    for token in tokens:
        expanded.append(token)
        if "_" in token:
            expanded.extend(part for part in token.split("_") if part)
    return expanded


@dataclass
class BM25Retriever:
    documents: list[dict[str, str]]
    tokenized_docs: list[list[str]]
    doc_freq: dict[str, int]
    avg_doc_len: float
    k1: float = 1.5
    b: float = 0.75

    @classmethod
    def from_documents(cls, documents: list[DocumentChunk]) -> "BM25Retriever":
        docs = [doc.to_dict() for doc in documents]
        tokenized = [tokenize(f"{doc.title} {doc.source_path} {doc.chunk_text}") for doc in documents]
        df: dict[str, int] = {}
        for tokens in tokenized:
            for token in set(tokens):
                df[token] = df.get(token, 0) + 1
        avg_len = sum(len(tokens) for tokens in tokenized) / max(len(tokenized), 1)
        return cls(docs, tokenized, df, avg_len)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            pickle.dump(self, handle)

    @classmethod
    def load(cls, path: Path) -> "BM25Retriever":
        with path.open("rb") as handle:
            return pickle.load(handle)

    def _idf(self, token: str) -> float:
        n_docs = len(self.documents)
        df = self.doc_freq.get(token, 0)
        return math.log(1 + (n_docs - df + 0.5) / (df + 0.5))

    def _score(self, query_tokens: list[str], doc_index: int) -> float:
        tokens = self.tokenized_docs[doc_index]
        counts = Counter(tokens)
        doc_len = len(tokens) or 1
        score = 0.0
        for token in query_tokens:
            freq = counts.get(token, 0)
            if not freq:
                continue
            denom = freq + self.k1 * (1 - self.b + self.b * doc_len / max(self.avg_doc_len, 1))
            score += self._idf(token) * freq * (self.k1 + 1) / denom
        return score

    def search(self, query: str, top_k: int = 5) -> list[dict[str, object]]:
        query_tokens = tokenize(query)
        query_text = query.lower()
        results = []
        for index, document in enumerate(self.documents):
            score = self._score(query_tokens, index)
            haystack = f"{document['title']} {document['source_path']} {document['chunk_text']}".lower()
            if query_text and query_text in haystack:
                score += 2.0
            if score > 0:
                item = dict(document)
                item["bm25_score"] = score
                results.append(item)
        results.sort(key=lambda row: row["bm25_score"], reverse=True)
        return results[:top_k]
