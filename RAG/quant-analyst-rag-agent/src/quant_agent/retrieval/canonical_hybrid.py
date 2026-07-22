from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from domain.knowledge import KnowledgeDocumentType, KnowledgeReliability
from domain.query import RAGQueryRequest
from quant_agent.retrieval.canonical_vector import CanonicalVectorIndex
from quant_agent.retrieval.lexical import CanonicalLexicalIndex


HYBRID_INDEX_VERSION = "temporal-hybrid-rrf-v1.0.0"


@dataclass(frozen=True, slots=True)
class HybridHit:
    document_id: str
    document_version: int
    chunk_id: str
    document_type: KnowledgeDocumentType
    title: str
    section: str
    text: str
    source_uri: str | None
    event_time: datetime | None
    available_at: datetime
    reliability: KnowledgeReliability
    lexical_score: float
    semantic_score: float
    fusion_score: float
    matched_lexical: bool
    matched_semantic: bool


class TemporalHybridRetriever:
    """Fuse two independently point-in-time-filtered candidate rankings at chunk level."""

    def __init__(
        self,
        lexical_index: CanonicalLexicalIndex,
        vector_index: CanonicalVectorIndex,
        *,
        lexical_weight: float = 0.55,
        rrf_k: int = 60,
    ) -> None:
        if lexical_index.db_path.resolve() != vector_index.db_path.resolve():
            raise ValueError("lexical and vector indexes must use the same SQLite database")
        if not 0 <= lexical_weight <= 1:
            raise ValueError("lexical_weight must be between 0 and 1")
        if rrf_k < 1:
            raise ValueError("rrf_k must be >= 1")
        self.lexical_index = lexical_index
        self.vector_index = vector_index
        self.lexical_weight = lexical_weight
        self.rrf_k = rrf_k

    def search(self, request: RAGQueryRequest) -> list[HybridHit]:
        candidate_limit = max(request.top_k * 5, 20)
        lexical_hits = self.lexical_index.search(request, candidate_limit=candidate_limit)
        vector_hits = self.vector_index.search(request, candidate_limit=candidate_limit)
        lexical_ranks = {
            (hit.chunk_id, hit.document_version): rank
            for rank, hit in enumerate(lexical_hits, start=1)
        }
        vector_ranks = {
            (hit.chunk_id, hit.document_version): rank
            for rank, hit in enumerate(vector_hits, start=1)
        }
        lexical_by_id = {
            (hit.chunk_id, hit.document_version): hit for hit in lexical_hits
        }
        vector_by_id = {
            (hit.chunk_id, hit.document_version): hit for hit in vector_hits
        }
        identities = lexical_by_id.keys() | vector_by_id.keys()
        maximum_rrf = 1.0 / (self.rrf_k + 1)
        output: list[HybridHit] = []
        for identity in identities:
            lexical = lexical_by_id.get(identity)
            semantic = vector_by_id.get(identity)
            source = lexical or semantic
            lexical_component = (
                self.lexical_weight / (self.rrf_k + lexical_ranks[identity])
                if lexical is not None
                else 0.0
            )
            semantic_component = (
                (1.0 - self.lexical_weight) / (self.rrf_k + vector_ranks[identity])
                if semantic is not None
                else 0.0
            )
            output.append(HybridHit(
                document_id=source.document_id,
                document_version=source.document_version,
                chunk_id=source.chunk_id,
                document_type=source.document_type,
                title=source.title,
                section=source.section,
                text=source.text,
                source_uri=source.source_uri,
                event_time=source.event_time,
                available_at=source.available_at,
                reliability=source.reliability,
                lexical_score=lexical.lexical_score if lexical is not None else 0.0,
                semantic_score=semantic.semantic_score if semantic is not None else 0.0,
                fusion_score=round((lexical_component + semantic_component) / maximum_rrf, 8),
                matched_lexical=lexical is not None,
                matched_semantic=semantic is not None,
            ))
        output.sort(
            key=lambda hit: (
                hit.fusion_score,
                hit.lexical_score,
                hit.semantic_score,
                hit.available_at,
                hit.chunk_id,
            ),
            reverse=True,
        )
        return output[:request.top_k]
