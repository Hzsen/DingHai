from __future__ import annotations

import time
import uuid

from domain.query import RAGQueryRequest, RAGSearchResponse, RetrievedEvidence
from quant_agent.retrieval.canonical_hybrid import HYBRID_INDEX_VERSION, TemporalHybridRetriever
from quant_agent.retrieval.canonical_vector import CanonicalVectorIndex
from quant_agent.retrieval.lexical import INDEX_VERSION, CanonicalLexicalIndex


class RAGQueryService:
    """Application boundary shared by CLI and future HTTP/UI adapters."""

    def __init__(
        self,
        lexical_index: CanonicalLexicalIndex,
        vector_index: CanonicalVectorIndex | None = None,
    ) -> None:
        self.lexical_index = lexical_index
        self.vector_index = vector_index
        self.hybrid_retriever = (
            TemporalHybridRetriever(lexical_index, vector_index)
            if vector_index is not None
            else None
        )

    def search(self, request: RAGQueryRequest) -> RAGSearchResponse:
        started = time.perf_counter()
        hits = (
            self.hybrid_retriever.search(request)
            if self.hybrid_retriever is not None
            else self.lexical_index.search(request)
        )
        evidence = tuple(
            RetrievedEvidence(
                evidence_id=f"knowledge:{hit.chunk_id}@{hit.document_version}",
                document_id=hit.document_id,
                document_version=hit.document_version,
                chunk_id=hit.chunk_id,
                document_type=hit.document_type,
                title=hit.title,
                section=hit.section,
                text=hit.text,
                source_uri=hit.source_uri,
                event_time=hit.event_time,
                available_at=hit.available_at,
                reliability=hit.reliability,
                lexical_score=hit.lexical_score,
                semantic_score=getattr(hit, "semantic_score", 0.0),
                fusion_score=getattr(hit, "fusion_score", hit.lexical_score),
                reason_codes=tuple(
                    code
                    for code, matched in (
                        ("LEXICAL_MATCH", getattr(hit, "matched_lexical", True)),
                        ("SEMANTIC_MATCH", getattr(hit, "matched_semantic", False)),
                        ("POINT_IN_TIME_VISIBLE", True),
                    )
                    if matched
                ),
            )
            for hit in hits[:request.top_k]
        )
        warnings: list[str] = []
        if request.use_llm:
            warnings.append("SEARCH_ONLY_LLM_NOT_USED")
        if request.mode.value != "SEARCH_ONLY":
            warnings.append("RETRIEVAL_RETURNS_EVIDENCE_ONLY")
        elapsed_ms = (time.perf_counter() - started) * 1_000
        return RAGSearchResponse(
            query_id=str(uuid.uuid4()),
            mode=request.mode,
            query_text=request.query_text,
            data_as_of=request.as_of,
            evidence=evidence,
            warnings=tuple(warnings),
            index_mode=(
                f"sqlite-fts5+local-vector:{HYBRID_INDEX_VERSION}"
                if self.hybrid_retriever is not None
                else f"sqlite-fts5:{INDEX_VERSION}"
            ),
            timings_ms={
                "hybrid_search" if self.hybrid_retriever is not None else "lexical_search": round(elapsed_ms, 3),
                "total": round(elapsed_ms, 3),
            },
        )
