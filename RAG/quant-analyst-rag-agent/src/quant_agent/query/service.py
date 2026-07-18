from __future__ import annotations

import time
import uuid

from domain.query import RAGQueryRequest, RAGSearchResponse, RetrievedEvidence
from quant_agent.retrieval.lexical import INDEX_VERSION, CanonicalLexicalIndex


class RAGQueryService:
    """Application boundary shared by CLI and future HTTP/UI adapters."""

    def __init__(self, lexical_index: CanonicalLexicalIndex) -> None:
        self.lexical_index = lexical_index

    def search(self, request: RAGQueryRequest) -> RAGSearchResponse:
        started = time.perf_counter()
        hits = self.lexical_index.search(request)
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
                semantic_score=0.0,
                fusion_score=hit.lexical_score,
                reason_codes=("LEXICAL_MATCH", "POINT_IN_TIME_VISIBLE"),
            )
            for hit in hits[:request.top_k]
        )
        warnings: list[str] = []
        if request.use_llm:
            warnings.append("PHASE_A_SEARCH_ONLY_LLM_NOT_USED")
        if request.mode.value != "SEARCH_ONLY":
            warnings.append("PHASE_A_RETURNS_EVIDENCE_ONLY")
        elapsed_ms = (time.perf_counter() - started) * 1_000
        return RAGSearchResponse(
            query_id=str(uuid.uuid4()),
            mode=request.mode,
            query_text=request.query_text,
            data_as_of=request.as_of,
            evidence=evidence,
            warnings=tuple(warnings),
            index_mode=f"sqlite-fts5:{INDEX_VERSION}",
            timings_ms={"lexical_search": round(elapsed_ms, 3), "total": round(elapsed_ms, 3)},
        )
