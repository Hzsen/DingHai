from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from domain.knowledge import (
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeReliability,
)


class QueryMode(str, Enum):
    SEARCH_ONLY = "SEARCH_ONLY"
    ANSWER = "ANSWER"
    CAUSAL_RESEARCH = "CAUSAL_RESEARCH"
    MACRO_RESEARCH = "MACRO_RESEARCH"
    CN_EQUITY_RESEARCH = "CN_EQUITY_RESEARCH"


def _require_aware(field_name: str, value: datetime | None) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{field_name} must be timezone-aware")


def _unique_nonempty(field_name: str, values: tuple[str, ...]) -> None:
    if any(not value.strip() for value in values):
        raise ValueError(f"{field_name} values must not be empty")
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} values must be unique")


@dataclass(frozen=True, slots=True)
class RAGQueryRequest:
    query_text: str
    as_of: datetime
    mode: QueryMode = QueryMode.SEARCH_ONLY
    tickers: tuple[str, ...] = ()
    themes: tuple[str, ...] = ()
    document_types: tuple[KnowledgeDocumentType, ...] = ()
    statuses: tuple[KnowledgeDocumentStatus, ...] = (KnowledgeDocumentStatus.FINALIZED,)
    reliability: tuple[KnowledgeReliability, ...] = ()
    event_time_from: datetime | None = None
    event_time_to: datetime | None = None
    top_k: int = 8
    use_llm: bool = False

    def __post_init__(self) -> None:
        if not self.query_text or not self.query_text.strip():
            raise ValueError("query_text must not be empty")
        for field_name in ("as_of", "event_time_from", "event_time_to"):
            _require_aware(field_name, getattr(self, field_name))
        _unique_nonempty("tickers", self.tickers)
        _unique_nonempty("themes", self.themes)
        if not 1 <= self.top_k <= 50:
            raise ValueError("top_k must be between 1 and 50")
        if not self.statuses:
            raise ValueError("statuses must not be empty")
        if KnowledgeDocumentStatus.RETRACTED in self.statuses:
            raise ValueError("RETRACTED documents cannot be requested")
        if self.event_time_from and self.event_time_to and self.event_time_from > self.event_time_to:
            raise ValueError("event_time_from must not be after event_time_to")


@dataclass(frozen=True, slots=True)
class RetrievedEvidence:
    evidence_id: str
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
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        for field_name in ("evidence_id", "document_id", "chunk_id", "title", "section", "text"):
            value = getattr(self, field_name)
            if not value or not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.document_version < 1:
            raise ValueError("document_version must be >= 1")
        _require_aware("event_time", self.event_time)
        _require_aware("available_at", self.available_at)
        if min(self.lexical_score, self.semantic_score, self.fusion_score) < 0:
            raise ValueError("retrieval scores must be >= 0")
        _unique_nonempty("reason_codes", self.reason_codes)


@dataclass(frozen=True, slots=True)
class RAGSearchResponse:
    query_id: str
    mode: QueryMode
    query_text: str
    data_as_of: datetime
    evidence: tuple[RetrievedEvidence, ...]
    warnings: tuple[str, ...]
    index_mode: str
    timings_ms: dict[str, float]

    def __post_init__(self) -> None:
        if not self.query_id.strip():
            raise ValueError("query_id must not be empty")
        if not self.query_text.strip():
            raise ValueError("query_text must not be empty")
        _require_aware("data_as_of", self.data_as_of)
        _unique_nonempty("warnings", self.warnings)
        if not self.index_mode.strip():
            raise ValueError("index_mode must not be empty")
        if any(value < 0 for value in self.timings_ms.values()):
            raise ValueError("timings_ms values must be >= 0")
