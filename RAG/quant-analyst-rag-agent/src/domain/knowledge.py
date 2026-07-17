from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TypeAlias


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class KnowledgeDocumentType(str, Enum):
    FACTOR_DEFINITION = "FACTOR_DEFINITION"
    WEEKLY_RESEARCH = "WEEKLY_RESEARCH"
    THESIS_UPDATE = "THESIS_UPDATE"
    THEME_RESEARCH = "THEME_RESEARCH"
    FUNDAMENTAL_EVIDENCE = "FUNDAMENTAL_EVIDENCE"
    RISK_EVENT = "RISK_EVENT"
    MARKET_REGIME = "MARKET_REGIME"
    SCREENING_REPORT = "SCREENING_REPORT"
    BACKTEST_NOTE = "BACKTEST_NOTE"
    ADR = "ADR"
    MACRO_VIEWPOINT = "MACRO_VIEWPOINT"


class KnowledgeDocumentStatus(str, Enum):
    DRAFT = "DRAFT"
    FINALIZED = "FINALIZED"
    SUPERSEDED = "SUPERSEDED"
    RETRACTED = "RETRACTED"


class KnowledgeChunkType(str, Enum):
    BODY = "BODY"
    SUMMARY = "SUMMARY"
    FACTOR_DEFINITION = "FACTOR_DEFINITION"
    THESIS = "THESIS"
    EVIDENCE = "EVIDENCE"
    RISK = "RISK"
    STATE_CHANGE = "STATE_CHANGE"
    WEEKLY_SUMMARY = "WEEKLY_SUMMARY"
    DRAFT_SUMMARY = "DRAFT_SUMMARY"
    VIEWPOINT = "VIEWPOINT"
    INVALIDATION = "INVALIDATION"


class KnowledgeSourceType(str, Enum):
    COMPANY_DISCLOSURE = "COMPANY_DISCLOSURE"
    FINANCIAL_STATEMENT = "FINANCIAL_STATEMENT"
    REGULATORY = "REGULATORY"
    RESEARCH_REPORT = "RESEARCH_REPORT"
    NEWS = "NEWS"
    WEB = "WEB"
    MARKET_DATA = "MARKET_DATA"
    MANUAL_NOTE = "MANUAL_NOTE"
    SYSTEM_DERIVED = "SYSTEM_DERIVED"
    LICENSED_RESEARCH = "LICENSED_RESEARCH"
    PRIVATE_CHANNEL = "PRIVATE_CHANNEL"
    UNKNOWN = "UNKNOWN"


class KnowledgeReliability(str, Enum):
    PRIMARY = "PRIMARY"
    DERIVED = "DERIVED"
    SECONDARY = "SECONDARY"
    UNVERIFIED = "UNVERIFIED"
    CONFLICTED = "CONFLICTED"


def content_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_json_sha256(value: JsonValue) -> str:
    serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _require_text(field_name: str, value: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_aware(field_name: str, value: datetime | None) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_hash(field_name: str, value: str) -> None:
    if not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 hex digest")


def _validate_metadata(metadata: dict[str, JsonValue]) -> None:
    if any(not isinstance(key, str) or not key.strip() for key in metadata):
        raise ValueError("metadata keys must be non-empty strings")
    try:
        json.dumps(metadata, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("metadata must be finite JSON-serializable data") from exc


def _normalized_unique(values: tuple[str, ...], field_name: str) -> None:
    if any(not value.strip() for value in values):
        raise ValueError(f"{field_name} values must not be empty")
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} values must be unique")


@dataclass(frozen=True, slots=True)
class KnowledgeDocument:
    """Canonical, versioned research document metadata and source content.

    ``event_time`` is when the described event occurred. ``as_of`` is the
    information cutoff represented by the document. ``available_at`` is the first
    time the system/user could legitimately know the document. Historical retrieval
    must filter on ``available_at``, never merely on ``event_time`` or ``as_of``.
    """

    document_id: str
    document_type: KnowledgeDocumentType
    title: str
    content: str
    tickers: tuple[str, ...]
    themes: tuple[str, ...]
    thesis_id: str | None
    event_time: datetime | None
    as_of: datetime
    available_at: datetime
    status: KnowledgeDocumentStatus
    version: int
    source_type: KnowledgeSourceType
    source_uri: str | None
    source_hash: str
    content_hash: str
    reliability: KnowledgeReliability
    language: str
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in ("document_id", "title", "content", "language"):
            _require_text(field_name, getattr(self, field_name))
        if self.thesis_id is not None:
            _require_text("thesis_id", self.thesis_id)
        if self.source_uri is not None:
            _require_text("source_uri", self.source_uri)
        _normalized_unique(self.tickers, "tickers")
        _normalized_unique(self.themes, "themes")
        for field_name in ("event_time", "as_of", "available_at", "created_at", "updated_at"):
            _require_aware(field_name, getattr(self, field_name))
        if self.version < 1:
            raise ValueError("version must be >= 1")
        _require_hash("source_hash", self.source_hash)
        _require_hash("content_hash", self.content_hash)
        if self.content_hash != content_sha256(self.content):
            raise ValueError("content_hash does not match content")
        _validate_metadata(self.metadata)
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must not be before created_at")

    @property
    def retrieval_enabled(self) -> bool:
        return self.status not in {KnowledgeDocumentStatus.SUPERSEDED, KnowledgeDocumentStatus.RETRACTED}

    def visible_at(self, query_as_of: datetime) -> bool:
        _require_aware("query_as_of", query_as_of)
        return self.retrieval_enabled and self.available_at <= query_as_of

    def accepts_chunk(self, chunk: "KnowledgeChunk") -> bool:
        return chunk.document_id == self.document_id and chunk.document_version == self.version

    def chunk_visible_at(self, chunk: "KnowledgeChunk", query_as_of: datetime) -> bool:
        if not self.accepts_chunk(chunk):
            raise ValueError("chunk document identity/version does not match document")
        return self.visible_at(query_as_of) and chunk.visible_at(query_as_of)


@dataclass(frozen=True, slots=True)
class KnowledgeChunk:
    """Smallest independently indexed and cited unit of knowledge."""

    chunk_id: str
    document_id: str
    document_version: int
    chunk_type: KnowledgeChunkType
    section: str
    text: str
    ordinal: int
    event_time: datetime | None
    available_at: datetime
    content_hash: str
    token_count: int
    indexable: bool
    metadata: dict[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in ("chunk_id", "document_id", "section", "text"):
            _require_text(field_name, getattr(self, field_name))
        if self.document_version < 1:
            raise ValueError("document_version must be >= 1")
        if self.ordinal < 0:
            raise ValueError("ordinal must be >= 0")
        if self.token_count < 1:
            raise ValueError("token_count must be >= 1")
        for field_name in ("event_time", "available_at"):
            _require_aware(field_name, getattr(self, field_name))
        _require_hash("content_hash", self.content_hash)
        if self.content_hash != content_sha256(self.text):
            raise ValueError("content_hash does not match text")
        _validate_metadata(self.metadata)
        if self.chunk_type == KnowledgeChunkType.DRAFT_SUMMARY and self.indexable:
            raise ValueError("DRAFT_SUMMARY chunks must not be indexable")

    def visible_at(self, query_as_of: datetime) -> bool:
        _require_aware("query_as_of", query_as_of)
        return self.indexable and self.available_at <= query_as_of


@dataclass(frozen=True, slots=True)
class KnowledgeQuery:
    """Typed retrieval boundary; vector similarity is applied after these filters."""

    query_text: str
    as_of: datetime
    tickers: tuple[str, ...] = ()
    themes: tuple[str, ...] = ()
    document_types: tuple[KnowledgeDocumentType, ...] = ()
    statuses: tuple[KnowledgeDocumentStatus, ...] = (KnowledgeDocumentStatus.FINALIZED, KnowledgeDocumentStatus.DRAFT)
    event_time_from: datetime | None = None
    event_time_to: datetime | None = None
    reliability: tuple[KnowledgeReliability, ...] = ()
    top_k: int = 5

    def __post_init__(self) -> None:
        _require_text("query_text", self.query_text)
        for field_name in ("as_of", "event_time_from", "event_time_to"):
            _require_aware(field_name, getattr(self, field_name))
        _normalized_unique(self.tickers, "tickers")
        _normalized_unique(self.themes, "themes")
        if self.event_time_from and self.event_time_to and self.event_time_from > self.event_time_to:
            raise ValueError("event_time_from must not be after event_time_to")
        if not 1 <= self.top_k <= 100:
            raise ValueError("top_k must be between 1 and 100")
        if KnowledgeDocumentStatus.RETRACTED in self.statuses:
            raise ValueError("RETRACTED documents cannot be requested for retrieval")
