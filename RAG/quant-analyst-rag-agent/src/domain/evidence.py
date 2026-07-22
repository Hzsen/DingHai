from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TypeAlias


JSONScalar: TypeAlias = str | int | float | bool | None


@dataclass(frozen=True, slots=True)
class EvidenceExcerpt:
    evidence_id: str
    document_id: str
    document_version: int
    chunk_id: str
    title: str
    section: str
    text: str
    source_uri: str | None
    available_at: datetime
    reliability: str
    token_estimate: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class DroppedEvidence:
    evidence_id: str
    reason_code: str


@dataclass(frozen=True, slots=True)
class EvidencePacket:
    packet_id: str
    query: str
    as_of: datetime
    numeric_evidence: dict[str, JSONScalar]
    contexts: tuple[EvidenceExcerpt, ...]
    dropped: tuple[DroppedEvidence, ...]
    token_budget: int
    estimated_tokens: int
    policy_version: str

    def __post_init__(self) -> None:
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if self.estimated_tokens > self.token_budget:
            raise ValueError("estimated_tokens must not exceed token_budget")
        ids = [context.evidence_id for context in self.contexts]
        if len(ids) != len(set(ids)):
            raise ValueError("context evidence_id values must be unique")


@dataclass(frozen=True, slots=True)
class GroundedSynthesisResult:
    payload: dict[str, object]
    mode: str
    cache_hit: bool
    warning: str | None = None
