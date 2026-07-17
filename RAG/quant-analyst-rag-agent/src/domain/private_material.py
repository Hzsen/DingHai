from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class MaterialSensitivity(str, Enum):
    PUBLIC = "PUBLIC"
    LICENSED_LOCAL_ONLY = "LICENSED_LOCAL_ONLY"
    PRIVATE_CONFIDENTIAL = "PRIVATE_CONFIDENTIAL"
    EXTERNAL_LLM_ALLOWED = "EXTERNAL_LLM_ALLOWED"


class RightsScope(str, Enum):
    PERSONAL_RESEARCH_ONLY = "PERSONAL_RESEARCH_ONLY"
    INTERNAL_RESEARCH = "INTERNAL_RESEARCH"
    EXTERNAL_PROCESSING_ALLOWED = "EXTERNAL_PROCESSING_ALLOWED"
    UNKNOWN = "UNKNOWN"


class ExternalContextMode(str, Enum):
    DENY = "DENY"
    ABSTRACTED_CLAIMS_ONLY = "ABSTRACTED_CLAIMS_ONLY"
    ALLOWLISTED_EXCERPTS = "ALLOWLISTED_EXCERPTS"


class ViewpointStatus(str, Enum):
    DRAFT = "DRAFT"
    APPROVED = "APPROVED"
    SUPERSEDED = "SUPERSEDED"
    RETRACTED = "RETRACTED"


def canonical_hash(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _nonempty(name: str, value: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{name} must not be empty")


def _unique_nonempty(name: str, values: tuple[str, ...]) -> None:
    if any(not value.strip() for value in values):
        raise ValueError(f"{name} values must not be empty")
    if len(values) != len(set(values)):
        raise ValueError(f"{name} values must be unique")


@dataclass(frozen=True, slots=True)
class MaterialManifest:
    """Rights and provenance metadata for a local source; never contains source text."""

    material_id: str
    title: str
    local_path: str
    source_hash: str
    source_label: str
    sensitivity: MaterialSensitivity
    rights_scope: RightsScope
    external_context_mode: ExternalContextMode
    max_external_chars: int
    redaction_required: bool
    owner: str
    as_of: datetime
    license_expires_on: date | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("material_id", "title", "local_path", "source_hash", "source_label", "owner"):
            _nonempty(name, getattr(self, name))
        if not _SHA256_RE.fullmatch(self.source_hash):
            raise ValueError("source_hash must be a lowercase SHA-256 hex digest")
        for name in ("as_of", "created_at", "updated_at"):
            _aware(name, getattr(self, name))
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must not be before created_at")
        if self.max_external_chars < 0:
            raise ValueError("max_external_chars must be >= 0")
        if self.external_context_mode is ExternalContextMode.DENY and self.max_external_chars != 0:
            raise ValueError("DENY mode requires max_external_chars=0")
        if self.external_context_mode is ExternalContextMode.ALLOWLISTED_EXCERPTS:
            if self.sensitivity is not MaterialSensitivity.EXTERNAL_LLM_ALLOWED:
                raise ValueError("verbatim excerpts require EXTERNAL_LLM_ALLOWED sensitivity")
            if self.rights_scope is not RightsScope.EXTERNAL_PROCESSING_ALLOWED:
                raise ValueError("verbatim excerpts require EXTERNAL_PROCESSING_ALLOWED rights")
            if self.max_external_chars < 1:
                raise ValueError("allowlisted excerpts require a positive character limit")

    @property
    def expired(self) -> bool:
        return self.license_expires_on is not None and self.license_expires_on < date.today()


@dataclass(frozen=True, slots=True)
class MacroViewpoint:
    """Human-reviewed, non-reconstructive claim derived locally from source material."""

    viewpoint_id: str
    material_id: str
    title: str
    topic: str
    claim: str
    horizon: str
    evidence_summary: tuple[str, ...]
    market_implications: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    confidence: float
    source_disclosure: str
    verbatim_text_included: bool
    status: ViewpointStatus
    approved_for_external: bool
    as_of: datetime
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "viewpoint_id", "material_id", "title", "topic", "claim", "horizon", "source_disclosure"
        ):
            _nonempty(name, getattr(self, name))
        for name in ("evidence_summary", "market_implications", "invalidation_conditions"):
            _unique_nonempty(name, getattr(self, name))
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        for name in ("as_of", "created_at", "updated_at"):
            _aware(name, getattr(self, name))
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must not be before created_at")
        if self.approved_for_external and self.status is not ViewpointStatus.APPROVED:
            raise ValueError("only APPROVED viewpoints can be approved for external processing")

    @property
    def content_hash(self) -> str:
        return canonical_hash({
            "topic": self.topic,
            "claim": self.claim,
            "horizon": self.horizon,
            "evidence_summary": self.evidence_summary,
            "market_implications": self.market_implications,
            "invalidation_conditions": self.invalidation_conditions,
            "confidence": self.confidence,
            "source_disclosure": self.source_disclosure,
        })


@dataclass(frozen=True, slots=True)
class ApprovedContext:
    context_id: str
    material_id: str
    viewpoint_id: str | None
    mode: ExternalContextMode
    text: str
    content_hash: str

    def __post_init__(self) -> None:
        for name in ("context_id", "material_id", "text"):
            _nonempty(name, getattr(self, name))
        if not _SHA256_RE.fullmatch(self.content_hash):
            raise ValueError("content_hash must be a lowercase SHA-256 hex digest")
        if hashlib.sha256(self.text.encode("utf-8")).hexdigest() != self.content_hash:
            raise ValueError("content_hash does not match approved context text")


@dataclass(frozen=True, slots=True)
class EgressDecision:
    decision_id: str
    allowed: bool
    mode: ExternalContextMode
    material_ids: tuple[str, ...]
    viewpoint_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]
    contexts: tuple[ApprovedContext, ...]
    total_characters: int
    context_hash: str
    decided_at: datetime

    def __post_init__(self) -> None:
        _nonempty("decision_id", self.decision_id)
        _unique_nonempty("material_ids", self.material_ids)
        _unique_nonempty("viewpoint_ids", self.viewpoint_ids)
        _unique_nonempty("reason_codes", self.reason_codes)
        _aware("decided_at", self.decided_at)
        if self.total_characters != sum(len(context.text) for context in self.contexts):
            raise ValueError("total_characters does not match contexts")
        if not _SHA256_RE.fullmatch(self.context_hash):
            raise ValueError("context_hash must be a lowercase SHA-256 hex digest")
        if not self.allowed and self.contexts:
            raise ValueError("blocked egress decision cannot contain contexts")
