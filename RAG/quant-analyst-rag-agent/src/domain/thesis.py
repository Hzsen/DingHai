from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum


class ThesisStatus(str, Enum):
    WATCHLIST = "WATCHLIST"
    THEME_WARMUP = "THEME_WARMUP"
    BREAKOUT_CANDIDATE = "BREAKOUT_CANDIDATE"
    MAIN_UPTREND_CONFIRMED = "MAIN_UPTREND_CONFIRMED"
    ACCELERATION = "ACCELERATION"
    DISTRIBUTION_RISK = "DISTRIBUTION_RISK"
    INVALIDATED = "INVALIDATED"
    COMPLETED = "COMPLETED"
    REACTIVATION_WATCH = "REACTIVATION_WATCH"


class ThesisType(str, Enum):
    INSTITUTIONAL_TREND = "INSTITUTIONAL_TREND"
    NARRATIVE_MOMENTUM = "NARRATIVE_MOMENTUM"
    PRICE_HIKE_MATERIAL = "PRICE_HIKE_MATERIAL"
    SEMICONDUCTOR_EQUIPMENT = "SEMICONDUCTOR_EQUIPMENT"
    AI_INFRASTRUCTURE_CHAIN = "AI_INFRASTRUCTURE_CHAIN"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class StockThesis:
    thesis_id: str
    ticker: str
    name: str
    theme: str
    thesis_type: ThesisType
    start_date: date
    end_date: date | None
    status: ThesisStatus
    key_factors: list[str]
    validation_signals: list[str]
    invalidation_signals: list[str]
    narrative_summary: str
    fundamental_logic: str
    capital_flow_logic: str
    risk_notes: str
    source_document_ids: list[str]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("thesis_id", "ticker", "name"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError("end_date must not be before start_date")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must not be before created_at")


EvidenceValue = float | int | str | bool | None


@dataclass(frozen=True, slots=True)
class ThesisValidationResult:
    ticker: str
    thesis_id: str
    previous_status: ThesisStatus
    new_status: ThesisStatus
    changed: bool
    reason_codes: list[str]
    numeric_evidence: dict[str, EvidenceValue]
    needs_llm_update: bool
    needs_research_note: bool

    def __post_init__(self) -> None:
        if self.changed != (self.previous_status != self.new_status):
            raise ValueError("changed must match the status transition")
        if self.needs_llm_update and not self.changed:
            raise ValueError("needs_llm_update requires a changed status")
