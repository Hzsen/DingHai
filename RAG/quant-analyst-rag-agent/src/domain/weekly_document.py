from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum


class WeeklyDocumentStatus(str, Enum):
    DRAFT = "DRAFT"
    FINALIZED = "FINALIZED"


class WeeklyChunkType(str, Enum):
    DRAFT_SUMMARY = "DRAFT_SUMMARY"
    STATE_CHANGE = "STATE_CHANGE"
    WEEKLY_SUMMARY = "WEEKLY_SUMMARY"


@dataclass(frozen=True, slots=True)
class WeeklyResearchDocument:
    document_id: str
    ticker: str
    name: str
    week_start: date
    week_end: date
    as_of: datetime
    status: WeeklyDocumentStatus
    version: int
    opening_state: str
    closing_state: str
    daily_observation_ids: tuple[str, ...]
    state_change_ids: tuple[str, ...]
    metrics: dict[str, float | int | str | bool | None]
    content: str
    source_hash: str
    source_run_id: str
    llm_update_required: bool


@dataclass(frozen=True, slots=True)
class WeeklyKnowledgeChunk:
    chunk_id: str
    document_id: str
    chunk_type: WeeklyChunkType
    event_date: date | None
    content: str
    content_hash: str
    indexable: bool
