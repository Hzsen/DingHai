from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class MacroHistoryPoint:
    as_of: datetime
    snapshot_id: str
    model_version: str
    net_liquidity_20d_bn: float
    liquidity_score: float
    risk_score: float
    rate_pressure_score: float
    confidence: float
    source_flows_bn: dict[str, float]
    target_absorption: dict[str, float]
    target_states: dict[str, str]


@dataclass(frozen=True, slots=True)
class MacroChangeEvent:
    event_id: str
    as_of: datetime
    window_start: datetime
    window_days: int
    event_type: str
    entity_id: str
    previous_value: float | str | bool | None
    current_value: float | str | bool | None
    magnitude: float | None
    direction: str
    reason_codes: tuple[str, ...]
    needs_kimi_analysis: bool


@dataclass(frozen=True, slots=True)
class MacroAnalysisPacket:
    packet_id: str
    as_of: datetime
    window_start: datetime
    window_days: int
    model_version: str
    current_snapshot_id: str
    current_state: dict[str, object]
    window_change: dict[str, object]
    daily_history: tuple[dict[str, object], ...]
    change_events: tuple[dict[str, object], ...]
    candidate_risk_types: tuple[str, ...]
    data_quality: dict[str, object]

