from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class MacroDocumentStatus(str, Enum):
    DRAFT_INTRADAY = "DRAFT_INTRADAY"
    FINALIZED_DAILY = "FINALIZED_DAILY"
    SUPERSEDED = "SUPERSEDED"
    EXPIRED = "EXPIRED"


class MacroRegime(str, Enum):
    LIQUIDITY_EASING = "LIQUIDITY_EASING"
    RISK_ON_CONFIRMING = "RISK_ON_CONFIRMING"
    NEUTRAL_TRANSITION = "NEUTRAL_TRANSITION"
    LONG_DURATION_PRESSURE = "LONG_DURATION_PRESSURE"
    CREDIT_TIGHTENING = "CREDIT_TIGHTENING"
    BROAD_RISK_OFF = "BROAD_RISK_OFF"
    SYSTEMIC_STRESS = "SYSTEMIC_STRESS"


class RiskState(str, Enum):
    CALM = "CALM"
    NORMAL = "NORMAL"
    ELEVATED = "ELEVATED"
    HIGH_RISK = "HIGH_RISK"
    STRESS = "STRESS"
    SYSTEMIC_STRESS = "SYSTEMIC_STRESS"


class LiquidityState(str, Enum):
    STRONGLY_EXPANDING = "STRONGLY_EXPANDING"
    EXPANDING = "EXPANDING"
    NEUTRAL = "NEUTRAL"
    CONTRACTING = "CONTRACTING"
    STRONGLY_CONTRACTING = "STRONGLY_CONTRACTING"


class InflationQuadrant(str, Enum):
    REAL_RATE_SHOCK = "REAL_RATE_SHOCK"
    MIXED_INFLATION_PRESSURE = "MIXED_INFLATION_PRESSURE"
    REFLATION = "REFLATION"
    DISINFLATION_OR_GROWTH_SCARE = "DISINFLATION_OR_GROWTH_SCARE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class RatePressureState(str, Enum):
    LOW = "LOW"
    NEUTRAL = "NEUTRAL"
    PRESSURE_BUILDING = "PRESSURE_BUILDING"
    SUSTAINED_PRESSURE = "SUSTAINED_PRESSURE"
    EXTREME_PRESSURE = "EXTREME_PRESSURE"


class Stance(str, Enum):
    STRONGLY_BULLISH = "STRONGLY_BULLISH"
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"
    BEARISH = "BEARISH"
    STRONGLY_BEARISH = "STRONGLY_BEARISH"


class StanceHorizon(str, Enum):
    TACTICAL = "TACTICAL_1_5D"
    SWING = "SWING_1_4W"
    REGIME = "REGIME_1_3M"


class LiquidityFlowState(str, Enum):
    STRONG_ABSORPTION = "STRONG_ABSORPTION"
    ABSORBING = "ABSORBING"
    MIXED = "MIXED"
    REJECTING = "REJECTING"
    STRONG_REJECTION = "STRONG_REJECTION"


@dataclass(frozen=True, slots=True)
class SeriesFeature:
    series_id: str
    as_of: datetime
    value: float
    unit: str
    source: str
    observation_date: str
    available_at: str
    is_realtime: bool
    stale_days: int
    delta_1d: float | None
    delta_5d: float | None
    delta_20d: float | None
    percentile_5y: float | None
    z_change_5d_252: float | None
    quality_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AssetStance:
    asset_id: str
    horizon: StanceHorizon
    stance: Stance
    direction_score: float
    confidence: float
    supporting_factors: tuple[str, ...]
    opposing_factors: tuple[str, ...]
    risk_triggers: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    as_of: datetime
    valid_until: datetime


@dataclass(frozen=True, slots=True)
class LiquiditySourceFlow:
    source_id: str
    flow_billions_usd_20d: float
    direction: str
    observation_date: str
    confidence: float


@dataclass(frozen=True, slots=True)
class LiquidityTargetFlow:
    target_id: str
    proxy_symbol: str
    state: LiquidityFlowState
    absorption_score: float
    liquidity_impulse_component: float
    market_confirmation_component: float
    structural_component: float
    confidence: float
    supporting_signals: tuple[str, ...]
    conflicting_signals: tuple[str, ...]
    measurement_note: str


@dataclass(frozen=True, slots=True)
class MacroSnapshot:
    snapshot_id: str
    as_of: datetime
    valid_until: datetime
    primary_regime: MacroRegime
    risk_state: RiskState
    risk_score: float
    liquidity_state: LiquidityState
    liquidity_score: float
    inflation_quadrant: InflationQuadrant
    rate_pressure_state: RatePressureState
    rate_pressure_score: float
    liquidity_source_flows: tuple[LiquiditySourceFlow, ...]
    liquidity_target_flows: tuple[LiquidityTargetFlow, ...]
    asset_stances: tuple[AssetStance, ...]
    main_drivers: tuple[str, ...]
    confirming_signals: tuple[str, ...]
    conflicting_signals: tuple[str, ...]
    quality_flags: tuple[str, ...]
    data_coverage: float
    confidence: float
    stale_series: tuple[str, ...]
    model_version: str


@dataclass(frozen=True, slots=True)
class MacroRiskDocument:
    document_id: str
    as_of: datetime
    valid_from: datetime
    valid_until: datetime
    status: MacroDocumentStatus
    primary_regime: MacroRegime
    risk_state: RiskState
    liquidity_state: LiquidityState
    inflation_quadrant: InflationQuadrant
    rate_pressure_state: RatePressureState
    liquidity_source_flows: tuple[LiquiditySourceFlow, ...]
    liquidity_target_flows: tuple[LiquidityTargetFlow, ...]
    asset_stances: tuple[AssetStance, ...]
    main_drivers: tuple[str, ...]
    confirming_signals: tuple[str, ...]
    conflicting_signals: tuple[str, ...]
    risk_triggers: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    data_coverage: float
    confidence: float
    stale_series: tuple[str, ...]
    source_observation_ids: tuple[str, ...]
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, str | float | int | bool | None] = field(default_factory=dict)
