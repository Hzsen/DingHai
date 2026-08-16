from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class ThemeInstrumentKind(str, Enum):
    ETF = "ETF"
    EQUAL_WEIGHT_BASKET = "EQUAL_WEIGHT_BASKET"


class ThemeTrend(str, Enum):
    STRONG = "STRONG"
    LEAN_STRONG = "LEAN_STRONG"
    NEUTRAL = "NEUTRAL"
    LEAN_WEAK = "LEAN_WEAK"
    WEAK = "WEAK"


class ThemeRotationState(str, Enum):
    CROWDED_UPTREND = "CROWDED_UPTREND"
    CONFIRMED_ENTRY = "CONFIRMED_ENTRY"
    EARLY_ROTATION = "EARLY_ROTATION"
    NEUTRAL_WATCH = "NEUTRAL_WATCH"
    CAPITAL_EXIT = "CAPITAL_EXIT"
    DISTRIBUTION_EXIT = "DISTRIBUTION_EXIT"
    NO_DATA = "NO_DATA"


class ThemeReason(str, Enum):
    ACTIVE_INFLOW = "ACTIVE_INFLOW"
    EARLY_ROTATION = "EARLY_ROTATION"
    SOFTWARE_SEMICONDUCTOR_ROTATION = "SOFTWARE_SEMICONDUCTOR_ROTATION"
    AI_CHAIN_DIFFUSION = "AI_CHAIN_DIFFUSION"
    HYPERSCALER_HANDOFF = "HYPERSCALER_HANDOFF"
    CROWDED_UPTREND = "CROWDED_UPTREND"
    CROWDING_UNWIND = "CROWDING_UNWIND"
    DISTRIBUTION_EXIT = "DISTRIBUTION_EXIT"
    BETA_LIFT_FALSE_STRENGTH = "BETA_LIFT_FALSE_STRENGTH"
    RELATIVE_RESILIENCE = "RELATIVE_RESILIENCE"
    INTERNAL_DE_RISKING = "INTERNAL_DE_RISKING"
    MONTH_QUARTER_REBALANCE = "MONTH_QUARTER_REBALANCE"
    AI_APPLICATION_CATCHUP = "AI_APPLICATION_CATCHUP"
    NEUTRAL = "NEUTRAL"
    NO_DATA = "NO_DATA"


class RotationAlertDirection(str, Enum):
    CROSS_ABOVE = "CROSS_ABOVE"
    CROSS_BELOW = "CROSS_BELOW"


@dataclass(frozen=True, slots=True)
class ThemeDefinition:
    theme_id: str
    label: str
    kind: ThemeInstrumentKind
    proxy_symbol: str
    members: tuple[str, ...]
    description: str

    def __post_init__(self) -> None:
        if not self.theme_id.strip() or not self.label.strip() or not self.proxy_symbol.strip():
            raise ValueError("theme identity, label and proxy_symbol must not be empty")
        if not self.members or any(not member.strip() for member in self.members):
            raise ValueError("theme members must contain non-empty symbols")
        if len(self.members) != len(set(self.members)):
            raise ValueError("theme members must be unique")
        if self.kind is ThemeInstrumentKind.ETF and self.members != (self.proxy_symbol,):
            raise ValueError("ETF themes must use the proxy as their single member")


@dataclass(frozen=True, slots=True)
class ThemeRotationConfig:
    benchmark_symbol: str = "QQQ"
    calculation_timeframe: str = "1D"
    lookback_bars: int = 350
    trend_length: int = 20
    slow_length: int = 50
    volume_length: int = 20
    volume_confirmation: float = 1.30
    crowded_relative_60d: float = 18.0
    crowded_distance_50d: float = 8.0
    early_rotation_threshold: float = 60.0
    confirmed_entry_threshold: float = 75.0
    weakening_threshold: float = 45.0
    model_version: str = "tech-theme-rotation-v2.6.1r-python.1"
    reference_model_version: str = "pine-v2.6.1r"

    def __post_init__(self) -> None:
        if not self.benchmark_symbol.strip():
            raise ValueError("benchmark_symbol must not be empty")
        if self.lookback_bars < 120:
            raise ValueError("lookback_bars must be at least 120")
        if not 5 <= self.trend_length < self.slow_length:
            raise ValueError("trend_length must be >= 5 and below slow_length")
        if self.volume_length < 5 or self.volume_confirmation < 1:
            raise ValueError("volume settings are invalid")
        if not 0 <= self.weakening_threshold < self.early_rotation_threshold < self.confirmed_entry_threshold <= 100:
            raise ValueError("score thresholds must be ordered inside 0..100")


@dataclass(frozen=True, slots=True)
class ThemeMetricSnapshot:
    theme_id: str
    label: str
    proxy_symbol: str
    kind: ThemeInstrumentKind
    members: tuple[str, ...]
    relative_1d: float | None
    relative_5d: float | None
    relative_20d: float | None
    relative_60d: float | None
    absolute_5d: float | None
    acceleration: float | None
    volume_ratio: float | None
    breadth: float | None
    distance_from_relative_ma50: float | None
    trend: ThemeTrend
    base_score: float | None
    score: float | None
    state: ThemeRotationState
    reason: ThemeReason
    overextended: bool
    down_volume_pressure: bool
    beta_lift: bool
    distribution: bool
    live_members: int
    member_count: int
    data_coverage: float
    score_components: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AttributionSignal:
    attribution_id: str
    label: str
    score: float
    core_signal: str
    invalidation: str
    attention_threshold: float = 60.0


@dataclass(frozen=True, slots=True)
class RotationAlert:
    alert_id: str
    label: str
    direction: RotationAlertDirection
    threshold: float
    previous_value: float
    current_value: float


@dataclass(frozen=True, slots=True)
class ThemeRotationSnapshot:
    snapshot_id: str
    as_of: datetime
    valid_until: datetime
    benchmark_symbol: str
    selected_theme_id: str
    themes: tuple[ThemeMetricSnapshot, ...]
    attributions: tuple[AttributionSignal, ...]
    alerts: tuple[RotationAlert, ...]
    data_coverage: float
    quality_flags: tuple[str, ...]
    model_version: str
    reference_model_version: str

    def __post_init__(self) -> None:
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if self.valid_until.tzinfo is None or self.valid_until.utcoffset() is None:
            raise ValueError("valid_until must be timezone-aware")
        if self.valid_until <= self.as_of:
            raise ValueError("valid_until must be after as_of")
        if not 0 <= self.data_coverage <= 1:
            raise ValueError("data_coverage must be within 0..1")
        theme_ids = tuple(item.theme_id for item in self.themes)
        if len(theme_ids) != len(set(theme_ids)):
            raise ValueError("snapshot theme ids must be unique")
        if self.selected_theme_id not in theme_ids:
            raise ValueError("selected_theme_id must exist in themes")
