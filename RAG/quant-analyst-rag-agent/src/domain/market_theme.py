from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class ThemeHorizon(str, Enum):
    FAST = "FAST_1_5D"
    REPRICING = "REPRICING_14D"


class ThemeFamily(str, Enum):
    RATES_INFLATION = "RATES_INFLATION"
    EASING_DEFENSIVE = "EASING_DEFENSIVE"
    STRESS_OVERRIDE = "STRESS_OVERRIDE"
    EQUITY_INTERNALS = "EQUITY_INTERNALS"
    DIVERGENCE = "DIVERGENCE"


@dataclass(frozen=True, slots=True)
class ThemeCandidate:
    theme_id: str
    label: str
    family: ThemeFamily
    horizon: ThemeHorizon
    priority: int
    confidence: float
    activation_strength: float
    confirmation_count: int
    confirmation_total: int
    persistence_periods: int
    data_coverage: float
    supporting_evidence: tuple[str, ...]
    conflicting_evidence: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    summary: str


@dataclass(frozen=True, slots=True)
class MarketThemeState:
    as_of: datetime
    valid_until: datetime
    horizon: ThemeHorizon
    dominant_theme_id: str | None
    dominant_label: str
    summary: str
    confidence: float
    active_themes: tuple[ThemeCandidate, ...]
    strongest_signals: tuple[str, ...]
    no_dominant_reason: str | None
    model_version: str
