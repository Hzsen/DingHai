"""Typed contract for the unified ``quant-agent daily-run`` dashboard snapshot.

The daily run aggregates existing research modules (macro regime, technology
theme rotation, China A-share WaveScore / selloff-repair screens, and the
canonical RAG / lexical / vector index health) into a single immutable
snapshot.  The snapshot is a *view model*: it never recomputes research
results, it only carries the fields the dashboard is allowed to display.

State values that originate from upstream enums (macro regime, theme state,
repair stage, ...) are stored as plain strings so this contract stays
backward compatible when upstream enums evolve.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class DailyRunMode(str, Enum):
    FIXTURE = "fixture"
    LIVE = "live"


class ModuleStatus(str, Enum):
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class FreshnessState(str, Enum):
    FRESH = "FRESH"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


class AttentionSeverity(str, Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


class ArtifactKind(str, Enum):
    HTML = "HTML"
    MARKDOWN = "MARKDOWN"
    JSON = "JSON"
    CSV = "CSV"
    DATABASE = "DATABASE"
    OTHER = "OTHER"


@dataclass(frozen=True, slots=True)
class RunArtifact:
    """A clickable, local, relative link to a file produced by the run."""

    label: str
    path: str
    kind: ArtifactKind = ArtifactKind.OTHER

    def __post_init__(self) -> None:
        if not self.label.strip():
            raise ValueError("artifact label must not be empty")
        if not self.path.strip():
            raise ValueError("artifact path must not be empty")
        if self.path.startswith(("/", "http://", "https://")) or "://" in self.path:
            raise ValueError("artifact path must be a local relative path")


@dataclass(frozen=True, slots=True)
class Freshness:
    state: FreshnessState
    data_as_of: str | None = None
    stale_days: int | None = None

    def __post_init__(self) -> None:
        if self.stale_days is not None and self.stale_days < 0:
            raise ValueError("stale_days must be >= 0")


@dataclass(frozen=True, slots=True)
class ModuleResult:
    module_id: str
    label: str
    status: ModuleStatus
    summary: str = ""
    freshness: Freshness = Freshness(FreshnessState.UNKNOWN)
    coverage: float | None = None
    cache_used: bool | None = None
    model_version: str | None = None
    rule_version: str | None = None
    duration_ms: int | None = None
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    artifacts: tuple[RunArtifact, ...] = ()
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.module_id.strip() or not self.label.strip():
            raise ValueError("module identity and label must not be empty")
        if self.coverage is not None and not 0 <= self.coverage <= 1:
            raise ValueError("coverage must be within 0..1")
        if self.duration_ms is not None and self.duration_ms < 0:
            raise ValueError("duration_ms must be >= 0")


@dataclass(frozen=True, slots=True)
class CrossAssetFlow:
    target_id: str
    proxy_symbol: str
    state: str
    absorption_score: float | None = None


@dataclass(frozen=True, slots=True)
class MacroOverview:
    primary_regime: str | None = None
    risk_state: str | None = None
    risk_score: float | None = None
    liquidity_state: str | None = None
    liquidity_score: float | None = None
    rate_pressure_state: str | None = None
    rate_pressure_score: float | None = None
    inflation_quadrant: str | None = None
    data_coverage: float | None = None
    confidence: float | None = None
    stale_series: tuple[str, ...] = ()
    target_flows: tuple[CrossAssetFlow, ...] = ()
    model_version: str | None = None

    def __post_init__(self) -> None:
        for name, value in (("data_coverage", self.data_coverage), ("confidence", self.confidence)):
            if value is not None and not 0 <= value <= 1:
                raise ValueError(f"{name} must be within 0..1")


@dataclass(frozen=True, slots=True)
class ThemeRow:
    theme_id: str
    label: str
    proxy_symbol: str
    score: float | None
    state: str
    trend: str | None = None
    coverage: float | None = None
    relative_5d: float | None = None
    relative_20d: float | None = None
    selected: bool = False


@dataclass(frozen=True, slots=True)
class ThemeAlert:
    label: str
    direction: str
    threshold: float
    previous_value: float
    current_value: float


@dataclass(frozen=True, slots=True)
class AttributionRow:
    label: str
    score: float
    core_signal: str
    invalidation: str


@dataclass(frozen=True, slots=True)
class ThemeOverview:
    benchmark_symbol: str
    selected_theme_id: str | None
    themes: tuple[ThemeRow, ...] = ()
    attributions: tuple[AttributionRow, ...] = ()
    alerts: tuple[ThemeAlert, ...] = ()
    data_coverage: float | None = None
    model_version: str | None = None


@dataclass(frozen=True, slots=True)
class WaveCandidate:
    ticker: str
    name: str
    leader_score: int | None
    coverage: float | None = None
    stage_label: str | None = None
    top_reasons: tuple[str, ...] = ()
    risk_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RepairCandidate:
    ticker: str
    name: str
    reversal_score: float | None
    stage: str | None = None
    focus_selected: bool = False
    top_reasons: tuple[str, ...] = ()
    risk_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ChinaAShareOverview:
    wave_candidates: tuple[WaveCandidate, ...] = ()
    repair_candidates: tuple[RepairCandidate, ...] = ()
    universe_note: str = ""
    wave_model_version: str | None = None
    repair_model_version: str | None = None


@dataclass(frozen=True, slots=True)
class IndexHealth:
    canonical_chunks: int | None = None
    lexical_chunks: int | None = None
    vector_chunks: int | None = None
    lexical_in_parity: bool | None = None
    vector_in_parity: bool | None = None
    indexes_in_parity: bool | None = None
    outbox_pending: int | None = None
    outbox_running: int | None = None
    outbox_completed: int | None = None
    outbox_failed: int | None = None
    lexical_index_version: str | None = None
    vector_index_version: str | None = None


@dataclass(frozen=True, slots=True)
class AttentionItem:
    severity: AttentionSeverity
    module_id: str
    title: str
    impact: str
    recommended_action: str

    def __post_init__(self) -> None:
        if not self.title.strip():
            raise ValueError("attention title must not be empty")


@dataclass(frozen=True, slots=True)
class DailyRunSnapshot:
    run_id: str
    as_of: datetime
    generated_at: datetime
    mode: DailyRunMode
    synthetic: bool
    overall_status: ModuleStatus
    modules: tuple[ModuleResult, ...]
    macro: MacroOverview | None
    themes: ThemeOverview | None
    china_a_share: ChinaAShareOverview | None
    index_health: IndexHealth | None
    attention: tuple[AttentionItem, ...] = ()
    artifacts: tuple[RunArtifact, ...] = ()
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        for name, value in (("as_of", self.as_of), ("generated_at", self.generated_at)):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        module_ids = tuple(item.module_id for item in self.modules)
        if len(module_ids) != len(set(module_ids)):
            raise ValueError("module ids must be unique")
