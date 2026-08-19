"""Orchestration for the unified daily run.

Two modes:

- ``fixture``: a deterministic, fully synthetic snapshot that exercises every
  dashboard branch (success / warning / failure / skipped, stale series,
  rotation alerts in both directions, index parity mismatch).  Clearly marked
  ``synthetic`` so it can never be confused with real market state.
- ``live``: aggregates what is actually available offline — the canonical /
  lexical / vector index health is read from the real database, and macro /
  theme sections are parsed from the latest published module outputs when
  present.  Modules without available inputs are marked ``SKIPPED`` instead of
  being fabricated.

The service never recomputes research results; it only assembles view models.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from domain.daily_run import (
    AttentionItem,
    AttentionSeverity,
    AttributionRow,
    ChinaAShareOverview,
    CrossAssetFlow,
    DailyRunMode,
    DailyRunSnapshot,
    Freshness,
    FreshnessState,
    IndexHealth,
    MacroOverview,
    ModuleResult,
    ModuleStatus,
    RepairCandidate,
    RunArtifact,
    ArtifactKind,
    ThemeAlert,
    ThemeOverview,
    ThemeRow,
    WaveCandidate,
)
from quant_agent.config import Paths
from quant_agent.daily_run.repository import DailyRunRepository

LOW_COVERAGE_THRESHOLD = 0.70
STALE_DAYS_THRESHOLD = 2

PILOT_UNIVERSE_NOTE = "当前 live universe 仅为试点样本（pilot universe），不代表全市场覆盖。"


@dataclass(frozen=True)
class DailyRunResult:
    snapshot: DailyRunSnapshot
    outputs: dict[str, Path]


# --------------------------------------------------------------------------- #
# shared derivation helpers
# --------------------------------------------------------------------------- #

_SEVERITY_ORDER = {
    AttentionSeverity.CRITICAL: 0,
    AttentionSeverity.WARNING: 1,
    AttentionSeverity.INFO: 2,
}


def _freshness_for(data_as_of: datetime | None, run_as_of: datetime) -> Freshness:
    if data_as_of is None:
        return Freshness(FreshnessState.UNKNOWN)
    stale_days = max(0, (run_as_of.date() - data_as_of.date()).days)
    state = FreshnessState.STALE if stale_days > STALE_DAYS_THRESHOLD else FreshnessState.FRESH
    return Freshness(state, data_as_of=data_as_of.isoformat(), stale_days=stale_days)


def _derive_attention(
    modules: tuple[ModuleResult, ...],
    themes: ThemeOverview | None,
    index_health: IndexHealth | None,
) -> tuple[AttentionItem, ...]:
    items: list[AttentionItem] = []
    for module in modules:
        if module.status is ModuleStatus.FAILED:
            items.append(
                AttentionItem(
                    severity=AttentionSeverity.CRITICAL,
                    module_id=module.module_id,
                    title=f"{module.label} 运行失败",
                    impact="；".join(module.errors) or "模块未产出结果",
                    recommended_action="查看模块详情中的错误信息，修复后重新运行 daily-run。",
                )
            )
        elif module.status is ModuleStatus.WARNING:
            items.append(
                AttentionItem(
                    severity=AttentionSeverity.WARNING,
                    module_id=module.module_id,
                    title=f"{module.label} 存在警告",
                    impact="；".join(module.warnings) or module.summary or "模块降级运行",
                    recommended_action="确认警告是否可接受；必要时重新运行对应模块。",
                )
            )
        if module.freshness.state is FreshnessState.STALE:
            items.append(
                AttentionItem(
                    severity=AttentionSeverity.WARNING,
                    module_id=module.module_id,
                    title=f"{module.label} 数据陈旧",
                    impact=f"数据滞后 {module.freshness.stale_days} 天（阈值 {STALE_DAYS_THRESHOLD} 天），结论可能不再反映最新市场状态。",
                    recommended_action="刷新上游数据后重新运行该模块。",
                )
            )
        if module.coverage is not None and module.coverage < LOW_COVERAGE_THRESHOLD:
            coverage_already_flagged = module.status is ModuleStatus.WARNING and any(
                "覆盖" in warning or "coverage" in warning.lower() for warning in module.warnings
            )
            if not coverage_already_flagged:
                items.append(
                    AttentionItem(
                        severity=AttentionSeverity.WARNING,
                        module_id=module.module_id,
                        title=f"{module.label} 覆盖率偏低",
                        impact=f"覆盖率 {module.coverage:.0%} 低于阈值 {LOW_COVERAGE_THRESHOLD:.0%}，评分可信度下降。",
                        recommended_action="检查上游数据缺口，补齐后重新运行。",
                    )
                )
    if index_health is not None:
        if index_health.indexes_in_parity is False:
            items.append(
                AttentionItem(
                    severity=AttentionSeverity.CRITICAL,
                    module_id="rag-index",
                    title="canonical / lexical / vector 索引不一致",
                    impact=(
                        f"canonical={index_health.canonical_chunks}, lexical={index_health.lexical_chunks}, "
                        f"vector={index_health.vector_chunks}；检索结果可能遗漏文档。"
                    ),
                    recommended_action="运行 `quant-agent index sync` 消费 outbox，使三类索引恢复一致。",
                )
            )
        if (index_health.outbox_failed or 0) > 0:
            items.append(
                AttentionItem(
                    severity=AttentionSeverity.WARNING,
                    module_id="rag-index",
                    title=f"索引 outbox 有 {index_health.outbox_failed} 个失败任务",
                    impact="失败任务对应的 chunk 未进入索引。",
                    recommended_action="排查失败原因后重新运行 `quant-agent index sync`。",
                )
            )
    if themes is not None:
        for alert in themes.alerts:
            direction = "上穿" if alert.direction == "CROSS_ABOVE" else "下穿"
            items.append(
                AttentionItem(
                    severity=AttentionSeverity.INFO,
                    module_id="theme-rotation",
                    title=f"主题告警：{alert.label}",
                    impact=f"{direction}阈值 {alert.threshold:.0f}：{alert.previous_value:.1f} → {alert.current_value:.1f}",
                    recommended_action="在主题轮动区查看该主题的相对强度与归因。",
                )
            )
    items.sort(key=lambda item: (_SEVERITY_ORDER[item.severity], item.module_id, item.title))
    return tuple(items)


def _derive_overall_status(
    modules: tuple[ModuleResult, ...], attention: tuple[AttentionItem, ...]
) -> ModuleStatus:
    if any(module.status is ModuleStatus.FAILED for module in modules) or any(
        item.severity is AttentionSeverity.CRITICAL for item in attention
    ):
        return ModuleStatus.FAILED
    if any(module.status is ModuleStatus.WARNING for module in modules) or any(
        item.severity is AttentionSeverity.WARNING for item in attention
    ):
        return ModuleStatus.WARNING
    return ModuleStatus.SUCCESS


# --------------------------------------------------------------------------- #
# fixture mode
# --------------------------------------------------------------------------- #

def _fixture_sections(as_of: datetime) -> tuple[
    tuple[ModuleResult, ...], MacroOverview, ThemeOverview, ChinaAShareOverview, IndexHealth
]:
    macro = MacroOverview(
        primary_regime="NEUTRAL_TRANSITION",
        risk_state="RISK_ON",
        risk_score=0.58,
        liquidity_state="MODERATELY_EXPANDING",
        liquidity_score=0.64,
        rate_pressure_state="ELEVATED",
        rate_pressure_score=-0.31,
        inflation_quadrant="DISINFLATION",
        data_coverage=0.92,
        confidence=0.71,
        stale_series=("DGS2", "WALCL"),
        target_flows=(
            CrossAssetFlow("global_equities", "ACWI", "STRONG_ABSORPTION", 0.62),
            CrossAssetFlow("us_credit", "HYG", "ABSORBING", 0.28),
            CrossAssetFlow("long_duration_rates", "TLT", "REJECTING", -0.44),
            CrossAssetFlow("usd_liquidity", "DXY", "MIXED", 0.05),
        ),
        model_version="macro-regime-v1.4.0",
    )
    themes = ThemeOverview(
        benchmark_symbol="QQQ",
        selected_theme_id="semiconductor",
        themes=(
            ThemeRow("semiconductor", "半导体", "SMH", 82.4, "CONFIRMED_ENTRY", "STRONG", 1.0, 3.8, 11.2, selected=True),
            ThemeRow("ai_application", "AI应用", "AI-BASKET", 66.1, "EARLY_ROTATION", "LEAN_STRONG", 0.96, 2.1, 5.4),
            ThemeRow("cloud_infra", "云计算基础设施", "SKYY", 58.7, "NEUTRAL_WATCH", "NEUTRAL", 0.94, 0.6, 1.9),
            ThemeRow("consumer_electronics", "消费电子", "CE-BASKET", 47.9, "NEUTRAL_WATCH", "LEAN_WEAK", 0.91, -0.8, -1.6),
            ThemeRow("software", "软件/SaaS", "IGV", 41.3, "CAPITAL_EXIT", "WEAK", 0.97, -2.4, -6.8),
            ThemeRow("communication_equipment", "通信设备", "COMM-BASKET", 35.6, "DISTRIBUTION_EXIT", "WEAK", 0.89, -3.1, -9.4),
        ),
        attributions=(
            AttributionRow("AI链条扩散", 74.0, "算力→应用→终端的相对强度依次确认", "半导体 20D 相对强度跌破 +4%"),
            AttributionRow("强弱对调", 61.5, "软件相对半导体连续 10 日走弱", "软件 5D 相对强度转正并持续 3 日"),
            AttributionRow("拥挤出清", 55.2, "通信设备高位放量滞涨", "成交额占比回落至 20 日均值下方"),
        ),
        alerts=(
            ThemeAlert("半导体", "CROSS_ABOVE", 75.0, 72.8, 82.4),
            ThemeAlert("软件/SaaS", "CROSS_BELOW", 45.0, 47.5, 41.3),
        ),
        data_coverage=0.97,
        model_version="tech-theme-rotation-v2.6.1r-python.1",
    )
    china = ChinaAShareOverview(
        wave_candidates=(
            WaveCandidate("300308", "中际旭创", 9, 0.91, "confirmed_main_uptrend", ("放量突破平台", "相对强度前5%"), ("高位换手放大",)),
            WaveCandidate("002475", "立讯精密", 8, 0.88, "momentum_setup", ("缩量回踩确认", "板块共振"), ()),
            WaveCandidate("601138", "工业富联", 7, 0.84, "breakout_candidate", ("筹码集中", "创新高"), ("波动率抬升",)),
        ),
        repair_candidates=(
            RepairCandidate("300750", "宁德时代", 78.5, "LEADER_REPAIR_CONFIRMED", True, ("急跌后放量收复", "龙头属性保持"), ("套牢盘密集",)),
            RepairCandidate("688981", "中芯国际", 64.2, "REPAIR_CANDIDATE", False, ("抗跌性强于指数",), ("量能尚未确认",)),
        ),
        universe_note=PILOT_UNIVERSE_NOTE,
        wave_model_version="cn-wave-market-behavior-v0.2.0",
        repair_model_version="cn-reversal-screen-v0.3.1",
    )
    index_health = IndexHealth(
        canonical_chunks=1248,
        lexical_chunks=1248,
        vector_chunks=1246,
        lexical_in_parity=True,
        vector_in_parity=False,
        indexes_in_parity=False,
        outbox_pending=1,
        outbox_running=0,
        outbox_completed=412,
        outbox_failed=2,
        lexical_index_version="canonical-lexical-v3",
        vector_index_version="canonical-vector-v2",
    )
    fresh = Freshness(FreshnessState.FRESH, data_as_of=as_of.isoformat(), stale_days=0)
    modules = (
        ModuleResult(
            module_id="macro-regime",
            label="宏观体制",
            status=ModuleStatus.SUCCESS,
            summary="NEUTRAL_TRANSITION / 流动性温和扩张",
            freshness=fresh,
            coverage=0.92,
            cache_used=True,
            model_version="macro-regime-v1.4.0",
            rule_version="macro-rules-v1.4",
            duration_ms=1840,
            warnings=("2 条序列陈旧：DGS2, WALCL",),
            metadata=(("数据源", "fixture feature set"), ("最低覆盖阈值", "0.50")),
        ),
        ModuleResult(
            module_id="theme-rotation",
            label="科技主题轮动",
            status=ModuleStatus.SUCCESS,
            summary="半导体确认进入；2 条阈值穿越告警",
            freshness=fresh,
            coverage=0.97,
            cache_used=False,
            model_version="tech-theme-rotation-v2.6.1r-python.1",
            rule_version="pine-v2.6.1r",
            duration_ms=2310,
            metadata=(("基准", "QQQ"), ("主题数", "6")),
        ),
        ModuleResult(
            module_id="cn-wave-screen",
            label="A股 WaveScore",
            status=ModuleStatus.SUCCESS,
            summary="3 只候选进入关注列表",
            freshness=fresh,
            coverage=0.88,
            cache_used=True,
            model_version="cn-wave-market-behavior-v0.2.0",
            duration_ms=3120,
            metadata=(("样本池", "pilot universe"),),
        ),
        ModuleResult(
            module_id="reversal-screen",
            label="A股 Selloff Repair",
            status=ModuleStatus.WARNING,
            summary="覆盖率低于阈值，候选仅供参考",
            freshness=fresh,
            coverage=0.61,
            cache_used=True,
            model_version="cn-reversal-screen-v0.3.1",
            duration_ms=2875,
            warnings=("覆盖率 61% 低于阈值 70%",),
            metadata=(("样本池", "pilot universe"),),
        ),
        ModuleResult(
            module_id="private-materials",
            label="私有材料情报",
            status=ModuleStatus.SKIPPED,
            summary="今日无新增私有材料输入，按调度跳过",
            freshness=Freshness(FreshnessState.UNKNOWN),
            cache_used=None,
            duration_ms=12,
        ),
        ModuleResult(
            module_id="rag-index",
            label="RAG 索引同步",
            status=ModuleStatus.FAILED,
            summary="vector 索引与 canonical 不一致",
            freshness=Freshness(FreshnessState.UNKNOWN),
            cache_used=None,
            duration_ms=940,
            errors=("vector index drift: 2 chunks missing relative to canonical manifest",),
            metadata=(("outbox_failed", "2"),),
        ),
    )
    return modules, macro, themes, china, index_health


# --------------------------------------------------------------------------- #
# live mode
# --------------------------------------------------------------------------- #

def _load_latest_json(directory: Path, pattern: str) -> tuple[Path, dict[str, Any]] | None:
    if not directory.is_dir():
        return None
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        return None
    path = candidates[-1]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return path, payload if isinstance(payload, dict) else {}


def _relative_artifact(
    path: Path, output_dir: Path, label: str, kind: ArtifactKind, project_root: Path
) -> RunArtifact | None:
    if not path.is_file():
        return None
    import os

    root = Path(project_root).resolve()
    if not (
        path.resolve().is_relative_to(root) and output_dir.resolve().is_relative_to(root)
    ):
        # A link outside the project tree would embed private absolute path
        # segments (home dir) in the page — skip it; the module metadata
        # already names the source file.
        return None
    return RunArtifact(label=label, path=os.path.relpath(path, output_dir), kind=kind)


def _live_index_health(db_path: Path) -> tuple[IndexHealth, ModuleResult]:
    started = time.monotonic()
    from quant_agent.knowledge.store import KnowledgeStore
    from quant_agent.retrieval.canonical_vector import VECTOR_INDEX_VERSION, CanonicalVectorIndex
    from quant_agent.retrieval.lexical import INDEX_VERSION, CanonicalLexicalIndex

    store = KnowledgeStore(db_path)
    canonical = store.current_index_manifest()
    lexical = CanonicalLexicalIndex(db_path).manifest()
    vector = CanonicalVectorIndex(db_path).manifest()
    outbox = {str(key).lower(): value for key, value in store.index_job_counts().items()}
    health = IndexHealth(
        canonical_chunks=len(canonical),
        lexical_chunks=len(lexical),
        vector_chunks=len(vector),
        lexical_in_parity=canonical == lexical,
        vector_in_parity=canonical == vector,
        indexes_in_parity=canonical == lexical == vector,
        outbox_pending=outbox.get("pending"),
        outbox_running=outbox.get("running"),
        outbox_completed=outbox.get("completed"),
        outbox_failed=outbox.get("failed"),
        lexical_index_version=INDEX_VERSION,
        vector_index_version=VECTOR_INDEX_VERSION,
    )
    failed = outbox.get("failed") or 0
    if health.indexes_in_parity is False:
        status = ModuleStatus.FAILED
        errors = ("canonical / lexical / vector manifest 不一致",)
        warnings: tuple[str, ...] = ()
    elif failed:
        status = ModuleStatus.WARNING
        errors = ()
        warnings = (f"outbox 有 {failed} 个失败任务",)
    else:
        status = ModuleStatus.SUCCESS
        errors = ()
        warnings = ()
    module = ModuleResult(
        module_id="rag-index",
        label="RAG 索引同步",
        status=status,
        summary=(
            f"canonical={health.canonical_chunks} / lexical={health.lexical_chunks} / vector={health.vector_chunks}"
        ),
        freshness=Freshness(FreshnessState.UNKNOWN),
        duration_ms=int((time.monotonic() - started) * 1000),
        warnings=warnings,
        errors=errors,
        metadata=(("数据库", db_path.name),),
    )
    return health, module


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _float_or_none(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _live_macro(paths: Paths, output_dir: Path, as_of: datetime) -> tuple[MacroOverview | None, ModuleResult]:
    started = time.monotonic()
    found = _load_latest_json(paths.project_root / "outputs" / "macro", "macro_snapshot_*.json")
    if found is None:
        return None, ModuleResult(
            module_id="macro-regime",
            label="宏观体制",
            status=ModuleStatus.SKIPPED,
            summary="未找到已发布的宏观快照（outputs/macro/macro_snapshot_*.json）",
            freshness=Freshness(FreshnessState.UNKNOWN),
            duration_ms=int((time.monotonic() - started) * 1000),
            metadata=(("建议", "先运行 run_macro_regime 生成宏观快照"),),
        )
    path, payload = found
    if isinstance(payload.get("snapshot"), dict):
        payload = payload["snapshot"]  # published files wrap the MacroSnapshot
    snapshot_as_of = _parse_datetime(payload.get("as_of"))
    flows = tuple(
        CrossAssetFlow(
            target_id=str(item.get("target_id", "")),
            proxy_symbol=str(item.get("proxy_symbol", "")),
            state=str(item.get("state", "")),
            absorption_score=_float_or_none(item.get("absorption_score")),
        )
        for item in payload.get("liquidity_target_flows") or []
        if isinstance(item, dict)
    )
    overview = MacroOverview(
        primary_regime=payload.get("primary_regime"),
        risk_state=payload.get("risk_state"),
        risk_score=_float_or_none(payload.get("risk_score")),
        liquidity_state=payload.get("liquidity_state"),
        liquidity_score=_float_or_none(payload.get("liquidity_score")),
        rate_pressure_state=payload.get("rate_pressure_state"),
        rate_pressure_score=_float_or_none(payload.get("rate_pressure_score")),
        inflation_quadrant=payload.get("inflation_quadrant"),
        data_coverage=_float_or_none(payload.get("data_coverage")),
        confidence=_float_or_none(payload.get("confidence")),
        stale_series=tuple(str(item) for item in payload.get("stale_series") or ()),
        target_flows=flows,
        model_version=payload.get("model_version"),
    )
    suffix = snapshot_as_of.date().isoformat() if snapshot_as_of else path.stem.removeprefix("macro_snapshot_")
    artifacts = tuple(
        artifact
        for artifact in (
            _relative_artifact(path, output_dir, "宏观快照 JSON", ArtifactKind.JSON, paths.project_root),
            _relative_artifact(path.parent / f"macro_report_{suffix}.md", output_dir, "宏观报告 Markdown", ArtifactKind.MARKDOWN, paths.project_root),
            _relative_artifact(path.parent / f"macro_dashboard_{suffix}.html", output_dir, "宏观 Dashboard", ArtifactKind.HTML, paths.project_root),
        )
        if artifact is not None
    )
    warnings = (f"{len(overview.stale_series)} 条序列陈旧：{', '.join(overview.stale_series)}",) if overview.stale_series else ()
    module = ModuleResult(
        module_id="macro-regime",
        label="宏观体制",
        status=ModuleStatus.WARNING if warnings else ModuleStatus.SUCCESS,
        summary=f"{overview.primary_regime or 'N/A'} / {overview.liquidity_state or 'N/A'}",
        freshness=_freshness_for(snapshot_as_of, as_of),
        coverage=overview.data_coverage,
        model_version=overview.model_version,
        duration_ms=int((time.monotonic() - started) * 1000),
        warnings=warnings,
        artifacts=artifacts,
        metadata=(("来源", path.name),),
    )
    return overview, module


def _live_themes(paths: Paths, output_dir: Path, as_of: datetime) -> tuple[ThemeOverview | None, ModuleResult]:
    started = time.monotonic()
    found = _load_latest_json(paths.project_root / "outputs" / "theme-rotation", "theme_rotation_snapshot_*.json")
    if found is None:
        return None, ModuleResult(
            module_id="theme-rotation",
            label="科技主题轮动",
            status=ModuleStatus.SKIPPED,
            summary="未找到已发布的主题轮动快照（outputs/theme-rotation/theme_rotation_snapshot_*.json）",
            freshness=Freshness(FreshnessState.UNKNOWN),
            duration_ms=int((time.monotonic() - started) * 1000),
            metadata=(("建议", "先运行 `quant-agent theme-rotation` 生成快照"),),
        )
    path, payload = found
    snap = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else payload
    snapshot_as_of = _parse_datetime(snap.get("as_of"))
    themes = tuple(
        ThemeRow(
            theme_id=str(item.get("theme_id", "")),
            label=str(item.get("label", "")),
            proxy_symbol=str(item.get("proxy_symbol", "")),
            score=_float_or_none(item.get("score")),
            state=str(item.get("state", "")),
            trend=item.get("trend") if isinstance(item.get("trend"), str) else None,
            coverage=_float_or_none(item.get("data_coverage")),
            relative_5d=_float_or_none(item.get("relative_5d")),
            relative_20d=_float_or_none(item.get("relative_20d")),
            selected=item.get("theme_id") == snap.get("selected_theme_id"),
        )
        for item in snap.get("themes") or ()
        if isinstance(item, dict)
    )
    attributions = tuple(
        AttributionRow(
            label=str(item.get("label", "")),
            score=float(item.get("score") or 0.0),
            core_signal=str(item.get("core_signal", "")),
            invalidation=str(item.get("invalidation", "")),
        )
        for item in snap.get("attributions") or ()
        if isinstance(item, dict)
    )
    alerts = tuple(
        ThemeAlert(
            label=str(item.get("label", "")),
            direction=str(item.get("direction", "")),
            threshold=float(item.get("threshold") or 0.0),
            previous_value=float(item.get("previous_value") or 0.0),
            current_value=float(item.get("current_value") or 0.0),
        )
        for item in snap.get("alerts") or ()
        if isinstance(item, dict)
    )
    overview = ThemeOverview(
        benchmark_symbol=str(snap.get("benchmark_symbol", "")),
        selected_theme_id=snap.get("selected_theme_id") if isinstance(snap.get("selected_theme_id"), str) else None,
        themes=themes,
        attributions=attributions,
        alerts=alerts,
        data_coverage=_float_or_none(snap.get("data_coverage")),
        model_version=snap.get("model_version") if isinstance(snap.get("model_version"), str) else None,
    )
    suffix = snapshot_as_of.date().isoformat() if snapshot_as_of else path.stem.removeprefix("theme_rotation_snapshot_")
    artifacts = tuple(
        artifact
        for artifact in (
            _relative_artifact(path, output_dir, "主题快照 JSON", ArtifactKind.JSON, paths.project_root),
            _relative_artifact(path.parent / f"theme_rotation_report_{suffix}.md", output_dir, "主题报告 Markdown", ArtifactKind.MARKDOWN, paths.project_root),
            _relative_artifact(path.parent / f"theme_rotation_dashboard_{suffix}.html", output_dir, "主题 Dashboard", ArtifactKind.HTML, paths.project_root),
        )
        if artifact is not None
    )
    module = ModuleResult(
        module_id="theme-rotation",
        label="科技主题轮动",
        status=ModuleStatus.SUCCESS,
        summary=f"{len(themes)} 个主题，{len(alerts)} 条告警",
        freshness=_freshness_for(snapshot_as_of, as_of),
        coverage=overview.data_coverage,
        model_version=overview.model_version,
        duration_ms=int((time.monotonic() - started) * 1000),
        artifacts=artifacts,
        metadata=(("来源", path.name),),
    )
    return overview, module


def _skipped_china_module(module_id: str, label: str, started: float) -> ModuleResult:
    return ModuleResult(
        module_id=module_id,
        label=label,
        status=ModuleStatus.SKIPPED,
        summary="live 编排尚未接入该模块输入；请使用对应模块 CLI 单独运行",
        freshness=Freshness(FreshnessState.UNKNOWN),
        duration_ms=int((time.monotonic() - started) * 1000),
    )


def _live_sections(
    paths: Paths, db_path: Path, output_dir: Path, as_of: datetime
) -> tuple[tuple[ModuleResult, ...], MacroOverview | None, ThemeOverview | None, ChinaAShareOverview | None, IndexHealth]:
    index_health, index_module = _live_index_health(db_path)
    macro, macro_module = _live_macro(paths, output_dir, as_of)
    themes, theme_module = _live_themes(paths, output_dir, as_of)
    started = time.monotonic()
    china_modules = (
        _skipped_china_module("cn-wave-screen", "A股 WaveScore", started),
        _skipped_china_module("reversal-screen", "A股 Selloff Repair", started),
    )
    china = ChinaAShareOverview(universe_note=PILOT_UNIVERSE_NOTE)
    modules = (macro_module, theme_module, *china_modules, index_module)
    return modules, macro, themes, china, index_health


# --------------------------------------------------------------------------- #
# entry point
# --------------------------------------------------------------------------- #

def run_daily(
    *,
    mode: DailyRunMode,
    as_of: datetime,
    db_path: Path | str,
    output_dir: Path | str,
    paths: Paths | None = None,
) -> DailyRunResult:
    from quant_agent.daily_run.report import publish_daily_run_outputs

    paths = paths or Paths()
    db_path = Path(db_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now(timezone.utc)
    suffix = as_of.date().isoformat()
    run_id = f"daily-{suffix}-{mode.value}-{uuid.uuid4().hex[:8]}"

    if mode is DailyRunMode.FIXTURE:
        modules, macro, themes, china, index_health = _fixture_sections(as_of)
        notes = (
            "本页全部数据为 SYNTHETIC FIXTURE，仅用于演示与评审，不反映真实市场状态。",
            PILOT_UNIVERSE_NOTE,
        )
    else:
        modules, macro, themes, china, index_health = _live_sections(paths, db_path, output_dir, as_of)
        notes = (
            "live 模式聚合已发布的模块输出；未接入的模块标记为 SKIPPED。",
            PILOT_UNIVERSE_NOTE,
        )

    attention = _derive_attention(modules, themes, index_health)
    overall = _derive_overall_status(modules, attention)
    artifacts = (
        RunArtifact("Dashboard HTML", f"daily_run_dashboard_{suffix}.html", ArtifactKind.HTML),
        RunArtifact("快照 JSON", f"daily_run_snapshot_{suffix}.json", ArtifactKind.JSON),
        RunArtifact("摘要 Markdown", f"daily_run_report_{suffix}.md", ArtifactKind.MARKDOWN),
    )
    snapshot = DailyRunSnapshot(
        run_id=run_id,
        as_of=as_of,
        generated_at=generated_at,
        mode=mode,
        synthetic=mode is DailyRunMode.FIXTURE,
        overall_status=overall,
        modules=modules,
        macro=macro,
        themes=themes,
        china_a_share=china,
        index_health=index_health,
        attention=attention,
        artifacts=artifacts,
        notes=notes,
    )
    DailyRunRepository(db_path).save(snapshot)
    outputs = publish_daily_run_outputs(output_dir, snapshot)
    return DailyRunResult(snapshot=snapshot, outputs=outputs)
