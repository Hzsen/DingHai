from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path

from domain.macro import MacroRiskDocument, MacroSnapshot
from domain.macro_history import MacroAnalysisPacket, MacroChangeEvent, MacroHistoryPoint
from domain.market_theme import MarketThemeState, ThemeHorizon
from quant_agent.macro.dashboard import render_macro_dashboard
from quant_agent.macro.document import document_to_dict


FLOW_LABELS = {
    "FED_BALANCE_SHEET": "Fed balance sheet",
    "TREASURY_GENERAL_ACCOUNT": "TGA",
    "OVERNIGHT_REVERSE_REPO": "RRP",
    "US_LARGE_CAP": "US large cap",
    "AI_SEMICONDUCTOR": "AI / semiconductors",
    "US_SMALL_CAP": "US small cap",
    "US_BANKS_CREDIT": "Banks / credit",
    "TREASURY_7_10Y": "Treasury 7–10Y",
    "TREASURY_20Y_PLUS": "Treasury 20Y+",
    "GOLD": "Gold",
    "DOLLAR_CASH": "Dollar / cash",
}


def _format_billions(value: float) -> str:
    return f"{value:+.2f}bn" if 0 < abs(value) < 0.1 else f"{value:+.1f}bn"


def render_macro_markdown(
    snapshot: MacroSnapshot,
    history_points: list[MacroHistoryPoint] | None = None,
    change_events: list[MacroChangeEvent] | None = None,
    kimi_inference: dict | None = None,
    market_theme_states: tuple[MarketThemeState, ...] | None = None,
) -> str:
    history_points = history_points or []
    change_events = change_events or []
    market_theme_states = market_theme_states or ()
    net_flow = sum(item.flow_billions_usd_20d for item in snapshot.liquidity_source_flows)
    lines = [
        f"# Liquidity Transmission — {snapshot.as_of.date().isoformat()}", "",
        f"> Valid until {snapshot.valid_until.isoformat()}; model `{snapshot.model_version}`.", "",
        "## System Liquidity", "",
        f"- Primary: `{snapshot.primary_regime.value}`",
        f"- Risk: `{snapshot.risk_state.value}` ({snapshot.risk_score:.0f}/100)",
        f"- Liquidity: `{snapshot.liquidity_state.value}` ({snapshot.liquidity_score:+.0f})",
        f"- 20D source impulse: `{net_flow:+.1f}bn USD`",
        f"- Real-rate pressure: `{snapshot.rate_pressure_state.value}` ({snapshot.rate_pressure_score:.0f}/100)",
        f"- Confidence: {snapshot.confidence:.0%}; coverage: {snapshot.data_coverage:.0%}", "",
    ]
    _append_theme_markdown(lines, market_theme_states)
    _append_source_markdown(lines, snapshot)
    _append_target_markdown(lines, snapshot)
    _append_history_markdown(lines, history_points, change_events)
    _append_inference_markdown(lines, kimi_inference)
    lines.extend([
        "", "## Evidence", "", f"- Drivers: {', '.join(snapshot.main_drivers) or '-'}",
        f"- Confirmations: {', '.join(snapshot.confirming_signals) or '-'}",
        f"- Conflicts: {', '.join(snapshot.conflicting_signals) or '-'}",
        f"- Quality flags: {', '.join(snapshot.quality_flags) or '-'}",
        f"- Stale series: {', '.join(snapshot.stale_series) or '-'}", "",
        "> Target scores are relative liquidity-transmission proxies, not audited ETF creation/redemption flows, not investment advice, and not price forecasts.",
    ])
    return "\n".join(lines) + "\n"


def _append_theme_markdown(lines: list[str], states: tuple[MarketThemeState, ...]) -> None:
    for state in states:
        horizon_label = "Fast Market Theme (1–5D)" if state.horizon is ThemeHorizon.FAST else "Repricing Theme (14D)"
        lines.extend([
            f"## {horizon_label}", "",
            f"- Dominant: `{state.dominant_theme_id or 'NONE'}` — {state.dominant_label}",
            f"- Confidence: {state.confidence:.0%}",
            f"- Interpretation: {state.summary}",
            f"- Strongest signals: {', '.join(state.strongest_signals) or '-'}", "",
        ])
        if not state.active_themes:
            lines.extend([f"- No-theme reason: `{state.no_dominant_reason or 'UNKNOWN'}`", ""])
            continue
        lines.extend([
            "| Theme | Family | Confidence | Confirmations | Persistence |",
            "|---|---|---:|---:|---:|",
        ])
        for theme in state.active_themes:
            lines.append(
                f"| {theme.label} (`{theme.theme_id}`) | {theme.family.value} | {theme.confidence:.0%} | "
                f"{theme.confirmation_count}/{theme.confirmation_total} | {theme.persistence_periods} |"
            )
        dominant = state.active_themes[0]
        lines.extend([
            "", f"- Supporting evidence: {', '.join(dominant.supporting_evidence) or '-'}",
            f"- Conflicting evidence: {', '.join(dominant.conflicting_evidence) or '-'}",
            f"- Invalidation: {', '.join(dominant.invalidation_conditions) or '-'}", "",
        ])


def _append_source_markdown(lines: list[str], snapshot: MacroSnapshot) -> None:
    lines.extend([
        "## Source Decomposition", "",
        "| Source | 20D liquidity contribution | Direction | Observation |",
        "|---|---:|---|---|",
    ])
    for flow in snapshot.liquidity_source_flows:
        lines.append(
            f"| {FLOW_LABELS.get(flow.source_id, flow.source_id)} | {_format_billions(flow.flow_billions_usd_20d)} | "
            f"{flow.direction} | {flow.observation_date[:10]} |"
        )


def _append_target_markdown(lines: list[str], snapshot: MacroSnapshot) -> None:
    lines.extend([
        "", "## Liquidity Absorption by Target", "",
        "| Target | Proxy | Absorption | Score | Liquidity impulse | Market confirmation | Macro structure | Confidence |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ])
    for flow in snapshot.liquidity_target_flows:
        lines.append(
            f"| {FLOW_LABELS.get(flow.target_id, flow.target_id)} | {flow.proxy_symbol} | {flow.state.value} | "
            f"{flow.absorption_score:+.1f} | {flow.liquidity_impulse_component:+.1f} | "
            f"{flow.market_confirmation_component:+.1f} | {flow.structural_component:+.1f} | {flow.confidence:.0%} |"
        )


def _append_history_markdown(
    lines: list[str],
    history_points: list[MacroHistoryPoint],
    change_events: list[MacroChangeEvent],
) -> None:
    if not history_points:
        return
    first, current = history_points[0], history_points[-1]
    lines.extend([
        "", f"## Half-Month Change ({first.as_of.date()} → {current.as_of.date()})", "",
        f"- Net liquidity: {first.net_liquidity_20d_bn:+.1f}bn → {current.net_liquidity_20d_bn:+.1f}bn",
        f"- Risk score: {first.risk_score:.1f} → {current.risk_score:.1f}",
        f"- Real-rate pressure: {first.rate_pressure_score:.1f} → {current.rate_pressure_score:.1f}",
        f"- Material change events: {len(change_events)}",
    ])


def _append_inference_markdown(lines: list[str], kimi_inference: dict | None) -> None:
    if not kimi_inference:
        return
    dominant = kimi_inference.get("dominant_pricing_hypothesis", {})
    lines.extend([
        "", "## Kimi Pricing Hypothesis", "",
        f"- Risk type: `{dominant.get('risk_type', 'UNKNOWN')}`",
        f"- Confidence: {float(dominant.get('confidence', 0)):.0%}",
        f"- Hypothesis: {dominant.get('hypothesis', '')}",
        f"- Flow interpretation: {kimi_inference.get('flow_interpretation', '')}",
        f"- Unknowns: {', '.join(map(str, kimi_inference.get('unknowns', []))) or '-'}",
    ])


def publish_macro_outputs(
    output_dir: Path | str,
    snapshot: MacroSnapshot,
    document: MacroRiskDocument,
    history_points: list[MacroHistoryPoint] | None = None,
    change_events: list[MacroChangeEvent] | None = None,
    kimi_inference: dict | None = None,
    analysis_packet: MacroAnalysisPacket | None = None,
    market_theme_states: tuple[MarketThemeState, ...] | None = None,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = snapshot.as_of.date().isoformat()
    json_path = output_dir / f"macro_snapshot_{suffix}.json"
    markdown_path = output_dir / f"macro_report_{suffix}.md"
    html_path = output_dir / f"macro_dashboard_{suffix}.html"
    history_points = history_points or []
    change_events = change_events or []
    json_path.write_text(json.dumps({
        "snapshot": _serialize_snapshot(snapshot), "document": document_to_dict(document),
        "history": _serialize_snapshot(history_points), "change_events": _serialize_snapshot(change_events),
        "analysis_packet": _serialize_snapshot(analysis_packet) if analysis_packet else None,
        "market_theme_states": _serialize_snapshot(market_theme_states or ()),
        "kimi_inference": kimi_inference,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_macro_markdown(snapshot, history_points, change_events, kimi_inference, market_theme_states),
        encoding="utf-8",
    )
    html_path.write_text(
        render_macro_dashboard(snapshot, history_points, change_events, kimi_inference, market_theme_states),
        encoding="utf-8",
    )
    paths = {"json": json_path, "markdown": markdown_path, "html": html_path}
    if analysis_packet is not None:
        packet_path = output_dir / f"macro_analysis_packet_{suffix}.json"
        packet_path.write_text(
            json.dumps(_serialize_snapshot(analysis_packet), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        paths["analysis_packet"] = packet_path
    return paths


def _serialize_snapshot(snapshot: object) -> object:
    def convert(value: object) -> object:
        if isinstance(value, Enum):
            return value.value
        if hasattr(value, "isoformat"):
            return value.isoformat()
        if is_dataclass(value):
            return convert(asdict(value))
        if isinstance(value, tuple):
            return [convert(item) for item in value]
        if isinstance(value, list):
            return [convert(item) for item in value]
        if isinstance(value, dict):
            return {key: convert(item) for key, item in value.items()}
        return value

    return convert(snapshot)
