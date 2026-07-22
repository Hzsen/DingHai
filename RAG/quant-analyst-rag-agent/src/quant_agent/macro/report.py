from __future__ import annotations

import html
import json
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path

from domain.macro import MacroRiskDocument, MacroSnapshot
from domain.macro_history import MacroAnalysisPacket, MacroChangeEvent, MacroHistoryPoint
from domain.market_theme import MarketThemeState, ThemeHorizon
from quant_agent.macro.document import document_to_dict


def _value(value):
    return value.value if isinstance(value, Enum) else value


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


def _polyline(values: list[float], width: int, height: int, low: float | None = None, high: float | None = None) -> str:
    if not values:
        return ""
    low = min(values) if low is None else low
    high = max(values) if high is None else high
    if high == low:
        high, low = high + 1, low - 1
    return " ".join(
        f"{8 + index * (width - 16) / max(1, len(values) - 1):.1f},"
        f"{8 + (high - value) * (height - 16) / (high - low):.1f}"
        for index, value in enumerate(values)
    )


def _display_inference_item(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return "; ".join(f"{key}: {item}" for key, item in value.items())
    return str(value)


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
    for state in market_theme_states:
        horizon_label = "Fast Market Theme (1–5D)" if state.horizon is ThemeHorizon.FAST else "Repricing Theme (14D)"
        lines.extend([
            f"## {horizon_label}", "",
            f"- Dominant: `{state.dominant_theme_id or 'NONE'}` — {state.dominant_label}",
            f"- Confidence: {state.confidence:.0%}",
            f"- Interpretation: {state.summary}",
            f"- Strongest signals: {', '.join(state.strongest_signals) or '-'}", "",
        ])
        if state.active_themes:
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
        else:
            lines.extend([f"- No-theme reason: `{state.no_dominant_reason or 'UNKNOWN'}`", ""])
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
    if history_points:
        first, current = history_points[0], history_points[-1]
        lines.extend([
            "", f"## Half-Month Change ({first.as_of.date()} → {current.as_of.date()})", "",
            f"- Net liquidity: {first.net_liquidity_20d_bn:+.1f}bn → {current.net_liquidity_20d_bn:+.1f}bn",
            f"- Risk score: {first.risk_score:.1f} → {current.risk_score:.1f}",
            f"- Real-rate pressure: {first.rate_pressure_score:.1f} → {current.rate_pressure_score:.1f}",
            f"- Material change events: {len(change_events)}",
        ])
    if kimi_inference:
        dominant = kimi_inference.get("dominant_pricing_hypothesis", {})
        lines.extend([
            "", "## Kimi Pricing Hypothesis", "",
            f"- Risk type: `{dominant.get('risk_type', 'UNKNOWN')}`",
            f"- Confidence: {float(dominant.get('confidence', 0)):.0%}",
            f"- Hypothesis: {dominant.get('hypothesis', '')}",
            f"- Flow interpretation: {kimi_inference.get('flow_interpretation', '')}",
            f"- Unknowns: {', '.join(map(str, kimi_inference.get('unknowns', []))) or '-'}",
        ])
    lines.extend([
        "", "## Evidence", "", f"- Drivers: {', '.join(snapshot.main_drivers) or '-'}",
        f"- Confirmations: {', '.join(snapshot.confirming_signals) or '-'}",
        f"- Conflicts: {', '.join(snapshot.conflicting_signals) or '-'}",
        f"- Quality flags: {', '.join(snapshot.quality_flags) or '-'}",
        f"- Stale series: {', '.join(snapshot.stale_series) or '-'}", "",
        "> Target scores are relative liquidity-transmission proxies, not audited ETF creation/redemption flows, not investment advice, and not price forecasts.",
    ])
    return "\n".join(lines) + "\n"


def render_macro_dashboard(
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
    source_max = max((abs(item.flow_billions_usd_20d) for item in snapshot.liquidity_source_flows), default=1.0)
    source_rows: list[str] = []
    for item in snapshot.liquidity_source_flows:
        width = min(50.0, abs(item.flow_billions_usd_20d) / source_max * 50.0)
        side = "positive" if item.flow_billions_usd_20d >= 0 else "negative"
        position = f"left:50%;width:{width:.1f}%" if side == "positive" else f"right:50%;width:{width:.1f}%"
        source_rows.append(
            f'<div class="flow-row"><div><strong>{html.escape(FLOW_LABELS.get(item.source_id, item.source_id))}</strong>'
            f'<small>{html.escape(item.observation_date[:10])}</small></div>'
            f'<div class="signed-track" aria-label="{item.flow_billions_usd_20d:+.1f} billion US dollars">'
            f'<span class="{side}" style="{position}"></span></div>'
            f'<div class="number {side}">{_format_billions(item.flow_billions_usd_20d)}</div></div>'
        )
    target_rows: list[str] = []
    for item in snapshot.liquidity_target_flows:
        width = min(50.0, abs(item.absorption_score) / 2.0)
        side = "positive" if item.absorption_score >= 0 else "negative"
        position = f"left:50%;width:{width:.1f}%" if side == "positive" else f"right:50%;width:{width:.1f}%"
        support = ", ".join(item.supporting_signals) or "no positive confirmation"
        conflict = ", ".join(item.conflicting_signals) or "no material conflict"
        target_rows.append(
            f'<div class="target-row"><div class="target-name"><strong>{html.escape(FLOW_LABELS.get(item.target_id, item.target_id))}</strong>'
            f'<small>{html.escape(item.proxy_symbol)} · confidence {item.confidence:.0%}</small></div>'
            f'<div><div class="signed-track" aria-label="absorption score {item.absorption_score:+.0f}">'
            f'<span class="{side}" style="{position}"></span></div>'
            f'<small>Liq {item.liquidity_impulse_component:+.1f} · confirmation {item.market_confirmation_component:+.1f} · structure {item.structural_component:+.1f}</small></div>'
            f'<div class="target-state {side}"><strong>{html.escape(item.state.value)}</strong><span>{item.absorption_score:+.1f}</span></div>'
            f'<div class="evidence-line"><span>Support: {html.escape(support)}</span><span>Conflict: {html.escape(conflict)}</span></div></div>'
        )
    drivers = "".join(f"<li>{html.escape(item)}</li>" for item in snapshot.main_drivers) or "<li>None</li>"
    conflicts = "".join(f"<li>{html.escape(item)}</li>" for item in snapshot.conflicting_signals) or "<li>None</li>"
    stale = ", ".join(snapshot.stale_series) or "none"
    quality = ", ".join(snapshot.quality_flags) or "none"
    fast_theme = next((state for state in market_theme_states if state.horizon is ThemeHorizon.FAST), None)
    repricing_theme = next((state for state in market_theme_states if state.horizon is ThemeHorizon.REPRICING), None)
    llm_label = "Pending deterministic trigger"
    if kimi_inference:
        llm_label = str(kimi_inference.get("dominant_pricing_hypothesis", {}).get("risk_type", "UNKNOWN"))
    theme_state_payload = _serialize_snapshot(market_theme_states)
    horizon_buttons = "".join(
        f'<button type="button" class="theme-tab" data-theme-state="{index}" aria-pressed="{str(index == 0).lower()}">'
        f'{"1–5D fast theme" if state.horizon is ThemeHorizon.FAST else "14D repricing"}</button>'
        for index, state in enumerate(market_theme_states)
    )
    theme_explorer = ""
    theme_script = ""
    if market_theme_states:
        theme_explorer = (
            '<h2>Market theme evidence explorer</h2>'
            f'<div class="theme-tabs" aria-label="theme horizon">{horizon_buttons}</div>'
            '<div id="theme-candidate-list" class="theme-candidates" aria-label="active themes"></div>'
            '<section class="card theme-detail" aria-live="polite">'
            '<div class="theme-detail-head"><div><small id="theme-family"></small><div id="theme-label" class="value"></div></div>'
            '<div><strong id="theme-confidence"></strong><small id="theme-confirmation"></small></div></div>'
            '<p id="theme-summary"></p><div class="evidence theme-evidence">'
            '<div><h3>Supporting evidence</h3><ul id="theme-support"></ul></div>'
            '<div><h3>Conflicts & invalidation</h3><ul id="theme-conflict"></ul><ul id="theme-invalidation"></ul></div>'
            '</div></section>'
        )
        theme_script = """<script>
const themeStates = %s;
const candidateList = document.getElementById('theme-candidate-list');
const putList = (id, values, prefix) => {
  const node = document.getElementById(id);
  node.replaceChildren();
  const items = values && values.length ? values : ['None'];
  items.forEach(value => {
    const li = document.createElement('li');
    li.textContent = prefix + value;
    node.appendChild(li);
  });
};
const renderCandidate = (stateIndex, candidateIndex) => {
  const state = themeStates[stateIndex];
  const item = state.active_themes[candidateIndex];
  document.querySelectorAll('.theme-choice').forEach((button, index) => button.setAttribute('aria-pressed', String(index === candidateIndex)));
  document.getElementById('theme-family').textContent = item ? item.family + ' · ' + item.horizon : state.horizon;
  document.getElementById('theme-label').textContent = item ? item.label : state.dominant_label;
  document.getElementById('theme-confidence').textContent = Math.round((item ? item.confidence : state.confidence) * 100) + '%% confidence';
  document.getElementById('theme-confirmation').textContent = item ? item.confirmation_count + '/' + item.confirmation_total + ' confirmations · ' + item.persistence_periods + ' periods' : (state.no_dominant_reason || 'No active theme');
  document.getElementById('theme-summary').textContent = item ? item.summary : state.summary;
  putList('theme-support', item ? item.supporting_evidence : state.strongest_signals, '');
  putList('theme-conflict', item ? item.conflicting_evidence : [], 'Conflict: ');
  putList('theme-invalidation', item ? item.invalidation_conditions : [], 'Invalidation: ');
};
const renderState = stateIndex => {
  const state = themeStates[stateIndex];
  document.querySelectorAll('.theme-tab').forEach((button, index) => button.setAttribute('aria-pressed', String(index === stateIndex)));
  candidateList.replaceChildren();
  const themes = state.active_themes.length ? state.active_themes : [{label: state.dominant_label}];
  themes.forEach((item, index) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'theme-choice';
    button.textContent = item.label;
    button.setAttribute('aria-pressed', String(index === 0));
    button.addEventListener('click', () => renderCandidate(stateIndex, index));
    candidateList.appendChild(button);
  });
  renderCandidate(stateIndex, 0);
};
document.querySelectorAll('.theme-tab').forEach((button, index) => button.addEventListener('click', () => renderState(index)));
renderState(0);
</script>""" % json.dumps(theme_state_payload, ensure_ascii=False).replace("</", "<\\/")
    history_section = ""
    if history_points:
        width, height = 920, 105
        net_values = [point.net_liquidity_20d_bn for point in history_points]
        risk_values = [point.risk_score for point in history_points]
        rate_values = [point.rate_pressure_score for point in history_points]
        start_label, end_label = history_points[0].as_of.date().isoformat(), history_points[-1].as_of.date().isoformat()
        target_rows_history: list[str] = []
        ordered_targets = [item.target_id for item in snapshot.liquidity_target_flows]
        for target_id in ordered_targets:
            values = [point.target_absorption.get(target_id, 0.0) for point in history_points]
            line_class = "target-positive" if values[-1] >= 0 else "target-negative"
            target_rows_history.append(
                f'<div class="spark-row"><div><strong>{html.escape(FLOW_LABELS.get(target_id, target_id))}</strong>'
                f'<small>{values[0]:+.1f} → {values[-1]:+.1f}</small></div>'
                f'<svg viewBox="0 0 {width} 70" role="img" aria-label="14 day {html.escape(target_id)} absorption trend">'
                f'<line class="zero" x1="8" y1="35" x2="{width-8}" y2="35"></line>'
                f'<polyline class="{line_class}" points="{_polyline(values, width, 70, -100, 100)}"></polyline></svg></div>'
            )
        history_section = (
            f'<h2>14-day liquidity repricing</h2><div class="range-label"><span>{start_label}</span><span>{end_label}</span></div>'
            f'<div class="trend-chart"><div class="chart-label"><strong>Net USD liquidity</strong><small>{net_values[0]:+.1f}bn → {net_values[-1]:+.1f}bn</small></div>'
            f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="14 day net dollar liquidity trend"><polyline class="net-line" points="{_polyline(net_values, width, height)}"></polyline></svg></div>'
            f'<div class="trend-chart"><div class="chart-label"><strong>Constraints</strong><small>risk {risk_values[-1]:.0f} · real-rate {rate_values[-1]:.0f}</small></div>'
            f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="14 day risk and real rate pressure trend">'
            f'<polyline class="risk-line" points="{_polyline(risk_values, width, height, 0, 100)}"></polyline>'
            f'<polyline class="rate-line" points="{_polyline(rate_values, width, height, 0, 100)}"></polyline></svg>'
            f'<div class="legend"><span>Risk</span><span>Real-rate pressure</span></div></div>'
            f'<h2>14-day target absorption rotation</h2><section class="sparks">{"".join(target_rows_history)}</section>'
        )
    if kimi_inference:
        dominant = kimi_inference.get("dominant_pricing_hypothesis", {})
        support = "".join(f"<li>{html.escape(_display_inference_item(item))}</li>" for item in dominant.get("supporting_evidence", [])) or "<li>None</li>"
        contradict = "".join(f"<li>{html.escape(_display_inference_item(item))}</li>" for item in dominant.get("contradicting_evidence", [])) or "<li>None</li>"
        rotations = "".join(f"<li>{html.escape(_display_inference_item(item))}</li>" for item in kimi_inference.get("target_rotation", [])) or "<li>None</li>"
        kimi_section = (
            '<h2>Kimi pricing hypothesis</h2><section class="kimi-grid">'
            f'<div class="card"><small>Dominant hypothesis · confidence {float(dominant.get("confidence", 0)):.0%}</small>'
            f'<div class="value">{html.escape(str(dominant.get("risk_type", "UNKNOWN")))}</div>'
            f'<p>{html.escape(str(dominant.get("hypothesis", "")))}</p></div>'
            f'<div class="card"><strong>Flow interpretation</strong><p>{html.escape(str(kimi_inference.get("flow_interpretation", "")))}</p>'
            f'<strong>Target rotation</strong><ul>{rotations}</ul></div></section>'
            f'<section class="evidence"><div class="card"><h3>Supporting evidence</h3><ul>{support}</ul></div>'
            f'<div class="card"><h3>Contradicting evidence</h3><ul>{contradict}</ul></div></section>'
        )
    else:
        event_items = "".join(f"<li>{html.escape(event.event_type)} · {html.escape(event.entity_id)} · {html.escape(event.direction)}</li>" for event in change_events) or "<li>No material trigger</li>"
        kimi_section = (
            '<h2>Kimi pricing hypothesis</h2><section class="card pending"><strong>Analysis not generated for this packet.</strong>'
            f'<small>{len(change_events)} deterministic trigger(s) detected.</small><ul>{event_items}</ul></section>'
        )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Liquidity Transmission {snapshot.as_of.date().isoformat()}</title>
<style>
:root{{--bg:#f6f7f9;--fg:#17202a;--card:#fff;--muted:#5f6b76;--border:#d9dee5;--positive:#177245;--negative:#a63a3a;--neutral:#6b7280;--track:#dce2e8}}
@media(prefers-color-scheme:dark){{:root{{--bg:#11151a;--fg:#edf1f5;--card:#1b2128;--muted:#a9b2bc;--border:#36404b;--positive:#57c78a;--negative:#ef8585;--neutral:#a9b2bc;--track:#36404b}}}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;overflow-x:hidden;overflow-wrap:anywhere}}
main{{width:100%;max-width:1180px;margin:auto;padding:24px;overflow:hidden}} h1,h2{{font-weight:500}} h2{{margin-top:26px}} .meta,small{{color:var(--muted)}} small{{display:block}}
.summary{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:18px 0}} .layers{{grid-template-columns:repeat(3,minmax(0,1fr))}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px;min-width:0}} .value{{font-size:20px;font-weight:500;margin-top:6px;overflow-wrap:anywhere}}
.flow-row{{display:grid;grid-template-columns:190px 1fr 90px;gap:14px;align-items:center;padding:12px 0;border-bottom:1px solid var(--border)}}
.target-row{{display:grid;grid-template-columns:190px 1fr 150px;gap:14px;align-items:center;padding:14px 0;border-bottom:1px solid var(--border)}}
.signed-track{{height:10px;background:var(--track);position:relative}} .signed-track:after{{content:"";position:absolute;left:50%;top:-3px;bottom:-3px;width:1px;background:var(--muted)}}
.signed-track span{{display:block;position:absolute;top:0;height:100%}} .signed-track .positive{{background:var(--positive)}} .signed-track .negative{{background:var(--negative)}}
.number,.target-state{{text-align:right}} .positive{{color:var(--positive)}} .negative{{color:var(--negative)}} .target-state span{{display:block;font-size:20px}}
.evidence-line{{grid-column:2/4;display:flex;gap:18px;flex-wrap:wrap;color:var(--muted);font-size:12px;overflow-wrap:anywhere}}
.evidence{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:18px}} ul{{padding-left:18px}} .warning{{color:var(--negative)}} .note{{margin:10px 0;color:var(--muted)}}
.range-label{{display:flex;justify-content:space-between;color:var(--muted);margin-bottom:4px}} .trend-chart{{display:grid;grid-template-columns:180px 1fr;gap:16px;align-items:center;padding:10px 0;border-bottom:1px solid var(--border)}}
.trend-chart svg,.spark-row svg{{width:100%;height:auto}} .trend-chart polyline,.spark-row polyline{{fill:none;stroke-width:3;vector-effect:non-scaling-stroke}} .net-line,.target-positive{{stroke:var(--positive)}} .target-negative,.risk-line{{stroke:var(--negative)}} .rate-line{{stroke:var(--neutral)}} .zero{{stroke:var(--border);stroke-width:1}}
.legend{{grid-column:2;display:flex;gap:18px;color:var(--muted)}} .spark-row{{display:grid;grid-template-columns:180px 1fr;gap:16px;align-items:center;padding:7px 0;border-bottom:1px solid var(--border)}}
.kimi-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px}} .pending{{margin-top:8px}} h3{{font-weight:500}}
.theme-tabs,.theme-candidates{{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}} .theme-tab,.theme-choice{{font:inherit;color:var(--fg);background:var(--card);border:1px solid var(--border);border-radius:8px;padding:8px 12px;cursor:pointer}}
.theme-tab[aria-pressed="true"],.theme-choice[aria-pressed="true"]{{background:var(--fg);color:var(--bg)}} .theme-detail{{margin-top:10px}} .theme-detail-head{{display:flex;justify-content:space-between;gap:18px;align-items:start}} .theme-detail-head>div:last-child{{text-align:right}} .theme-evidence{{margin-top:12px}}
@media(max-width:760px){{main{{padding:12px}} .summary,.layers{{grid-template-columns:1fr}} .flow-row,.target-row,.trend-chart,.spark-row{{grid-template-columns:1fr}} .flow-row>*,.target-row>*,.trend-chart>*,.spark-row>*{{min-width:0}} .number,.target-state{{text-align:left}} .evidence-line{{grid-column:auto}} .evidence,.kimi-grid{{grid-template-columns:1fr}} .legend{{grid-column:1}} .theme-detail-head{{display:block}} .theme-detail-head>div:last-child{{text-align:left;margin-top:8px}}}}
</style></head><body><main>
<h1>Liquidity Transmission</h1><div class="meta">As of {snapshot.as_of.isoformat()} · valid until {snapshot.valid_until.isoformat()} · coverage {snapshot.data_coverage:.0%} · confidence {snapshot.confidence:.0%}</div>
<div class="note">Source flows are balance-sheet changes in USD. Target scores are relative absorption proxies, not audited ETF fund flows.</div>
<h2>Three-layer market view</h2><section class="summary layers" aria-label="slow fast and explanation layers">
<div class="card"><small>Slow layer · weeks to months</small><div class="value">{html.escape(snapshot.primary_regime.value)}</div><p>Risk {html.escape(snapshot.risk_state.value)} · liquidity {html.escape(snapshot.liquidity_state.value)} · real-rate {html.escape(snapshot.rate_pressure_state.value)}</p></div>
<div class="card"><small>Fast layer · 1–5D{f' / 14D {repricing_theme.dominant_label}' if repricing_theme else ''}</small><div class="value">{html.escape(fast_theme.dominant_label if fast_theme else 'Not evaluated')}</div><p>{html.escape(fast_theme.summary if fast_theme else 'No market-theme state was supplied.')}</p></div>
<div class="card"><small>Explanation layer · RAG / LLM</small><div class="value">{html.escape(llm_label)}</div><p>Uses deterministic theme evidence, retrieved research and policy context; it does not recalculate market signals.</p></div>
</section>
{theme_explorer}
<section class="summary" aria-label="liquidity summary">
<div class="card"><div>20D system impulse</div><div class="value">{net_flow:+.1f}bn</div><small>WALCL − TGA − RRP</small></div>
<div class="card"><div>Liquidity state</div><div class="value">{html.escape(snapshot.liquidity_state.value)}</div><small>score {snapshot.liquidity_score:+.0f}</small></div>
<div class="card"><div>Risk transmission</div><div class="value">{html.escape(snapshot.risk_state.value)}</div><small>{snapshot.risk_score:.0f}/100</small></div>
<div class="card"><div>Real-rate constraint</div><div class="value">{html.escape(snapshot.rate_pressure_state.value)}</div><small>{snapshot.rate_pressure_score:.0f}/100</small></div>
</section>
{history_section}
<h2>Where system liquidity came from</h2><section aria-label="source flow decomposition">{''.join(source_rows)}</section>
<h2>Where liquidity is being absorbed</h2><section aria-label="target liquidity absorption">{''.join(target_rows)}</section>
{kimi_section}
<section class="evidence"><div class="card"><h2>Main drivers</h2><ul>{drivers}</ul></div><div class="card"><h2>Conflicts & quality</h2><ul>{conflicts}</ul><div class="warning">Quality flags: {html.escape(quality)}<br>Stale series: {html.escape(stale)}</div></div></section>
</main>{theme_script}</body></html>"""


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
    def convert(value):
        if isinstance(value, Enum): return value.value
        if hasattr(value, "isoformat"): return value.isoformat()
        if is_dataclass(value): return convert(asdict(value))
        if isinstance(value, tuple): return [convert(item) for item in value]
        if isinstance(value, list): return [convert(item) for item in value]
        if isinstance(value, dict): return {key: convert(item) for key, item in value.items()}
        return value
    return convert(snapshot)
