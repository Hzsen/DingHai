from __future__ import annotations

import html
import json
from pathlib import Path

from domain.theme_rotation import ThemeReason, ThemeRotationState, ThemeTrend
from quant_agent.theme_rotation.engine import ThemeRotationEvaluation
from quant_agent.theme_rotation.serialization import jsonable


STATE_LABELS = {
    ThemeRotationState.CROWDED_UPTREND: "拥挤主升",
    ThemeRotationState.CONFIRMED_ENTRY: "确认进入",
    ThemeRotationState.EARLY_ROTATION: "早期轮动",
    ThemeRotationState.NEUTRAL_WATCH: "中性观察",
    ThemeRotationState.CAPITAL_EXIT: "资金撤出",
    ThemeRotationState.DISTRIBUTION_EXIT: "派发/撤出",
    ThemeRotationState.NO_DATA: "无数据",
}
TREND_LABELS = {
    ThemeTrend.STRONG: "强",
    ThemeTrend.LEAN_STRONG: "偏强",
    ThemeTrend.NEUTRAL: "中性",
    ThemeTrend.LEAN_WEAK: "偏弱",
    ThemeTrend.WEAK: "弱",
}
REASON_LABELS = {
    ThemeReason.ACTIVE_INFLOW: "主动流入",
    ThemeReason.EARLY_ROTATION: "早期轮动",
    ThemeReason.SOFTWARE_SEMICONDUCTOR_ROTATION: "强弱对调",
    ThemeReason.AI_CHAIN_DIFFUSION: "AI链条扩散",
    ThemeReason.HYPERSCALER_HANDOFF: "云巨头接棒",
    ThemeReason.CROWDED_UPTREND: "拥挤主升",
    ThemeReason.CROWDING_UNWIND: "拥挤出清",
    ThemeReason.DISTRIBUTION_EXIT: "派发撤出",
    ThemeReason.BETA_LIFT_FALSE_STRENGTH: "跟涨假强",
    ThemeReason.RELATIVE_RESILIENCE: "抗跌观察",
    ThemeReason.INTERNAL_DE_RISKING: "内部降风险",
    ThemeReason.MONTH_QUARTER_REBALANCE: "月/季再平衡",
    ThemeReason.AI_APPLICATION_CATCHUP: "AI应用追赶",
    ThemeReason.NEUTRAL: "中性",
    ThemeReason.NO_DATA: "无数据",
}


def _pct(value: float | None) -> str:
    return "na" if value is None else f"{value:.2f}%"


def _number(value: float | None, suffix: str = "") -> str:
    return "na" if value is None else f"{value:.2f}{suffix}"


def _score_class(score: float | None, state: ThemeRotationState | None = None) -> str:
    if state is ThemeRotationState.NO_DATA or score is None:
        return "missing"
    if state is ThemeRotationState.DISTRIBUTION_EXIT:
        return "distribution"
    if state is ThemeRotationState.CAPITAL_EXIT:
        return "exit"
    if state is ThemeRotationState.CROWDED_UPTREND:
        return "crowded"
    if score >= 75:
        return "confirmed"
    if score >= 60:
        return "early"
    if score >= 45:
        return "watch"
    return "weak"


def render_theme_rotation_markdown(evaluation: ThemeRotationEvaluation) -> str:
    snapshot = evaluation.snapshot
    lines = [
        f"# 科技主题资金轮动 — {snapshot.as_of.date().isoformat()}", "",
        f"- 基准：`{snapshot.benchmark_symbol}`",
        f"- 数据覆盖：{snapshot.data_coverage:.0%}",
        f"- 模型：`{snapshot.model_version}`；参考规则：`{snapshot.reference_model_version}`",
        f"- 质量标记：{', '.join(snapshot.quality_flags) or 'none'}", "",
        "## 主题横截面", "",
        "| 主题 | 代理 | 5D相对 | 20D相对 | 60D相对 | 绝对5D | 趋势 | 成交额 | 广度 | 分数/状态 | 可能原因 |",
        "|---|---:|---:|---:|---:|---:|---|---:|---:|---|---|",
    ]
    for item in snapshot.themes:
        proxy = item.proxy_symbol if item.kind.value == "ETF" else f"篮子 {item.live_members}/{item.member_count}"
        lines.append(
            f"| {item.label} | {proxy} | {_pct(item.relative_5d)} | {_pct(item.relative_20d)} | "
            f"{_pct(item.relative_60d)} | {_pct(item.absolute_5d)} | {TREND_LABELS[item.trend]} | "
            f"{_number(item.volume_ratio, 'x')} | {_pct(item.breadth)} | "
            f"{_number(item.score)} / {STATE_LABELS[item.state]} | {REASON_LABELS[item.reason]} |"
        )
    lines.extend(["", "## 轮动归因", "", "| 归因 | 分数 | 核心信号 | 失效条件 |", "|---|---:|---|---|"])
    for item in snapshot.attributions:
        lines.append(f"| {item.label} | {item.score:.0f} | {item.core_signal} | {item.invalidation} |")
    lines.extend(["", "## 阈值穿越告警", ""])
    if snapshot.alerts:
        lines.extend(
            f"- {item.label}: {item.previous_value:.1f} → {item.current_value:.1f} ({item.threshold:.0f})"
            for item in snapshot.alerts
        )
    else:
        lines.append("- 本次没有新的阈值穿越。")
    lines.extend([
        "", "> 本监控是历史市场数据的确定性统计整理，不预测未来，不构成投资建议。",
        "> “归因”是经验规则的证据强度，不是已验证的资金流因果关系。",
    ])
    return "\n".join(lines)


def render_theme_rotation_dashboard(evaluation: ThemeRotationEvaluation) -> str:
    snapshot = evaluation.snapshot
    rows: list[str] = []
    for item in snapshot.themes:
        score = "na" if item.score is None else f"{item.score:.0f}"
        proxy = item.proxy_symbol if item.kind.value == "ETF" else f"篮子 {item.live_members}/{item.member_count}"
        tooltip = f"成员: {' '.join(item.members)}；覆盖 {item.data_coverage:.0%}"
        css = _score_class(item.score, item.state)
        rows.append(
            f'<tr class="theme-row {css}" data-theme="{html.escape(item.theme_id)}" title="{html.escape(tooltip)}">'
            f'<td>{html.escape(item.label)}</td><td>{html.escape(proxy)}</td>'
            f'<td>{_pct(item.relative_5d)}</td><td>{_pct(item.relative_20d)}</td><td>{_pct(item.relative_60d)}</td>'
            f'<td>{_pct(item.absolute_5d)}</td><td>{TREND_LABELS[item.trend]}</td>'
            f'<td>{_number(item.volume_ratio, "x")}</td><td>{_pct(item.breadth)}</td>'
            f'<td class="state">{score} / {STATE_LABELS[item.state]}</td>'
            f'<td class="reason">{REASON_LABELS[item.reason]}</td></tr>'
        )
    attr_rows = "".join(
        f'<tr class="{_score_class(item.score)}" title="失效: {html.escape(item.invalidation)}">'
        f'<td>{html.escape(item.label)}</td><td>{item.score:.0f}</td><td>{html.escape(item.core_signal)}</td></tr>'
        for item in snapshot.attributions
    )
    alert_items = "".join(
        f'<li>{html.escape(item.label)} · {item.previous_value:.1f} → {item.current_value:.1f}</li>'
        for item in snapshot.alerts
    ) or "<li>本次没有新的阈值穿越</li>"
    history_json = json.dumps(
        {"dates": evaluation.dates, "scores": evaluation.theme_score_history},
        ensure_ascii=False, separators=(",", ":"), allow_nan=False,
    )
    initial_label = next(item.label for item in snapshot.themes if item.theme_id == snapshot.selected_theme_id)
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>科技主题资金轮动 {snapshot.as_of.date().isoformat()}</title>
<style>
:root{{--bg:#0d0f11;--panel:#15181c;--grid:#2c3035;--head:#363b45;--fg:#f3f4f6;--muted:#9ca3af;--green:#3ca84b;--teal:#0f8f84;--orange:#d98200;--red:#8b2635;--purple:#7b3b82;--cyan:#128f9d}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--fg);font:13px/1.35 system-ui,-apple-system,"PingFang SC",sans-serif}}
main{{max-width:1500px;margin:auto;padding:12px}} .meta{{display:flex;gap:18px;flex-wrap:wrap;color:var(--muted);padding:6px 2px 12px}}
table{{width:100%;border-collapse:collapse;table-layout:fixed}} th,td{{border:1px solid #17191c;padding:5px 6px;text-align:center;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
th{{background:var(--head);font-weight:600}} th:first-child,td:first-child{{text-align:left}} .theme-row{{cursor:pointer}} .theme-row:hover{{outline:1px solid #e5e7eb;outline-offset:-1px}}
.confirmed td{{background:#286f34}} .early td{{background:#0b746d}} .crowded td{{background:#b56a00}} .watch td{{background:#272b30}} .weak td{{background:#5c202a}} .exit td{{background:#7f2635}} .distribution td{{background:#a52f3e}} .missing td{{background:#30343a;color:#aaa}}
.state{{font-weight:700}} .reason{{font-weight:600}} .chart{{height:390px;margin:12px 0;border:1px solid var(--grid);background:linear-gradient(#111416,#0c0e10);position:relative}}
.chart-head{{position:absolute;top:10px;left:14px;z-index:2}} .chart-head strong{{font-size:15px}} .chart-head span{{color:var(--muted);margin-left:10px}}
#score-chart{{width:100%;height:100%}} .threshold{{stroke-width:1;stroke-dasharray:5 5}} .gridline{{stroke:#20242a;stroke-width:1}} #score-line{{fill:none;stroke:#e5e7eb;stroke-width:2.5;vector-effect:non-scaling-stroke}}
.lower{{display:grid;grid-template-columns:minmax(0,1fr) minmax(460px,0.9fr);gap:12px}} .card{{background:var(--panel);border:1px solid var(--grid);padding:10px}}
.card h2{{font-size:14px;font-weight:600;margin:0 0 8px}} .attr td:nth-child(1){{text-align:left}} .attr td:nth-child(2){{width:70px}} ul{{margin:6px 0;padding-left:20px}}
.disclaimer{{color:var(--muted);font-size:12px;margin-top:12px}} @media(max-width:900px){{.table-wrap{{overflow:auto}} table{{min-width:1050px}} .lower{{grid-template-columns:1fr}}}}
</style></head><body><main>
<div class="meta"><span>截至 {snapshot.as_of.isoformat()}</span><span>基准 {html.escape(snapshot.benchmark_symbol)}</span><span>覆盖 {snapshot.data_coverage:.0%}</span><span>模型 {html.escape(snapshot.model_version)}</span></div>
<div class="table-wrap"><table aria-label="科技主题资金轮动">
<thead><tr><th>主题</th><th>代理</th><th>5D相对</th><th>20D相对</th><th>60D相对</th><th>绝对5D</th><th>趋势</th><th>成交额</th><th>广度</th><th>分数/状态</th><th>可能原因</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div>
<section class="chart"><div class="chart-head"><strong id="chart-title">{html.escape(initial_label)}</strong><span>轮动分数 · 点击上方主题切换</span></div>
<svg id="score-chart" viewBox="0 0 1200 390" preserveAspectRatio="none" role="img" aria-label="主题轮动分数历史">
<line class="gridline" x1="0" y1="195" x2="1200" y2="195"/><line class="threshold" stroke="#3ca84b" x1="0" y1="97.5" x2="1200" y2="97.5"/><line class="threshold" stroke="#0f8f84" x1="0" y1="156" x2="1200" y2="156"/><line class="threshold" stroke="#8b2635" x1="0" y1="214.5" x2="1200" y2="214.5"/><polyline id="score-line" points=""/></svg></section>
<div class="lower"><section class="card"><h2>阈值穿越告警</h2><ul>{alert_items}</ul><h2>数据质量</h2><p>{html.escape(', '.join(snapshot.quality_flags) or '无额外质量标记')}</p></section>
<section class="card"><table class="attr"><thead><tr><th>归因</th><th>分数</th><th>核心信号</th></tr></thead><tbody>{attr_rows}</tbody></table></section></div>
<p class="disclaimer">仅供信息参考与教学演示，不构成投资建议。归因分数是人工规则的证据强度，不代表已核验资金流或因果结论。</p>
</main><script>
const history={history_json};
function renderTheme(id,label){{const values=history.scores[id]||[];const points=[];const n=Math.max(1,values.length-1);values.forEach((v,i)=>{{if(v!==null)points.push(`${{1200*i/n}},${{390-3.9*v}}`)}});document.getElementById('score-line').setAttribute('points',points.join(' '));document.getElementById('chart-title').textContent=label;}}
document.querySelectorAll('.theme-row').forEach(row=>row.addEventListener('click',()=>renderTheme(row.dataset.theme,row.cells[0].textContent)));
renderTheme({json.dumps(snapshot.selected_theme_id)},{json.dumps(initial_label, ensure_ascii=False)});
</script></body></html>"""


def publish_theme_rotation_outputs(
    output_dir: Path | str,
    evaluation: ThemeRotationEvaluation,
) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = evaluation.snapshot.as_of.date().isoformat()
    json_path = output_dir / f"theme_rotation_snapshot_{suffix}.json"
    markdown_path = output_dir / f"theme_rotation_report_{suffix}.md"
    html_path = output_dir / f"theme_rotation_dashboard_{suffix}.html"
    json_path.write_text(
        json.dumps(jsonable(evaluation), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    )
    markdown_path.write_text(render_theme_rotation_markdown(evaluation), encoding="utf-8")
    html_path.write_text(render_theme_rotation_dashboard(evaluation), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path, "html": html_path}
