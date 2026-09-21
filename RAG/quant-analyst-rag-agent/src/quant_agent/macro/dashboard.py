from __future__ import annotations

import html
import json
from datetime import datetime, timezone

from domain.macro import MacroSnapshot
from domain.macro_history import MacroChangeEvent, MacroHistoryPoint
from domain.market_theme import MarketThemeState, ThemeHorizon


FLOW_LABELS = {
    "FED_BALANCE_SHEET": "美联储资产负债表",
    "TREASURY_GENERAL_ACCOUNT": "财政部一般账户（TGA）",
    "OVERNIGHT_REVERSE_REPO": "隔夜逆回购（RRP）",
    "US_LARGE_CAP": "美股大盘",
    "AI_SEMICONDUCTOR": "AI / 半导体",
    "US_SMALL_CAP": "美股小盘",
    "US_BANKS_CREDIT": "银行 / 信用",
    "TREASURY_7_10Y": "7–10 年期美债",
    "TREASURY_20Y_PLUS": "20 年以上美债",
    "GOLD": "黄金",
    "DOLLAR_CASH": "美元 / 现金",
}

STATE_LABELS = {
    "RISK_ON_CONFIRMING": "风险偏好正在确认",
    "RISK_ON_FRAGILE": "风险偏好脆弱",
    "TRANSITION": "环境过渡中",
    "RISK_OFF": "风险规避",
    "STRESS": "压力状态",
    "NORMAL": "正常",
    "ELEVATED": "偏高",
    "HIGH": "高",
    "CRITICAL": "严重",
    "STRONGLY_EXPANDING": "强扩张",
    "EXPANDING": "扩张",
    "NEUTRAL": "中性",
    "CONTRACTING": "收缩",
    "STRONGLY_CONTRACTING": "强收缩",
    "LOW": "低",
    "SUSTAINED_PRESSURE": "持续压力",
    "EXTREME_PRESSURE": "极高压力",
    "ABSORBING": "吸收",
    "MIXED": "分化",
    "REJECTING": "未吸收",
    "INJECTION": "注入",
    "DRAIN": "抽离",
}

REASON_LABELS = {
    "10Y_REAL_YIELD_HIGH_PERCENTILE": "10 年期实际利率处于历史高位",
    "10Y_REAL_YIELD_RISING_20D": "10 年期实际利率在 20 日窗口上行",
    "GOLD_WEAKNESS_CONFIRMS_REAL_RATE_PRESSURE": "黄金走弱印证实际利率约束",
    "SMALL_CAP_TRANSMISSION_WEAK": "小盘资产传导偏弱",
    "FED_BALANCE_SHEET_EXPANDING": "美联储资产负债表扩张",
    "TGA_DRAWDOWN_INJECTS_LIQUIDITY": "TGA 下降释放系统流动性",
    "RRP_DRAWDOWN_RELEASES_LIQUIDITY": "RRP 下降释放系统流动性",
    "DOLLAR_WEAKER_SUPPORTS_LIQUIDITY": "美元走弱支持流动性环境",
    "CREDIT_SPREAD_CONTAINED": "信用利差仍受控",
    "SYSTEM_LIQUIDITY_IMPULSE_POSITIVE": "系统流动性脉冲为正",
    "SYSTEM_LIQUIDITY_IMPULSE_NEGATIVE": "系统流动性脉冲为负",
    "MARKET_ABSORPTION_CONFIRMED": "市场价格确认流动性吸收",
    "MARKET_ABSORPTION_NOT_CONFIRMED": "市场价格尚未确认流动性吸收",
    "RELATIVE_TRANSMISSION_WEAKENING": "相对传导正在减弱",
    "MACRO_STRUCTURE_RESTRICTIVE": "宏观结构形成约束",
    "MACRO_STRUCTURE_SUPPORTIVE": "宏观结构提供支持",
    "BROAD_DOLLAR_PROXY_NOT_ICE_DXY": "美元使用广义代理指标，并非 ICE DXY",
    "STALE_SERIES": "数据序列已过期",
    "LIQUIDITY_SCORE_BELOW_20": "流动性评分降至 20 以下",
    "TARGET_DISPERSION_BELOW_20": "资产传导分化收敛至 20 以下",
    "LIQUIDITY_SCORE_AT_LEAST_20": "流动性评分至少为 20",
    "TARGET_ABSORPTION_DISPERSION_AT_LEAST_30": "资产吸收评分分化至少为 30",
    "AI_ABSORPTION_NEGATIVE": "AI / 半导体吸收评分为负",
    "BANKS_LEAD_AI_BY_15": "银行 / 信用领先 AI 至少 15 分",
    "LARGE_CAP_LEADS_AI_BY_15": "美股大盘领先 AI 至少 15 分",
    "NET_LIQUIDITY_14D_CHANGE": "14 日美元净流动性变化",
    "REAL_RATE_PRESSURE_14D_CHANGE": "14 日实际利率压力变化",
    "RISK_SCORE_14D_CHANGE": "14 日风险评分变化",
}

EVENT_LABELS = {
    "CROSS_ASSET_DIVERGENCE": "跨资产分歧",
    "MACRO_CONSTRAINT_SHIFT": "宏观约束变化",
    "MARKET_THEME_SHIFT": "市场主题切换",
    "SYSTEM_LIQUIDITY_SHIFT": "系统流动性变化",
    "TARGET_ROTATION": "资产传导轮动",
    "REGIME_SHIFT": "宏观环境切换",
    "RISK_SHIFT": "风险状态变化",
}

ENTITY_LABELS = {
    **FLOW_LABELS,
    "AI_VS_LARGE_CAP": "AI 相对大盘",
    "REAL_RATE_PRESSURE": "实际利率压力",
    "FAST_1_5D": "1–5 日主题",
    "REPRICING_14D": "14 日再定价主题",
    "NET_USD_LIQUIDITY": "美元净流动性",
}

DIRECTION_LABELS = {
    "EXPANDING": "扩张",
    "CONTRACTING": "收缩",
    "RISING": "上升",
    "FALLING": "下降",
    "CHANGED": "发生切换",
    "AI_LAGGING": "AI 落后",
    "ABSORPTION_STRENGTHENING": "吸收增强",
    "ABSORPTION_WEAKENING": "吸收减弱",
    "UP": "上升",
    "DOWN": "下降",
}

THEME_FAMILY_LABELS = {
    "RATES_INFLATION": "利率与通胀",
    "EASING_DEFENSIVE": "宽松与防御",
    "STRESS_OVERRIDE": "压力优先",
    "EQUITY_INTERNALS": "股市内部结构",
    "DIVERGENCE": "资产分化",
}


def _escape(value: object) -> str:
    return html.escape(str(value), quote=True)


def _state_label(value: object) -> str:
    raw = getattr(value, "value", value)
    return STATE_LABELS.get(str(raw), str(raw).replace("_", " ").title())


def _reason_label(value: object) -> str:
    raw = str(value)
    code, separator, observed_value = raw.partition("=")
    label = REASON_LABELS.get(code, code.replace("_", " ").lower())
    return f"{label}（{observed_value}）" if separator else label


def _format_billions(value: float) -> str:
    return f"{value:+.2f}" if 0 < abs(value) < 0.1 else f"{value:+.1f}"


def _format_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _snapshot_kind(snapshot: MacroSnapshot) -> str:
    now = datetime.now(timezone.utc)
    valid_until = snapshot.valid_until
    if valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=timezone.utc)
    return "历史快照" if valid_until < now else "当前有效快照"


def _compatible_history(
    snapshot: MacroSnapshot,
    history_points: list[MacroHistoryPoint],
) -> tuple[list[MacroHistoryPoint], str | None]:
    eligible = sorted((point for point in history_points if point.as_of <= snapshot.as_of), key=lambda point: point.as_of)
    if not eligible:
        return [], "暂无历史数据"
    compatible = [point for point in eligible if point.model_version == snapshot.model_version]
    if not compatible:
        return [], "历史点与当前模型版本不兼容"
    if len(compatible) != len(eligible):
        return compatible, "已排除模型版本不兼容的历史点"
    return compatible, None


def _comparison_point(history_points: list[MacroHistoryPoint]) -> MacroHistoryPoint | None:
    return history_points[0] if len(history_points) >= 2 else None


def _event_value(event: MacroChangeEvent, value: object) -> str:
    if value is None:
        return "暂无"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (float, int)):
        suffix = " 十亿美元" if event.entity_id == "NET_USD_LIQUIDITY" else ""
        return f"{float(value):+.1f}{suffix}"
    return _state_label(value)


def _event_summary(event: MacroChangeEvent) -> str:
    event_name = EVENT_LABELS.get(event.event_type, event.event_type.replace("_", " ").lower())
    entity_name = ENTITY_LABELS.get(event.entity_id, event.entity_id.replace("_", " ").lower())
    direction = DIRECTION_LABELS.get(event.direction, event.direction.replace("_", " ").lower())
    return f"{event_name} · {entity_name} {direction}"


def _key_changes(
    events: list[MacroChangeEvent],
    history_points: list[MacroHistoryPoint],
) -> list[MacroChangeEvent]:
    if len(history_points) < 2:
        return []
    start, end = history_points[0].as_of, history_points[-1].as_of
    eligible = [event for event in events if start <= event.window_start and event.as_of <= end]
    return sorted(eligible, key=lambda event: (event.as_of, abs(event.magnitude or 0.0)), reverse=True)[:3]


def _json_for_script(value: object) -> str:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def _svg_points(dates: list[datetime], values: list[float], low: float, high: float) -> list[tuple[float, float]]:
    if not dates or not values:
        return []
    timestamps = [date.timestamp() for date in dates]
    first, last = min(timestamps), max(timestamps)
    span = last - first
    value_span = high - low or 1.0
    return [
        (
            28.0 + (timestamp - first) / span * 644.0 if span else 350.0,
            16.0 + (high - value) / value_span * 104.0,
        )
        for timestamp, value in zip(timestamps, values, strict=True)
    ]


def _line_chart(
    chart_id: str,
    label: str,
    dates: list[datetime],
    values: list[float],
    unit: str,
    *,
    fixed_range: tuple[float, float] | None = None,
    tone: str = "blue",
) -> str:
    if not values:
        return '<div class="empty-inline">暂无可绘制数据</div>'
    low, high = fixed_range or (min(values), max(values))
    if low == high:
        padding = max(abs(low) * 0.1, 1.0)
        low, high = low - padding, high + padding
    points = _svg_points(dates, values, low, high)
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    circles = "".join(
        f'<circle tabindex="0" cx="{x:.1f}" cy="{y:.1f}" r="4" '
        f'aria-label="{_escape(date.date().isoformat())}，{value:.1f}{_escape(unit)}">'
        f'<title>{_escape(date.date().isoformat())} · {value:.1f}{_escape(unit)}</title></circle>'
        for (x, y), date, value in zip(points, dates, values, strict=True)
    )
    rows = "".join(
        f"<tr><td>{date.date().isoformat()}</td><td>{value:.1f}{_escape(unit)}</td></tr>"
        for date, value in zip(dates, values, strict=True)
    )
    return (
        f'<div class="chart" id="{_escape(chart_id)}"><div class="chart-head"><strong>{_escape(label)}</strong>'
        f'<span>{values[0]:.1f}{_escape(unit)} → {values[-1]:.1f}{_escape(unit)}</span></div>'
        f'<svg viewBox="0 0 700 136" role="img" aria-label="{_escape(label)}，{dates[0].date()} 至 {dates[-1].date()}">'
        f'<line class="chart-grid" x1="28" y1="120" x2="672" y2="120"></line>'
        f'<polyline class="chart-line {tone}" points="{polyline}"></polyline>{circles}</svg>'
        f'<div class="chart-dates"><span>{dates[0].date()}</span><span>{dates[-1].date()}</span></div>'
        f'<details class="data-table"><summary>查看逐日数值</summary><table><tbody>{rows}</tbody></table></details></div>'
    )


def _build_status_bar(snapshot: MacroSnapshot) -> str:
    latest_observation = max(
        (item.observation_date[:10] for item in snapshot.liquidity_source_flows),
        default=snapshot.as_of.date().isoformat(),
    )
    quality_text = f"{len(snapshot.quality_flags)} 项质量标记" if snapshot.quality_flags else "无质量标记"
    return f"""
    <header class="topbar">
      <button class="nav-toggle" id="nav-toggle" type="button" aria-label="打开页面导航" aria-expanded="false">☰</button>
      <div class="brand"><span class="brand-mark">宏</span><div><strong>宏观监测</strong><small>流动性与资产传导</small></div></div>
      <div class="status-strip" aria-label="快照与数据状态">
        <div><span>本期快照</span><strong>{_escape(_format_datetime(snapshot.as_of))}</strong></div>
        <div><span>来源数据截至</span><strong>{_escape(latest_observation)} · UTC</strong></div>
        <div><span>{_escape(_snapshot_kind(snapshot))}</span><strong>有效期至 {_escape(_format_datetime(snapshot.valid_until))}</strong></div>
        <div><span>数据覆盖与质量</span><strong>{snapshot.data_coverage:.0%} · {_escape(quality_text)}</strong></div>
      </div>
    </header>"""


def _build_overview(
    snapshot: MacroSnapshot,
    themes: tuple[MarketThemeState, ...],
    changes: list[MacroChangeEvent],
    kimi_inference: dict | None,
    comparison: MacroHistoryPoint | None,
) -> str:
    fast = next((state for state in themes if state.horizon is ThemeHorizon.FAST), None)
    repricing = next((state for state in themes if state.horizon is ThemeHorizon.REPRICING), None)
    theme_label = fast.dominant_label if fast else "暂无明确主导主题"
    conflicts = len(snapshot.conflicting_signals)
    summary = (
        f"宏观环境为{_state_label(snapshot.primary_regime)}，系统流动性{_state_label(snapshot.liquidity_state)}；"
        f"1–5 日市场主题为“{theme_label}”，当前有 {conflicts} 项冲突信号。"
    )
    change_cards = _build_change_cards(changes, comparison)
    limitation = _build_limitation_card(snapshot)
    explanation = _build_explanation_card(kimi_inference)
    return f"""
    <section class="module overview" id="overview" aria-labelledby="overview-title">
      <div class="section-kicker">10 秒总览</div>
      <div class="overview-heading"><div><h1 id="overview-title">当前宏观状态</h1><p>{_escape(summary)}</p></div>
        <a class="text-link" href="#quality">查看数据质量与依据 →</a></div>
      <div class="answer-grid">
        <article class="answer-card"><span>当前宏观环境</span><strong>{_escape(_state_label(snapshot.primary_regime))}</strong>
          <p>风险 {_escape(_state_label(snapshot.risk_state))} · 流动性 {_escape(_state_label(snapshot.liquidity_state))} · 实际利率 {_escape(_state_label(snapshot.rate_pressure_state))}</p></article>
        <article class="answer-card"><span>最近市场在交易什么</span><strong>{_escape(theme_label)}</strong>
          <p>{_escape(fast.summary if fast else "尚未提供 1–5 日主题状态。")}</p></article>
        <article class="answer-card"><span>与上一可比期相比</span>{change_cards}</article>
        {limitation}
      </div>
      <div class="layer-grid" aria-label="慢层、快层、14 日层与解释层">
        <article><span>慢层 · 数周至数月</span><strong>{_escape(_state_label(snapshot.primary_regime))}</strong><small>风险 {snapshot.risk_score:.0f}/100 · 流动性 {snapshot.liquidity_score:+.0f} · 实际利率 {snapshot.rate_pressure_score:.0f}/100</small></article>
        <article><span>快层 · 1–5 日</span><strong>{_escape(theme_label)}</strong><small>{f'{fast.confidence:.0%} 规则证据完整度' if fast else '本期未提供主题状态'}</small></article>
        <article><span>14 日层 · 再定价</span><strong>{_escape(repricing.dominant_label if repricing else "暂无可比较数据")}</strong><small>{_escape(repricing.summary if repricing else "未生成 14 日主题，不补推历史状态。")}</small></article>
        {explanation}
      </div>
      <div class="jump-row" aria-label="快速跳转">
        <a href="#liquidity">流动性来源</a><a href="#transmission">资产传导</a><a href="#themes">市场主题</a><a href="#changes">变化与事件</a>
      </div>
    </section>"""


def _build_change_cards(
    changes: list[MacroChangeEvent],
    comparison: MacroHistoryPoint | None,
) -> str:
    if comparison is None:
        return '<strong>暂无可比较数据</strong><p>需要至少两个同模型版本历史点。</p>'
    if not changes:
        return f'<strong>未检测到重大变化</strong><p>上一可比点：{comparison.as_of.date().isoformat()}</p>'
    items = "".join(
        f'<li><span>{_escape(event.as_of.date().isoformat())}</span>{_escape(_event_summary(event))}</li>'
        for event in changes
    )
    return f'<strong>{len(changes)} 项重要变化</strong><ol class="mini-change-list">{items}</ol>'


def _build_limitation_card(snapshot: MacroSnapshot) -> str:
    signal_text = (
        f"{len(snapshot.conflicting_signals)} 项市场冲突信号"
        if snapshot.conflicting_signals
        else "未记录市场冲突信号"
    )
    quality_text = (
        f"{len(snapshot.quality_flags)} 项数据质量标记"
        if snapshot.quality_flags
        else "未记录数据质量标记"
    )
    return (
        '<article class="answer-card limitation"><span>当前结论最主要的限制</span>'
        f'<strong>{_escape(signal_text)}</strong><p><b>市场分歧</b>与<b>数据质量</b>分别展示；当前另有 {_escape(quality_text)}。</p></article>'
    )


def _build_explanation_card(kimi_inference: dict | None) -> str:
    if not kimi_inference:
        return (
            '<article><span>解释层 · RAG / LLM</span><strong>本期未生成解释结果</strong>'
            '<small>仅展示确定性规则与现有数据，不新增推理调用。</small></article>'
        )
    dominant = kimi_inference.get("dominant_pricing_hypothesis", {})
    risk_type = dominant.get("risk_type", "未标注")
    hypothesis = dominant.get("hypothesis", "未提供解释文本")
    return (
        '<article><span>解释层 · RAG / LLM（已有结果）</span>'
        f'<strong>{_escape(risk_type)}</strong><small>{_escape(hypothesis)}</small></article>'
    )


def _build_liquidity(snapshot: MacroSnapshot) -> str:
    flows = snapshot.liquidity_source_flows
    net_flow = sum(item.flow_billions_usd_20d for item in flows)
    scale = max((abs(item.flow_billions_usd_20d) for item in flows), default=0.0)
    rows = "".join(_source_row(item, scale) for item in flows)
    return f"""
    <section class="module" id="liquidity" aria-labelledby="liquidity-title">
      <div class="section-heading"><div><span class="section-kicker">30 秒拆解</span><h2 id="liquidity-title">流动性来自哪里</h2>
        <p>观察窗口内 Fed、TGA、RRP 对系统流动性的模型贡献；统一单位为十亿美元，不代表今日实际资金流。</p></div>
        <div class="net-chip"><span>20 日净脉冲</span><strong>{_format_billions(net_flow)}<small> 十亿美元</small></strong></div></div>
      <div class="source-chart" role="img" aria-label="流动性来源共享零轴正负条形图">
        <div class="source-axis"><span>← 抽离</span><b>0</b><span>注入 →</span></div>{rows}
      </div>
      <p class="definition-note">读法：零轴右侧为注入、左侧为抽离；每行使用相同尺度。净值按现有模型定义计算。</p>
    </section>"""


def _source_row(item: object, scale: float) -> str:
    value = item.flow_billions_usd_20d
    width = 0.0 if scale == 0 else abs(value) / scale * 50.0
    side = "inject" if value >= 0 else "drain"
    position = f"left:50%;width:{width:.2f}%" if value >= 0 else f"right:50%;width:{width:.2f}%"
    return (
        '<div class="source-row">'
        f'<div><strong>{_escape(FLOW_LABELS.get(item.source_id, item.source_id))}</strong><small>观测日 {item.observation_date[:10]}</small></div>'
        f'<div class="zero-track" aria-label="{_escape(FLOW_LABELS.get(item.source_id, item.source_id))} {value:+.1f} 十亿美元">'
        f'<span class="{side}" style="{position}"></span></div>'
        f'<div class="numeric {side}">{_format_billions(value)}</div></div>'
    )


def _target_history(
    target_id: str,
    history_points: list[MacroHistoryPoint],
) -> tuple[list[datetime], list[float]]:
    pairs = [
        (point.as_of, point.target_absorption[target_id])
        for point in history_points
        if target_id in point.target_absorption
    ]
    return [date for date, _ in pairs], [value for _, value in pairs]


def _sparkline(dates: list[datetime], values: list[float], label: str) -> str:
    if not values:
        return '<span class="no-data">暂无趋势</span>'
    points = _svg_points(dates, values, -100.0, 100.0)
    compressed = " ".join(f"{x / 4.38:.1f},{(y - 6) / 2:.1f}" for x, y in points)
    circles = "".join(
        f'<circle tabindex="0" cx="{x / 4.38:.1f}" cy="{(y - 6) / 2:.1f}" r="3">'
        f'<title>{date.date().isoformat()} · {value:+.1f}</title></circle>'
        for (x, y), date, value in zip(points, dates, values, strict=True)
    )
    return (
        f'<svg class="sparkline" viewBox="0 0 160 62" role="img" aria-label="{_escape(label)}历史趋势">'
        f'<line x1="0" y1="31" x2="160" y2="31"></line><polyline points="{compressed}"></polyline>{circles}</svg>'
    )


def _build_transmission(
    snapshot: MacroSnapshot,
    history_points: list[MacroHistoryPoint],
    comparison: MacroHistoryPoint | None,
) -> tuple[str, list[dict[str, object]]]:
    rows: list[str] = []
    payload: list[dict[str, object]] = []
    for index, item in enumerate(snapshot.liquidity_target_flows):
        dates, values = _target_history(item.target_id, history_points)
        previous = comparison.target_absorption.get(item.target_id) if comparison else None
        change = item.absorption_score - previous if previous is not None else None
        rows.append(_target_row(index, item, dates, values, change))
        payload.append(_target_payload(item, dates, values, change))
    body = "".join(rows) or '<tr><td colspan="7" class="empty-inline">暂无资产传导数据</td></tr>'
    section = f"""
    <section class="module" id="transmission" aria-labelledby="transmission-title">
      <div class="section-heading"><div><span class="section-kicker">资产传导</span><h2 id="transmission-title">谁在吸收流动性</h2>
        <p>点击资产行查看评分构成、支持信号与冲突。吸收评分统一使用 −100 至 +100 尺度。</p></div></div>
      <div class="table-wrap"><table class="asset-table" id="asset-table">
        <thead><tr><th><button type="button" data-sort="label">资产 / 代理代码</button></th>
          <th><button type="button" data-sort="state">当前状态</button></th>
          <th class="numeric"><button type="button" data-sort="score">吸收评分</button></th>
          <th class="numeric"><button type="button" data-sort="change">可比期变化</button></th>
          <th>历史趋势</th><th class="numeric"><button type="button" data-sort="confidence">证据完整度</button></th><th aria-label="操作"></th></tr></thead>
        <tbody>{body}</tbody></table></div>
      <p class="definition-note"><strong>口径提示：</strong>资产吸收评分是相对流动性传导的模型代理指标，不是 ETF 申赎、真实资金净流入、上涨概率或预测准确率。</p>
    </section>"""
    return section, payload


def _target_row(
    index: int,
    item: object,
    dates: list[datetime],
    values: list[float],
    change: float | None,
) -> str:
    label = FLOW_LABELS.get(item.target_id, item.target_id)
    change_text = f"{change:+.1f}" if change is not None else "—"
    change_value = "" if change is None else f"{change:.8f}"
    return (
        f'<tr tabindex="0" data-asset-index="{index}" data-label="{_escape(label)}" '
        f'data-state="{_escape(_state_label(item.state))}" data-score="{item.absorption_score:.8f}" '
        f'data-change="{change_value}" data-confidence="{item.confidence:.8f}" '
        f'aria-label="查看 {_escape(label)} 详情">'
        f'<td><strong>{_escape(label)}</strong><small>{_escape(item.proxy_symbol)}</small></td>'
        f'<td><span class="state-pill state-{_escape(item.state.value.lower())}">{_escape(_state_label(item.state))}</span></td>'
        f'<td class="numeric score-value">{item.absorption_score:+.1f}</td>'
        f'<td class="numeric">{change_text}</td><td>{_sparkline(dates, values, label)}</td>'
        f'<td class="numeric">{item.confidence:.0%}</td><td><button type="button" class="row-open" tabindex="-1" aria-hidden="true">›</button></td></tr>'
    )


def _target_payload(
    item: object,
    dates: list[datetime],
    values: list[float],
    change: float | None,
) -> dict[str, object]:
    return {
        "label": FLOW_LABELS.get(item.target_id, item.target_id),
        "symbol": item.proxy_symbol,
        "state": _state_label(item.state),
        "score": item.absorption_score,
        "change": change,
        "confidence": item.confidence,
        "components": [
            {"label": "流动性脉冲", "value": item.liquidity_impulse_component},
            {"label": "市场确认", "value": item.market_confirmation_component},
            {"label": "宏观结构", "value": item.structural_component},
        ],
        "support": [{"label": _reason_label(code), "code": code} for code in item.supporting_signals],
        "conflicts": [{"label": _reason_label(code), "code": code} for code in item.conflicting_signals],
        "measurementNote": item.measurement_note,
        "history": [
            {"date": date.date().isoformat(), "value": value}
            for date, value in zip(dates, values, strict=True)
        ],
    }


def _build_history(history_points: list[MacroHistoryPoint], note: str | None) -> str:
    if not history_points:
        return f"""
        <section class="module" id="history"><div class="section-heading"><div><span class="section-kicker">历史趋势</span>
        <h2>约束与流动性的变化</h2><p>{_escape(note or '暂无历史数据')}</p></div></div>
        <div class="empty-state">没有可用的同版本历史点；不会补零或推断趋势。</div></section>"""
    dates = [point.as_of for point in history_points]
    net = [point.net_liquidity_20d_bn for point in history_points]
    risk = [point.risk_score for point in history_points]
    rate = [point.rate_pressure_score for point in history_points]
    notice = f'<div class="inline-notice">{_escape(note)}</div>' if note else ""
    return f"""
    <section class="module" id="history" aria-labelledby="history-title">
      <div class="section-heading"><div><span class="section-kicker">历史趋势 · 实际日期间隔</span><h2 id="history-title">约束与流动性的变化</h2>
        <p>不同单位分别绘制，不使用双轴；圆点可悬浮或用 Tab 聚焦查看日期与数值。</p></div></div>{notice}
      <div class="chart-grid">
        {_line_chart('net-history', '美元净流动性（20 日窗口）', dates, net, ' 十亿美元', tone='blue')}
        {_line_chart('risk-history', '风险评分', dates, risk, '/100', fixed_range=(0, 100), tone='amber')}
        {_line_chart('rate-history', '实际利率压力', dates, rate, '/100', fixed_range=(0, 100), tone='gray')}
      </div>
    </section>"""


def _theme_payload(state: MarketThemeState, candidate: object | None) -> dict[str, object]:
    if candidate is None:
        return {
            "label": state.dominant_label,
            "horizon": "1–5 日" if state.horizon is ThemeHorizon.FAST else "14 日",
            "family": "未形成主导主题",
            "summary": state.summary,
            "confidence": state.confidence,
            "confirmation": state.no_dominant_reason or "暂无活跃主题",
            "support": [{"label": _reason_label(code), "code": code} for code in state.strongest_signals],
            "conflicts": [],
            "invalidation": [],
        }
    return {
        "label": candidate.label,
        "horizon": "1–5 日" if candidate.horizon is ThemeHorizon.FAST else "14 日",
        "family": THEME_FAMILY_LABELS.get(candidate.family.value, candidate.family.value),
        "summary": candidate.summary,
        "confidence": candidate.confidence,
        "confirmation": f"{candidate.confirmation_count}/{candidate.confirmation_total} 项确认 · 持续 {candidate.persistence_periods} 期",
        "support": [{"label": _reason_label(code), "code": code} for code in candidate.supporting_evidence],
        "conflicts": [{"label": _reason_label(code), "code": code} for code in candidate.conflicting_evidence],
        "invalidation": [{"label": _reason_label(code), "code": code} for code in candidate.invalidation_conditions],
    }


def _build_themes(states: tuple[MarketThemeState, ...]) -> tuple[str, list[dict[str, object]]]:
    cards: list[str] = []
    payload: list[dict[str, object]] = []
    for state in states:
        horizon = "1–5 日交易主题" if state.horizon is ThemeHorizon.FAST else "14 日再定价主题"
        candidates = state.active_themes or (None,)
        candidate_buttons: list[str] = []
        for candidate in candidates:
            index = len(payload)
            item = _theme_payload(state, candidate)
            payload.append(item)
            candidate_buttons.append(
                f'<button type="button" class="theme-open" data-theme-index="{index}">'
                f'<span>{_escape(item["label"])}</span><small>查看依据、反证与失效条件 →</small></button>'
            )
        cards.append(
            f'<article class="theme-card"><div class="theme-card-head"><span>{_escape(horizon)}</span>'
            f'<strong>{state.confidence:.0%} 规则证据完整度</strong></div><h3>{_escape(state.dominant_label)}</h3>'
            f'<p>{_escape(state.summary)}</p><div class="theme-list">{"".join(candidate_buttons)}</div></article>'
        )
    if not cards:
        cards.append(
            '<article class="theme-card empty-state"><h3>暂无明确主导主题</h3>'
            '<p>本期未提供主题状态，不根据离散事件补出历史主题。</p></article>'
        )
    section = f"""
    <section class="module" id="themes" aria-labelledby="themes-title">
      <div class="section-heading"><div><span class="section-kicker">市场主题</span><h2 id="themes-title">短期交易与 14 日再定价</h2>
        <p>两个时间层级独立展示，不与慢层宏观状态合成方向分数。</p></div></div>
      <div class="theme-grid">{"".join(cards)}</div>
    </section>"""
    return section, payload


def _build_events(events: list[MacroChangeEvent], history_points: list[MacroHistoryPoint]) -> str:
    if len(history_points) < 2:
        body = '<div class="empty-state">暂无可比较数据；不会以 0 填充变化，也不会生成日报结论。</div>'
    elif not events:
        body = '<div class="empty-state">当前可比窗口内没有重大变化事件。</div>'
    else:
        body = '<ol class="event-list">' + "".join(_event_item(event) for event in events) + "</ol>"
    return f"""
    <section class="module" id="changes" aria-labelledby="changes-title">
      <div class="section-heading"><div><span class="section-kicker">变化与事件</span><h2 id="changes-title">真实发生的变化</h2>
        <p>仅列出现有 MacroChangeEvent；日期来自事件本身，不连续补绘主题状态。</p></div></div>{body}
    </section>"""


def _event_item(event: MacroChangeEvent) -> str:
    reasons = "".join(
        f'<li>{_escape(_reason_label(code))}<code>{_escape(code)}</code></li>' for code in event.reason_codes
    ) or "<li>未提供原因代码</li>"
    return (
        f'<li><time>{event.as_of.date().isoformat()}</time><div><strong>{_escape(_event_summary(event))}</strong>'
        f'<p>{_escape(_event_value(event, event.previous_value))} → {_escape(_event_value(event, event.current_value))}'
        f' · 比较窗口 {event.window_days} 天</p><details><summary>查看事件依据</summary><ul>{reasons}</ul></details></div></li>'
    )


def _evidence_group(title: str, items: tuple[str, ...], empty: str) -> str:
    rows = "".join(
        f'<li><span>{_escape(_reason_label(item))}</span><code>{_escape(item)}</code></li>' for item in items
    ) or f'<li class="empty-inline">{_escape(empty)}</li>'
    return f'<details><summary>{_escape(title)} <span>{len(items)}</span></summary><ul>{rows}</ul></details>'


def _build_quality(snapshot: MacroSnapshot) -> str:
    quality_banner = (
        f'<div class="quality-banner attention"><strong>数据质量：</strong>{len(snapshot.quality_flags)} 项标记，'
        f'{len(snapshot.stale_series)} 个过期序列。</div>'
        if snapshot.quality_flags or snapshot.stale_series
        else '<div class="quality-banner"><strong>数据质量：</strong>当前快照未记录质量标记或过期序列。</div>'
    )
    divergence_banner = (
        f'<div class="quality-banner divergence"><strong>市场分歧：</strong>{len(snapshot.conflicting_signals)} 项冲突信号，'
        '这与数据缺失或过期不是同一问题。</div>'
        if snapshot.conflicting_signals
        else '<div class="quality-banner"><strong>市场分歧：</strong>当前快照未记录冲突信号。</div>'
    )
    stale_items = tuple(snapshot.stale_series)
    return f"""
    <section class="module" id="quality" aria-labelledby="quality-title">
      <div class="section-heading"><div><span class="section-kicker">3 分钟证据</span><h2 id="quality-title">证据与数据质量</h2>
        <p>摘要先给结论，完整定义与原始 reason code 按需展开。</p></div></div>
      <div class="quality-grid">{quality_banner}{divergence_banner}</div>
      <div class="evidence-accordion">
        {_evidence_group('主要驱动', snapshot.main_drivers, '暂无主要驱动')}
        {_evidence_group('确认信号', snapshot.confirming_signals, '暂无确认信号')}
        {_evidence_group('冲突信号', snapshot.conflicting_signals, '暂无冲突信号')}
        {_evidence_group('数据质量标记', snapshot.quality_flags, '无数据质量标记')}
        {_evidence_group('过期序列', stale_items, '无过期序列')}
      </div>
      <p class="definition-note">规则证据完整度仅描述现有规则证据覆盖，不代表上涨概率、收益概率或预测准确率。模型版本：{_escape(snapshot.model_version)}。</p>
    </section>"""


def _build_navigation() -> str:
    return """
    <nav class="side-nav" id="side-nav" aria-label="页面导航">
      <div><span>阅读顺序</span><a class="active" href="#overview">总览</a><a href="#liquidity">流动性来源</a>
      <a href="#transmission">资产传导</a><a href="#history">历史趋势</a><a href="#themes">市场主题</a>
      <a href="#changes">变化与事件</a><a href="#quality">证据与数据质量</a></div>
      <p>蓝色表示当前选择与交互；琥珀色表示注意、约束或分歧。</p>
    </nav>"""


def _build_drawer() -> str:
    return """
    <div class="drawer-backdrop" id="drawer-backdrop" hidden></div>
    <aside class="detail-drawer" id="detail-drawer" aria-hidden="true" aria-labelledby="drawer-title">
      <header><div><span id="drawer-kicker">证据详情</span><h2 id="drawer-title">详情</h2></div>
      <button type="button" id="drawer-close" aria-label="关闭详情">×</button></header>
      <div class="drawer-body" id="drawer-body"></div>
    </aside>"""


def render_macro_dashboard(
    snapshot: MacroSnapshot,
    history_points: list[MacroHistoryPoint] | None = None,
    change_events: list[MacroChangeEvent] | None = None,
    kimi_inference: dict | None = None,
    market_theme_states: tuple[MarketThemeState, ...] | None = None,
) -> str:
    history_points = history_points or []
    change_events = change_events or []
    themes = market_theme_states or ()
    compatible_history, history_note = _compatible_history(snapshot, history_points)
    comparison = _comparison_point(compatible_history)
    important_changes = _key_changes(change_events, compatible_history)
    transmission, asset_payload = _build_transmission(snapshot, compatible_history, comparison)
    theme_section, theme_payload = _build_themes(themes)
    payload = _json_for_script({"assets": asset_payload, "themes": theme_payload})
    content = "".join([
        _build_overview(snapshot, themes, important_changes, kimi_inference, comparison),
        _build_liquidity(snapshot),
        transmission,
        _build_history(compatible_history, history_note),
        theme_section,
        _build_events(important_changes, compatible_history),
        _build_quality(snapshot),
    ])
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark"><title>宏观监测 · {snapshot.as_of.date().isoformat()}</title>
<style>{DASHBOARD_CSS}</style></head><body>{_build_status_bar(snapshot)}
<div class="layout">{_build_navigation()}<main>{content}<footer>仅供研究使用，不构成投资建议。</footer></main></div>
{_build_drawer()}<script id="dashboard-data" type="application/json">{payload}</script><script>{DASHBOARD_SCRIPT}</script></body></html>"""


DASHBOARD_CSS = r"""
:root{--bg:#0b0e13;--panel:#11151c;--panel-2:#171c24;--panel-3:#1d2430;--line:#252c37;--line-strong:#354052;--text:#e7ebf0;--muted:#8e99a8;--faint:#697485;--blue:#5e8bff;--blue-soft:rgba(94,139,255,.13);--amber:#e4ae56;--amber-soft:rgba(228,174,86,.12);--red:#ef6b73;--green:#48c78e;--header:74px;--nav:210px;--radius:8px;color-scheme:dark}
*{box-sizing:border-box}html{scroll-behavior:smooth;scroll-padding-top:calc(var(--header) + 16px)}body{margin:0;background:var(--bg);color:var(--text);font:13.5px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;overflow-wrap:anywhere}button,a{font:inherit}button{color:inherit}a{color:inherit;text-decoration:none}:focus-visible{outline:2px solid var(--blue);outline-offset:2px}
.topbar{position:sticky;z-index:40;top:0;min-height:var(--header);display:flex;align-items:center;gap:18px;padding:10px 18px;border-bottom:1px solid var(--line);background:rgba(11,14,19,.96);backdrop-filter:blur(14px)}.brand{min-width:190px;display:flex;align-items:center;gap:10px}.brand-mark{width:34px;height:34px;display:grid;place-items:center;border:1px solid rgba(94,139,255,.55);border-radius:7px;background:var(--blue-soft);color:#b9cbff;font-weight:750}.brand div{display:grid}.brand strong{font-size:14px}.brand small{color:var(--muted);font-size:11px}.status-strip{min-width:0;flex:1;display:grid;grid-template-columns:repeat(4,minmax(145px,1fr));border:1px solid var(--line);border-radius:7px;background:var(--panel)}.status-strip div{min-width:0;padding:7px 11px;border-right:1px solid var(--line)}.status-strip div:last-child{border-right:0}.status-strip span,.status-strip strong{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.status-strip span{color:var(--faint);font-size:10px}.status-strip strong{margin-top:2px;font-size:11.5px;font-variant-numeric:tabular-nums}.nav-toggle{display:none;width:34px;height:34px;border:1px solid var(--line);border-radius:6px;background:var(--panel-2)}
.layout{display:grid;grid-template-columns:var(--nav) minmax(0,1fr)}.side-nav{position:sticky;top:var(--header);height:calc(100vh - var(--header));padding:20px 12px;border-right:1px solid var(--line);background:var(--panel);overflow:auto}.side-nav>div{display:grid;gap:3px}.side-nav>div>span{padding:0 9px 8px;color:var(--faint);font-size:10px;font-weight:700;letter-spacing:.12em}.side-nav a{padding:8px 10px;border-left:2px solid transparent;border-radius:0 5px 5px 0;color:var(--muted)}.side-nav a:hover{background:rgba(255,255,255,.025);color:var(--text)}.side-nav a.active{border-left-color:var(--blue);background:var(--blue-soft);color:#c6d5ff}.side-nav p{position:absolute;right:15px;bottom:18px;left:15px;margin:0;color:var(--faint);font-size:11px;line-height:1.55}
main{min-width:0;width:100%;max-width:1220px;margin:0 auto;padding:26px 30px 60px}.module{padding:0 0 34px;margin:0 0 32px;border-bottom:1px solid var(--line)}.section-kicker{color:var(--blue);font-size:10px;font-weight:750;letter-spacing:.12em;text-transform:uppercase}h1,h2,h3,p{margin-top:0}h1{margin:4px 0 6px;font-size:26px;line-height:1.25}h2{margin:4px 0 6px;font-size:19px;line-height:1.3}h3{font-size:15px}.overview-heading,.section-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:28px}.overview-heading p,.section-heading p{max-width:720px;margin:0;color:var(--muted)}.text-link{flex:0 0 auto;margin-top:14px;color:#9ab5ff;font-size:12px}.answer-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:18px}.answer-card,.layer-grid article,.theme-card{min-width:0;border:1px solid var(--line);border-radius:var(--radius);background:var(--panel);padding:14px}.answer-card>span,.layer-grid article>span{display:block;color:var(--muted);font-size:11px}.answer-card>strong,.layer-grid article>strong{display:block;margin-top:8px;font-size:17px;line-height:1.3}.answer-card p{margin:8px 0 0;color:var(--muted);font-size:12px}.answer-card.limitation{border-color:rgba(228,174,86,.35);background:var(--amber-soft)}.answer-card b{color:#dfbd80}.mini-change-list{display:grid;gap:4px;margin:7px 0 0;padding:0;list-style:none;color:var(--muted);font-size:11px}.mini-change-list li span{display:block;color:var(--faint);font-variant-numeric:tabular-nums}.layer-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:8px}.layer-grid article{background:rgba(17,21,28,.65)}.layer-grid article>strong{font-size:14px}.layer-grid small{display:block;margin-top:6px;color:var(--muted);font-size:11px}.jump-row{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px}.jump-row a{padding:6px 9px;border:1px solid var(--line);border-radius:5px;background:var(--panel-2);color:#b8c8f2;font-size:11px}.jump-row a:hover{border-color:var(--blue)}
.net-chip{min-width:170px;padding:9px 12px;border:1px solid var(--line);border-radius:7px;background:var(--panel)}.net-chip span{display:block;color:var(--muted);font-size:10px}.net-chip strong{font:650 19px/1.25 ui-monospace,SFMono-Regular,Menlo,monospace}.net-chip small{color:var(--muted);font:11px/1.25 inherit}.source-chart{margin-top:16px;border:1px solid var(--line);border-radius:var(--radius);background:var(--panel);overflow:hidden}.source-axis,.source-row{display:grid;grid-template-columns:230px minmax(260px,1fr) 92px;align-items:center;gap:16px}.source-axis{min-height:31px;padding:0 14px;border-bottom:1px solid var(--line);color:var(--faint);font-size:10px}.source-axis span:first-child{text-align:left}.source-axis span:last-child{text-align:right}.source-axis b{text-align:center;font-weight:400}.source-row{min-height:62px;padding:9px 14px;border-bottom:1px solid var(--line)}.source-row:last-child{border-bottom:0}.source-row strong,.source-row small{display:block}.source-row small{color:var(--muted);font-size:11px}.zero-track{position:relative;height:10px;background:#1c2330}.zero-track:after{content:"";position:absolute;top:-5px;bottom:-5px;left:50%;width:1px;background:#647086}.zero-track span{position:absolute;top:0;height:100%}.zero-track .inject{background:var(--blue)}.zero-track .drain{background:var(--amber)}.numeric{font-variant-numeric:tabular-nums;text-align:right;white-space:nowrap}.numeric.inject{color:#a9c0ff}.numeric.drain{color:#edc47f}.definition-note{margin:10px 0 0;padding:9px 11px;border-left:2px solid var(--line-strong);background:rgba(17,21,28,.55);color:var(--muted);font-size:11.5px}
.table-wrap{margin-top:15px;border:1px solid var(--line);border-radius:var(--radius);overflow:auto;background:var(--panel)}table{width:100%;border-collapse:collapse}.asset-table{min-width:850px}.asset-table th{height:34px;padding:0 10px;border-bottom:1px solid var(--line);color:var(--faint);font-size:10px;font-weight:600;text-align:left;white-space:nowrap}.asset-table th.numeric{text-align:right}.asset-table th button{padding:0;border:0;background:none;color:inherit;cursor:pointer}.asset-table th button:hover{color:var(--text)}.asset-table th[aria-sort="ascending"] button:after{content:" ↑";color:var(--blue)}.asset-table th[aria-sort="descending"] button:after{content:" ↓";color:var(--blue)}.asset-table td{height:58px;padding:7px 10px;border-bottom:1px solid var(--line);vertical-align:middle}.asset-table tr:last-child td{border-bottom:0}.asset-table tbody tr{cursor:pointer}.asset-table tbody tr:hover,.asset-table tbody tr:focus{background:var(--blue-soft)}.asset-table td strong,.asset-table td small{display:block}.asset-table td small{color:var(--muted);font-size:10px}.state-pill{display:inline-flex;padding:3px 7px;border:1px solid var(--line-strong);border-radius:10px;background:var(--panel-2);color:#c8d0db;font-size:10px}.state-absorbing{border-color:rgba(94,139,255,.4);color:#adc3ff}.state-rejecting{border-color:rgba(228,174,86,.4);color:#e6c080}.score-value{font:650 13px/1 ui-monospace,SFMono-Regular,Menlo,monospace}.sparkline{display:block;width:150px;height:46px}.sparkline line{stroke:#333d4c}.sparkline polyline{fill:none;stroke:var(--blue);stroke-width:2;vector-effect:non-scaling-stroke}.sparkline circle{fill:var(--panel);stroke:var(--blue);stroke-width:2}.row-open{width:24px;height:24px;padding:0;border:0;background:none;color:var(--faint);font-size:19px}.no-data{color:var(--faint);font-size:11px}
.chart-grid{display:grid;grid-template-columns:1fr;gap:9px;margin-top:15px}.chart{padding:12px 14px;border:1px solid var(--line);border-radius:var(--radius);background:var(--panel)}.chart-head{display:flex;justify-content:space-between;gap:18px}.chart-head span{color:var(--muted);font-variant-numeric:tabular-nums}.chart svg{display:block;width:100%;height:150px}.chart-grid line{stroke:var(--line-strong)}.chart-line{fill:none;stroke-width:2.4;vector-effect:non-scaling-stroke}.chart-line.blue{stroke:var(--blue)}.chart-line.amber{stroke:var(--amber)}.chart-line.gray{stroke:#9aa4b3}.chart circle{fill:var(--panel);stroke-width:2}.chart .blue~circle{stroke:var(--blue)}.chart .amber~circle{stroke:var(--amber)}.chart .gray~circle{stroke:#9aa4b3}.chart-dates{display:flex;justify-content:space-between;color:var(--faint);font-size:10px}.data-table{margin-top:7px;color:var(--muted);font-size:11px}.data-table summary{cursor:pointer}.data-table table{max-width:320px;margin-top:6px}.data-table td{padding:3px 8px 3px 0;border-bottom:1px solid var(--line)}.inline-notice{margin-top:12px;padding:8px 10px;border:1px solid rgba(228,174,86,.26);border-radius:6px;background:var(--amber-soft);color:#d7ba86;font-size:11px}
.theme-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:15px}.theme-card{padding:16px}.theme-card-head{display:flex;justify-content:space-between;gap:12px;color:var(--muted);font-size:11px}.theme-card-head strong{color:#afbdd0;font-size:10px}.theme-card h3{margin:10px 0 5px}.theme-card>p{min-height:44px;margin-bottom:12px;color:var(--muted)}.theme-list{display:grid;gap:5px}.theme-open{display:flex;justify-content:space-between;align-items:center;gap:12px;width:100%;padding:8px 9px;border:1px solid var(--line);border-radius:5px;background:var(--panel-2);text-align:left;cursor:pointer}.theme-open:hover{border-color:var(--blue);background:var(--blue-soft)}.theme-open small{color:var(--muted);font-size:10px}
.event-list{display:grid;gap:0;margin:15px 0 0;padding:0;border:1px solid var(--line);border-radius:var(--radius);background:var(--panel);list-style:none}.event-list>li{display:grid;grid-template-columns:110px 1fr;gap:16px;padding:13px 14px;border-bottom:1px solid var(--line)}.event-list>li:last-child{border-bottom:0}.event-list time{color:var(--muted);font-variant-numeric:tabular-nums}.event-list p{margin:3px 0 0;color:var(--muted)}.event-list details{margin-top:7px;color:var(--muted);font-size:11px}.event-list summary{cursor:pointer}.event-list ul{padding-left:18px}.event-list code,.evidence-accordion code,.drawer-body code{display:block;color:var(--faint);font:10px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace}.empty-state{min-height:88px;display:grid;place-content:center;margin-top:14px;padding:18px;border:1px dashed var(--line-strong);border-radius:var(--radius);color:var(--muted);text-align:center}.empty-inline{color:var(--faint)}
.quality-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:15px}.quality-banner{padding:10px 12px;border:1px solid var(--line);border-radius:6px;background:var(--panel);color:var(--muted)}.quality-banner.attention{border-color:rgba(228,174,86,.35);background:var(--amber-soft);color:#dfbd80}.quality-banner.divergence{border-color:rgba(94,139,255,.35);background:var(--blue-soft);color:#b7c8f5}.evidence-accordion{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}.evidence-accordion details{padding:9px 11px;border:1px solid var(--line);border-radius:6px;background:var(--panel)}.evidence-accordion summary{cursor:pointer;font-weight:600}.evidence-accordion summary span{float:right;color:var(--muted)}.evidence-accordion ul{display:grid;gap:6px;margin:10px 0 3px;padding-left:18px;color:var(--muted)}footer{padding:0 0 20px;color:var(--faint);font-size:11px;text-align:center}
.drawer-backdrop{position:fixed;z-index:70;inset:0;background:rgba(3,6,10,.58)}.detail-drawer{position:fixed;z-index:80;top:0;right:0;width:min(420px,92vw);height:100vh;transform:translateX(102%);border-left:1px solid var(--line-strong);background:var(--panel);box-shadow:-18px 0 55px rgba(0,0,0,.36);transition:transform .18s ease}.detail-drawer.open{transform:translateX(0)}.detail-drawer>header{height:74px;display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--line)}.detail-drawer header span{color:var(--blue);font-size:10px;font-weight:700;letter-spacing:.1em}.detail-drawer header h2{margin:2px 0 0;font-size:17px}.detail-drawer header button{width:32px;height:32px;border:1px solid var(--line);border-radius:6px;background:var(--panel-2);font-size:20px}.drawer-body{height:calc(100vh - 74px);padding:16px;overflow:auto}.drawer-summary{margin:0 0 12px;color:var(--muted)}.drawer-metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:12px 0}.drawer-metrics div{padding:9px;border:1px solid var(--line);border-radius:6px;background:var(--panel-2)}.drawer-metrics span,.drawer-metrics strong{display:block}.drawer-metrics span{color:var(--muted);font-size:10px}.drawer-metrics strong{margin-top:3px;font-variant-numeric:tabular-nums}.drawer-body details{margin-top:7px;padding:9px;border:1px solid var(--line);border-radius:6px;background:var(--panel-2)}.drawer-body summary{cursor:pointer;font-weight:600}.drawer-body ul{display:grid;gap:7px;padding-left:18px;color:var(--muted)}.drawer-note{margin-top:12px;padding:9px;border-left:2px solid var(--amber);background:var(--amber-soft);color:#d7ba86;font-size:11px}
@media(max-width:1180px){.status-strip{grid-template-columns:repeat(2,minmax(180px,1fr))}.status-strip div:nth-child(2){border-right:0}.status-strip div:nth-child(-n+2){border-bottom:1px solid var(--line)}.answer-grid,.layer-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:900px){:root{--header:68px}.topbar{padding:9px 12px}.nav-toggle{display:block}.brand{min-width:155px}.brand small{display:none}.status-strip div:nth-child(n+3){display:none}.layout{display:block}.side-nav{position:fixed;z-index:60;top:var(--header);bottom:0;left:0;width:240px;height:auto;transform:translateX(-102%);box-shadow:14px 0 40px rgba(0,0,0,.4);transition:transform .18s ease}.side-nav.open{transform:translateX(0)}main{padding:22px 20px 50px}.source-axis,.source-row{grid-template-columns:190px minmax(230px,1fr) 80px}}
@media(max-width:700px){body{font-size:13px}.topbar{gap:8px}.brand{min-width:120px}.brand-mark{width:30px;height:30px}.status-strip{grid-template-columns:1fr}.status-strip div{display:none!important}.status-strip div:first-child{display:block!important;border:0!important}.status-strip span,.status-strip strong{white-space:normal}main{padding:18px 12px 44px}.overview-heading,.section-heading{display:block}.text-link{display:inline-block;margin-top:8px}.answer-grid,.layer-grid,.theme-grid,.quality-grid,.evidence-accordion{grid-template-columns:1fr}.answer-card,.layer-grid article{padding:12px}.source-chart{overflow-x:auto}.source-axis,.source-row{min-width:620px}.chart{padding:10px 8px}.chart svg{height:135px}.event-list>li{grid-template-columns:1fr;gap:3px}.theme-card>p{min-height:0}.detail-drawer{top:auto;bottom:0;width:100%;height:min(78vh,700px);transform:translateY(102%);border-top:1px solid var(--line-strong);border-left:0;border-radius:10px 10px 0 0}.detail-drawer.open{transform:translateY(0)}.drawer-body{height:calc(min(78vh,700px) - 66px)}.detail-drawer>header{height:66px}.drawer-metrics{grid-template-columns:1fr 1fr}}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}.detail-drawer,.side-nav{transition:none}}
"""


DASHBOARD_SCRIPT = r"""
const dashboardData = JSON.parse(document.getElementById('dashboard-data').textContent);
const drawer = document.getElementById('detail-drawer');
const drawerBackdrop = document.getElementById('drawer-backdrop');
const drawerBody = document.getElementById('drawer-body');
const drawerTitle = document.getElementById('drawer-title');
const drawerKicker = document.getElementById('drawer-kicker');
const drawerClose = document.getElementById('drawer-close');
let returnFocus = null;

function textNode(tag, text, className) {
  const node = document.createElement(tag);
  node.textContent = text;
  if (className) node.className = className;
  return node;
}

function evidenceDetails(title, items, emptyText, open) {
  const details = document.createElement('details');
  details.open = Boolean(open);
  details.appendChild(textNode('summary', `${title} · ${items.length}`));
  const list = document.createElement('ul');
  if (!items.length) list.appendChild(textNode('li', emptyText));
  items.forEach(item => {
    const li = document.createElement('li');
    li.append(textNode('span', item.label), textNode('code', item.code));
    list.appendChild(li);
  });
  details.appendChild(list);
  return details;
}

function metric(label, value) {
  const node = document.createElement('div');
  node.append(textNode('span', label), textNode('strong', value));
  return node;
}

function openDrawer(title, kicker, content, source) {
  returnFocus = source;
  drawerTitle.textContent = title;
  drawerKicker.textContent = kicker;
  drawerBody.replaceChildren(...content);
  drawer.hidden = false;
  drawerBackdrop.hidden = false;
  drawer.classList.add('open');
  drawer.setAttribute('aria-hidden', 'false');
  drawerClose.focus();
}

function closeDrawer() {
  drawer.classList.remove('open');
  drawer.setAttribute('aria-hidden', 'true');
  drawerBackdrop.hidden = true;
  if (returnFocus) returnFocus.focus();
}

function openAsset(index, source) {
  const item = dashboardData.assets[index];
  const summary = textNode('p', `${item.symbol} · ${item.state} · 规则证据完整度 ${Math.round(item.confidence * 100)}%`, 'drawer-summary');
  const metrics = document.createElement('div');
  metrics.className = 'drawer-metrics';
  metrics.append(metric('吸收评分', `${item.score >= 0 ? '+' : ''}${item.score.toFixed(1)}`));
  metrics.append(metric('可比期变化', item.change === null ? '暂无' : `${item.change >= 0 ? '+' : ''}${item.change.toFixed(1)}`));
  item.components.forEach(component => metrics.append(metric(component.label, `${component.value >= 0 ? '+' : ''}${component.value.toFixed(1)}`)));
  const history = evidenceDetails('逐日趋势', item.history.map(point => ({label: `${point.date} · ${point.value >= 0 ? '+' : ''}${point.value.toFixed(1)}`, code: ''})), '暂无历史趋势', false);
  const note = textNode('p', `口径：${item.measurementNote} 该评分不是实际 ETF 申赎或真实资金净流入。`, 'drawer-note');
  openDrawer(item.label, '资产传导详情', [summary, metrics, evidenceDetails('支持信号', item.support, '暂无支持信号', true), evidenceDetails('冲突信号', item.conflicts, '暂无冲突信号', false), history, note], source);
}

function openTheme(index, source) {
  const item = dashboardData.themes[index];
  const summary = textNode('p', item.summary, 'drawer-summary');
  const metrics = document.createElement('div');
  metrics.className = 'drawer-metrics';
  metrics.append(metric('时间层级', item.horizon), metric('证据完整度', `${Math.round(item.confidence * 100)}%`), metric('规则确认', item.confirmation));
  openDrawer(item.label, `市场主题 · ${item.family}`, [summary, metrics, evidenceDetails('支持证据', item.support, '暂无支持证据', true), evidenceDetails('反证', item.conflicts, '暂无反证', false), evidenceDetails('失效条件', item.invalidation, '暂无失效条件', false)], source);
}

document.querySelectorAll('[data-asset-index]').forEach(row => {
  const activate = () => openAsset(Number(row.dataset.assetIndex), row);
  row.addEventListener('click', activate);
  row.addEventListener('keydown', event => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      activate();
    }
  });
});
document.querySelectorAll('[data-theme-index]').forEach(button => button.addEventListener('click', () => openTheme(Number(button.dataset.themeIndex), button)));
drawerClose.addEventListener('click', closeDrawer);
drawerBackdrop.addEventListener('click', closeDrawer);
document.addEventListener('keydown', event => { if (event.key === 'Escape' && drawer.classList.contains('open')) closeDrawer(); });

const assetTable = document.getElementById('asset-table');
assetTable.querySelectorAll('th button[data-sort]').forEach(button => button.addEventListener('click', () => {
  const key = button.dataset.sort;
  const th = button.closest('th');
  const direction = th.getAttribute('aria-sort') === 'descending' ? 'ascending' : 'descending';
  assetTable.querySelectorAll('th').forEach(cell => cell.removeAttribute('aria-sort'));
  th.setAttribute('aria-sort', direction);
  const rows = Array.from(assetTable.tBodies[0].rows);
  const numericKeys = new Set(['score', 'change', 'confidence']);
  rows.sort((left, right) => {
    const a = left.dataset[key];
    const b = right.dataset[key];
    if (a === '') return 1;
    if (b === '') return -1;
    const comparison = numericKeys.has(key) ? Number(a) - Number(b) : a.localeCompare(b, 'zh-CN');
    return direction === 'ascending' ? comparison : -comparison;
  });
  rows.forEach(row => assetTable.tBodies[0].appendChild(row));
}));

const nav = document.getElementById('side-nav');
const navToggle = document.getElementById('nav-toggle');
navToggle.addEventListener('click', () => {
  const isOpen = nav.classList.toggle('open');
  navToggle.setAttribute('aria-expanded', String(isOpen));
});
nav.querySelectorAll('a').forEach(link => link.addEventListener('click', () => {
  nav.classList.remove('open');
  navToggle.setAttribute('aria-expanded', 'false');
}));

const navLinks = Array.from(nav.querySelectorAll('a'));
const sections = navLinks.map(link => document.querySelector(link.getAttribute('href'))).filter(Boolean);
const observer = new IntersectionObserver(entries => {
  const visible = entries.filter(entry => entry.isIntersecting).sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];
  if (!visible) return;
  navLinks.forEach(link => link.classList.toggle('active', link.getAttribute('href') === `#${visible.target.id}`));
}, {rootMargin: '-18% 0px -70% 0px', threshold: [0, 0.15]});
sections.forEach(section => observer.observe(section));
"""
