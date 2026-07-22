from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Callable, Sequence

from domain.macro import MacroSnapshot, SeriesFeature
from domain.market_theme import MarketThemeState, ThemeCandidate, ThemeFamily, ThemeHorizon


MARKET_THEME_MODEL_VERSION = "market-theme-v1.0.0"


@dataclass(frozen=True, slots=True)
class _Check:
    code: str
    passed: bool
    value: float | None

    @property
    def available(self) -> bool:
        return self.value is not None

    def evidence(self) -> str:
        return self.code if self.value is None else f"{self.code}={self.value:+.2f}"


@dataclass(frozen=True, slots=True)
class _ThemeRule:
    theme_id: str
    label: str
    family: ThemeFamily
    priority: int
    summary: str
    invalidation_conditions: tuple[str, ...]
    evaluate: Callable[[dict[str, SeriesFeature], MacroSnapshot], tuple[list[_Check], list[_Check], list[_Check]]]
    minimum_confirmations: int = 1


def _first(features: dict[str, SeriesFeature], *series_ids: str) -> SeriesFeature | None:
    return next((features[series_id] for series_id in series_ids if series_id in features), None)


def _z(features: dict[str, SeriesFeature], *series_ids: str) -> float | None:
    feature = _first(features, *series_ids)
    return None if feature is None or feature.z_change_5d_252 is None else float(feature.z_change_5d_252)


def _check(code: str, value: float | None, predicate: Callable[[float], bool]) -> _Check:
    return _Check(code, value is not None and predicate(value), value)


def _metric(code: str, value: float, predicate: Callable[[float], bool]) -> _Check:
    return _Check(code, predicate(value), value)


def _difference(features: dict[str, SeriesFeature], left: tuple[str, ...], right: tuple[str, ...]) -> float | None:
    left_value, right_value = _z(features, *left), _z(features, *right)
    return None if left_value is None or right_value is None else left_value - right_value


def _target_scores(snapshot: MacroSnapshot) -> dict[str, float]:
    return {item.target_id: item.absorption_score for item in snapshot.liquidity_target_flows}


def _target_dispersion(snapshot: MacroSnapshot) -> float:
    scores = list(_target_scores(snapshot).values())
    return max(scores) - min(scores) if scores else 0.0


def _rules() -> tuple[_ThemeRule, ...]:
    return (
        _ThemeRule(
            "REAL_RATE_TIGHTENING", "真实利率约束增强", ThemeFamily.RATES_INFLATION, 60,
            "真实利率正在上升，长久期资产面临更强的折现率约束。",
            ("DFII10_5D_Z_BELOW_0_25", "RATE_PRESSURE_SCORE_BELOW_50"),
            lambda f, s: (
                [_metric("RATE_PRESSURE_SCORE_AT_LEAST_60", s.rate_pressure_score, lambda v: v >= 60),
                 _check("DFII10_5D_Z_AT_LEAST_0_5", _z(f, "DFII10"), lambda v: v >= 0.5)],
                [_check("DGS10_5D_Z_POSITIVE", _z(f, "DGS10"), lambda v: v >= 0.25),
                 _check("GOLD_5D_Z_NEGATIVE", _z(f, "GLD"), lambda v: v <= -0.25),
                 _check("QQQ_5D_Z_NEGATIVE", _z(f, "QQQ"), lambda v: v <= -0.25)],
                [_check("GOLD_REAL_RATE_DIVERGENCE", _z(f, "GLD"), lambda v: v >= 0.5)],
            ),
        ),
        _ThemeRule(
            "INFLATION_REPRICING", "通胀补偿重新定价", ThemeFamily.RATES_INFLATION, 55,
            "盈亏平衡通胀与名义利率同步抬升，商品用于确认通胀而非纯真实利率冲击。",
            ("T10YIE_5D_Z_BELOW_0_25", "COMMODITY_CONFIRMATION_REVERSES"),
            lambda f, s: (
                [_check("T10YIE_5D_Z_AT_LEAST_0_5", _z(f, "T10YIE"), lambda v: v >= 0.5),
                 _check("DGS10_5D_Z_POSITIVE", _z(f, "DGS10"), lambda v: v >= 0.25)],
                [_check("OIL_5D_Z_POSITIVE", _z(f, "USO"), lambda v: v >= 0.5),
                 _check("COPPER_5D_Z_POSITIVE", _z(f, "CPER"), lambda v: v >= 0.5),
                 _check("GOLD_5D_Z_RESILIENT", _z(f, "GLD"), lambda v: v >= -0.25)],
                [_check("REAL_RATE_DOMINATES_BREAKEVEN", _difference(f, ("DFII10",), ("T10YIE",)), lambda v: v >= 0.75)],
            ),
        ),
        _ThemeRule(
            "TERM_PREMIUM_PRESSURE", "期限溢价与长端供给压力", ThemeFamily.RATES_INFLATION, 58,
            "长端利率显著跑赢短端，市场更像在交易久期和供给压力。",
            ("DGS30_5D_Z_BELOW_0_25", "LONG_SHORT_RATE_GAP_CLOSES"),
            lambda f, s: (
                [_check("DGS30_5D_Z_AT_LEAST_0_5", _z(f, "DGS30"), lambda v: v >= 0.5),
                 _check("DGS30_MINUS_DGS2_Z_AT_LEAST_0_25", _difference(f, ("DGS30",), ("DGS2",)), lambda v: v >= 0.25)],
                [_check("TLT_5D_Z_NEGATIVE", _z(f, "TLT"), lambda v: v <= -0.5),
                 _check("GOLD_NOT_SELLING_OFF", _z(f, "GLD"), lambda v: v >= -0.25),
                 _check("DXY_NOT_SURGING", _z(f, "DXY", "DXY_PROXY", "UUP"), lambda v: abs(v) < 0.75)],
                [_check("SHORT_END_LEADS", _difference(f, ("DGS2",), ("DGS30",)), lambda v: v >= 0.25)],
            ),
        ),
        _ThemeRule(
            "LIQUIDITY_EASING_TRANSMISSION", "流动性宽松正在传导", ThemeFamily.EASING_DEFENSIVE, 48,
            "系统流动性扩张且风险约束温和，价格开始确认资金向风险资产传导。",
            ("LIQUIDITY_SCORE_BELOW_20", "RISK_SCORE_AT_LEAST_50"),
            lambda f, s: (
                [_metric("LIQUIDITY_SCORE_AT_LEAST_20", s.liquidity_score, lambda v: v >= 20),
                 _metric("RISK_SCORE_BELOW_40", s.risk_score, lambda v: v < 40)],
                [_check("SPY_5D_Z_POSITIVE", _z(f, "SPY"), lambda v: v >= 0.25),
                 _check("IWM_5D_Z_POSITIVE", _z(f, "IWM"), lambda v: v >= 0.25),
                 _check("RSP_5D_Z_POSITIVE", _z(f, "RSP"), lambda v: v >= 0.25)],
                [_check("DXY_5D_Z_SURGING", _z(f, "DXY", "DXY_PROXY", "UUP"), lambda v: v >= 1.0)],
            ),
        ),
        _ThemeRule(
            "GROWTH_SCARE", "增长担忧扩散", ThemeFamily.EASING_DEFENSIVE, 70,
            "利率和股票同步下行，周期商品走弱时更接近增长冲击而非利好式降息。",
            ("DGS10_5D_Z_ABOVE_NEG_0_25", "SPY_5D_Z_ABOVE_NEG_0_25"),
            lambda f, s: (
                [_check("DGS10_5D_Z_BELOW_NEG_0_5", _z(f, "DGS10"), lambda v: v <= -0.5),
                 _check("SPY_5D_Z_BELOW_NEG_0_5", _z(f, "SPY"), lambda v: v <= -0.5)],
                [_check("OIL_5D_Z_NEGATIVE", _z(f, "USO"), lambda v: v <= -0.5),
                 _check("COPPER_5D_Z_NEGATIVE", _z(f, "CPER"), lambda v: v <= -0.5),
                 _check("IWM_5D_Z_NEGATIVE", _z(f, "IWM"), lambda v: v <= -0.5)],
                [_check("GOLD_AND_OIL_RISE_TOGETHER", _z(f, "USO"), lambda v: v >= 0.5)],
            ),
        ),
        _ThemeRule(
            "GEOPOLITICAL_HEDGE", "事件型商品与避险对冲", ThemeFamily.EASING_DEFENSIVE, 72,
            "黄金和能源同步走强，风险资产承压时更像事件冲击而非普通增长放缓。",
            ("OIL_5D_Z_BELOW_0_25", "GOLD_5D_Z_BELOW_0_25"),
            lambda f, s: (
                [_check("GOLD_5D_Z_AT_LEAST_0_5", _z(f, "GLD"), lambda v: v >= 0.5),
                 _check("OIL_5D_Z_AT_LEAST_0_5", _z(f, "USO"), lambda v: v >= 0.5)],
                [_check("SPY_5D_Z_NEGATIVE", _z(f, "SPY"), lambda v: v <= -0.25),
                 _check("DGS10_5D_Z_NON_POSITIVE", _z(f, "DGS10"), lambda v: v <= 0),
                 _check("DXY_5D_Z_POSITIVE", _z(f, "DXY", "DXY_PROXY", "UUP"), lambda v: v >= 0.25)],
                [_check("COPPER_GROWTH_CONFIRMATION", _z(f, "CPER"), lambda v: v >= 0.75)],
            ),
        ),
        _ThemeRule(
            "USD_FUNDING_STRESS", "美元融资压力覆盖", ThemeFamily.STRESS_OVERRIDE, 120,
            "美元异常走强且股票、黄金同步走弱，现金需求开始覆盖普通资产关系。",
            ("DXY_5D_Z_BELOW_0_5", "GOLD_AND_SPY_STABILIZE"),
            lambda f, s: (
                [_check("DXY_5D_Z_AT_LEAST_1", _z(f, "DXY", "DXY_PROXY", "UUP"), lambda v: v >= 1.0),
                 _check("SPY_5D_Z_BELOW_NEG_0_5", _z(f, "SPY"), lambda v: v <= -0.5),
                 _check("GOLD_5D_Z_NEGATIVE", _z(f, "GLD"), lambda v: v <= -0.25)],
                [_metric("RISK_SCORE_AT_LEAST_60", s.risk_score, lambda v: v >= 60),
                 _check("BTC_5D_Z_NEGATIVE", _z(f, "IBIT", "BTC"), lambda v: v <= -0.5),
                 _check("CREDIT_SPREAD_5D_Z_POSITIVE", _z(f, "BAMLC0A0CM"), lambda v: v >= 0.5)],
                [_check("GOLD_5D_Z_POSITIVE", _z(f, "GLD"), lambda v: v >= 0.25)],
            ),
        ),
        _ThemeRule(
            "CARRY_UNWIND", "套息与杠杆平仓", ThemeFamily.STRESS_OVERRIDE, 110,
            "日元代理异常走强且科技资产领跌，表现符合融资货币反转后的去杠杆。",
            ("JPY_5D_Z_BELOW_0_5", "QQQ_RELATIVE_WEAKNESS_REVERSES"),
            lambda f, s: (
                [_check("JPY_5D_Z_AT_LEAST_1", _z(f, "FXY"), lambda v: v >= 1.0),
                 _check("QQQ_5D_Z_BELOW_NEG_0_5", _z(f, "QQQ"), lambda v: v <= -0.5)],
                [_check("QQQ_MINUS_SPY_Z_NEGATIVE", _difference(f, ("QQQ",), ("SPY",)), lambda v: v <= -0.25),
                 _check("BTC_5D_Z_NEGATIVE", _z(f, "IBIT", "BTC"), lambda v: v <= -0.5),
                 _metric("RISK_SCORE_AT_LEAST_50", s.risk_score, lambda v: v >= 50)],
                [_check("JPY_AND_QQQ_RISE_TOGETHER", _z(f, "QQQ"), lambda v: v >= 0.25)],
            ),
        ),
        _ThemeRule(
            "BROAD_RISK_ON", "风险偏好广泛回升", ThemeFamily.EQUITY_INTERNALS, 45,
            "大盘上涨并得到小盘、等权或周期资产确认，风险偏好具有宽度。",
            ("SPY_5D_Z_BELOW_0_25", "BREADTH_CONFIRMATION_FAILS"),
            lambda f, s: (
                [_check("SPY_5D_Z_AT_LEAST_0_5", _z(f, "SPY"), lambda v: v >= 0.5),
                 _metric("RISK_SCORE_BELOW_40", s.risk_score, lambda v: v < 40)],
                [_check("IWM_5D_Z_AT_LEAST_0_5", _z(f, "IWM"), lambda v: v >= 0.5),
                 _check("RSP_5D_Z_AT_LEAST_0_5", _z(f, "RSP"), lambda v: v >= 0.5),
                 _check("COPPER_5D_Z_POSITIVE", _z(f, "CPER"), lambda v: v >= 0.25)],
                [_check("QQQ_ONLY_RALLY", _difference(f, ("QQQ",), ("IWM",)), lambda v: v >= 1.0)],
            ),
        ),
        _ThemeRule(
            "TECH_CONCENTRATION", "科技集中度上升", ThemeFamily.EQUITY_INTERNALS, 52,
            "纳指显著跑赢大盘，而小盘或等权缺乏确认，指数强势集中在少数久期资产。",
            ("QQQ_MINUS_SPY_Z_BELOW_0_25", "BREADTH_CATCHES_UP"),
            lambda f, s: (
                [_check("QQQ_5D_Z_AT_LEAST_0_5", _z(f, "QQQ"), lambda v: v >= 0.5),
                 _check("QQQ_MINUS_SPY_Z_AT_LEAST_0_5", _difference(f, ("QQQ",), ("SPY",)), lambda v: v >= 0.5)],
                [_check("IWM_5D_Z_NOT_CONFIRMING", _z(f, "IWM"), lambda v: v <= 0.25),
                 _check("RSP_5D_Z_NOT_CONFIRMING", _z(f, "RSP"), lambda v: v <= 0.25),
                 _check("SOXX_5D_Z_POSITIVE", _z(f, "SOXX"), lambda v: v >= 0.25)],
                [_check("SMALL_CAP_CATCH_UP", _z(f, "IWM"), lambda v: v >= 0.75)],
            ),
        ),
        _ThemeRule(
            "INTERNAL_ROTATION", "股票内部轮动而非撤退", ThemeFamily.EQUITY_INTERNALS, 50,
            "科技走弱但大盘和更广市场保持稳定，更接近换仓而非全面风险撤离。",
            ("SPY_5D_Z_BELOW_NEG_0_5", "BREADTH_TURNS_NEGATIVE"),
            lambda f, s: (
                [_check("QQQ_5D_Z_BELOW_NEG_0_5", _z(f, "QQQ"), lambda v: v <= -0.5),
                 _check("SPY_5D_Z_AT_LEAST_NEG_0_25", _z(f, "SPY"), lambda v: v >= -0.25)],
                [_check("RSP_5D_Z_RESILIENT", _z(f, "RSP"), lambda v: v >= -0.25),
                 _check("IWM_5D_Z_RESILIENT", _z(f, "IWM"), lambda v: v >= -0.25),
                 _check("KRE_5D_Z_RESILIENT", _z(f, "KRE"), lambda v: v >= -0.25)],
                [_check("SPY_5D_Z_BREAKS_DOWN", _z(f, "SPY"), lambda v: v <= -0.5)],
            ),
        ),
        _ThemeRule(
            "BROAD_RISK_OFF", "风险回避全面扩散", ThemeFamily.EQUITY_INTERNALS, 82,
            "大盘与科技同步显著下跌，并得到小盘、信用或美元压力确认。",
            ("SPY_OR_QQQ_RETURNS_TO_NEUTRAL",),
            lambda f, s: (
                [_check("SPY_5D_Z_BELOW_NEG_0_5", _z(f, "SPY"), lambda v: v <= -0.5),
                 _check("QQQ_5D_Z_BELOW_NEG_0_5", _z(f, "QQQ"), lambda v: v <= -0.5)],
                [_check("IWM_5D_Z_BELOW_NEG_0_5", _z(f, "IWM"), lambda v: v <= -0.5),
                 _check("BTC_5D_Z_BELOW_NEG_0_5", _z(f, "IBIT", "BTC"), lambda v: v <= -0.5),
                 _metric("RISK_SCORE_AT_LEAST_60", s.risk_score, lambda v: v >= 60)],
                [_check("GOLD_AND_TREASURY_RALLY", _z(f, "GLD"), lambda v: v >= 0.75)],
            ),
        ),
        _ThemeRule(
            "GOLD_REAL_RATE_DIVERGENCE", "黄金无视真实利率", ThemeFamily.DIVERGENCE, 65,
            "真实利率与黄金同步显著上升，传统负相关失效，可能存在财政或信用对冲需求。",
            ("GOLD_5D_Z_BELOW_0_25", "DFII10_5D_Z_BELOW_0_25"),
            lambda f, s: (
                [_check("DFII10_5D_Z_AT_LEAST_0_5", _z(f, "DFII10"), lambda v: v >= 0.5),
                 _check("GOLD_5D_Z_AT_LEAST_0_5", _z(f, "GLD"), lambda v: v >= 0.5)],
                [_check("DGS30_5D_Z_POSITIVE", _z(f, "DGS30"), lambda v: v >= 0.5),
                 _check("DXY_5D_Z_NOT_SURGING", _z(f, "DXY", "DXY_PROXY", "UUP"), lambda v: v <= 0.5),
                 _metric("RATE_PRESSURE_SCORE_AT_LEAST_60", s.rate_pressure_score, lambda v: v >= 60)],
                [_check("GOLD_CAPITULATION", _z(f, "GLD"), lambda v: v <= -0.5)],
            ),
        ),
        _ThemeRule(
            "SELECTIVE_LIQUIDITY_TRANSMISSION", "流动性选择性传导", ThemeFamily.DIVERGENCE, 62,
            "系统流动性偏宽松，但不同资产吸收能力明显分化，不能等同于全面风险偏好。",
            ("LIQUIDITY_SCORE_BELOW_20", "TARGET_DISPERSION_BELOW_20"),
            lambda f, s: (
                [_metric("LIQUIDITY_SCORE_AT_LEAST_20", s.liquidity_score, lambda v: v >= 20),
                 _metric("TARGET_ABSORPTION_DISPERSION_AT_LEAST_30", _target_dispersion(s), lambda v: v >= 30)],
                [_metric("AI_ABSORPTION_NEGATIVE", _target_scores(s).get("AI_SEMICONDUCTOR", 0.0), lambda v: v < 0),
                 _metric("BANKS_LEAD_AI_BY_15", _target_scores(s).get("US_BANKS_CREDIT", 0.0) - _target_scores(s).get("AI_SEMICONDUCTOR", 0.0), lambda v: v >= 15),
                 _metric("LARGE_CAP_LEADS_AI_BY_15", _target_scores(s).get("US_LARGE_CAP", 0.0) - _target_scores(s).get("AI_SEMICONDUCTOR", 0.0), lambda v: v >= 15)],
                [_metric("TARGETS_CONVERGING", _target_dispersion(s), lambda v: v < 20)],
            ),
        ),
        _ThemeRule(
            "CRYPTO_IDIOSYNCRATIC", "加密资产独立行情", ThemeFamily.DIVERGENCE, 30,
            "加密资产出现异常波动，但传统宏观资产保持安静，暂不升级为系统性主题。",
            ("OTHER_ASSETS_BEGIN_CROSS_ASSET_CONFIRMATION",),
            lambda f, s: (
                [_check("BTC_ABS_5D_Z_AT_LEAST_1_5", _z(f, "IBIT", "BTC"), lambda v: abs(v) >= 1.5),
                 _check("SPY_ABS_5D_Z_BELOW_0_5", _z(f, "SPY"), lambda v: abs(v) < 0.5)],
                [_check("DXY_ABS_5D_Z_BELOW_0_5", _z(f, "DXY", "DXY_PROXY", "UUP"), lambda v: abs(v) < 0.5),
                 _check("GOLD_ABS_5D_Z_BELOW_0_5", _z(f, "GLD"), lambda v: abs(v) < 0.5),
                 _metric("RATE_PRESSURE_SCORE_BELOW_50", s.rate_pressure_score, lambda v: v < 50)],
                [_metric("SYSTEM_RISK_ELEVATED", s.risk_score, lambda v: v >= 60)],
            ),
        ),
    )


def _candidate(rule: _ThemeRule, features: dict[str, SeriesFeature], snapshot: MacroSnapshot) -> ThemeCandidate | None:
    triggers, confirmations, conflicts = rule.evaluate(features, snapshot)
    if not triggers or not all(item.passed for item in triggers):
        return None
    passed_confirmations = [item for item in confirmations if item.passed]
    if len(passed_confirmations) < rule.minimum_confirmations:
        return None
    passed_conflicts = [item for item in conflicts if item.passed]
    all_checks = triggers + confirmations + conflicts
    coverage = sum(item.available for item in all_checks) / max(1, len(all_checks))
    confirmation_ratio = len(passed_confirmations) / max(1, len(confirmations))
    activation_strength = sum(item.passed for item in triggers) / len(triggers)
    conflict_penalty = len(passed_conflicts) / max(1, len(conflicts))
    confidence = max(0.0, min(1.0, 0.35 * activation_strength + 0.35 * confirmation_ratio + 0.15 * coverage + 0.05 - 0.10 * conflict_penalty))
    return ThemeCandidate(
        theme_id=rule.theme_id, label=rule.label, family=rule.family, horizon=ThemeHorizon.FAST,
        priority=rule.priority, confidence=confidence, activation_strength=activation_strength,
        confirmation_count=len(passed_confirmations), confirmation_total=len(confirmations),
        persistence_periods=1, data_coverage=coverage,
        supporting_evidence=tuple(item.evidence() for item in triggers + passed_confirmations),
        conflicting_evidence=tuple(item.evidence() for item in passed_conflicts),
        invalidation_conditions=rule.invalidation_conditions, summary=rule.summary,
    )


def _rank(candidates: Sequence[ThemeCandidate]) -> list[ThemeCandidate]:
    return sorted(
        candidates,
        key=lambda item: (item.family is ThemeFamily.STRESS_OVERRIDE, item.confidence, item.priority, item.confirmation_count),
        reverse=True,
    )


def _strongest_signals(features: dict[str, SeriesFeature], limit: int = 5) -> tuple[str, ...]:
    values = [
        (series_id, float(feature.z_change_5d_252))
        for series_id, feature in features.items()
        if feature.z_change_5d_252 is not None and "_" not in series_id
    ]
    values.sort(key=lambda item: abs(item[1]), reverse=True)
    return tuple(f"{series_id}={value:+.2f}z" for series_id, value in values[:limit])


def evaluate_fast_market_themes(
    features: dict[str, SeriesFeature],
    snapshot: MacroSnapshot,
) -> MarketThemeState:
    candidates = _rank([candidate for rule in _rules() if (candidate := _candidate(rule, features, snapshot)) is not None])
    dominant = candidates[0] if candidates else None
    available_z = sum(feature.z_change_5d_252 is not None for feature in features.values())
    no_dominant = None
    if dominant is None:
        no_dominant = (
            "INSUFFICIENT_STANDARDIZED_MARKET_DATA" if available_z < 5
            else "NO_CROSS_ASSET_THEME_PASSED_TRIGGER_AND_CONFIRMATION"
        )
    return MarketThemeState(
        as_of=snapshot.as_of, valid_until=snapshot.valid_until, horizon=ThemeHorizon.FAST,
        dominant_theme_id=dominant.theme_id if dominant else None,
        dominant_label=dominant.label if dominant else "无主导交易主题",
        summary=dominant.summary if dominant else "跨资产信号尚未形成足够共振。",
        confidence=dominant.confidence if dominant else 0.0,
        active_themes=tuple(candidates), strongest_signals=_strongest_signals(features),
        no_dominant_reason=no_dominant, model_version=MARKET_THEME_MODEL_VERSION,
    )


def _repricing_candidate(
    theme_id: str,
    label: str,
    family: ThemeFamily,
    summary: str,
    evidence: tuple[str, ...],
    invalidation: tuple[str, ...],
    magnitude: float,
    threshold: float,
    priority: int,
) -> ThemeCandidate:
    activation = min(1.0, abs(magnitude) / max(threshold, 1e-9))
    confidence = min(0.95, 0.50 + 0.40 * activation)
    return ThemeCandidate(
        theme_id=theme_id, label=label, family=family, horizon=ThemeHorizon.REPRICING,
        priority=priority, confidence=confidence, activation_strength=activation,
        confirmation_count=max(0, len(evidence) - 1), confirmation_total=max(1, len(evidence) - 1),
        persistence_periods=1, data_coverage=1.0, supporting_evidence=evidence,
        conflicting_evidence=(), invalidation_conditions=invalidation, summary=summary,
    )


def evaluate_repricing_market_themes(history_points: Sequence[Any]) -> MarketThemeState | None:
    if len(history_points) < 2:
        return None
    first, current = history_points[0], history_points[-1]
    candidates: list[ThemeCandidate] = []
    liquidity_delta = current.net_liquidity_20d_bn - first.net_liquidity_20d_bn
    risk_delta = current.risk_score - first.risk_score
    rate_delta = current.rate_pressure_score - first.rate_pressure_score
    if abs(liquidity_delta) >= 50:
        direction = "扩张" if liquidity_delta > 0 else "收缩"
        candidates.append(_repricing_candidate(
            "LIQUIDITY_ACCELERATION" if liquidity_delta > 0 else "LIQUIDITY_WITHDRAWAL",
            f"美元流动性加速{direction}", ThemeFamily.EASING_DEFENSIVE if liquidity_delta > 0 else ThemeFamily.STRESS_OVERRIDE,
            f"14日系统美元流动性变化达到 {liquidity_delta:+.1f}bn。",
            (f"NET_LIQUIDITY_CHANGE_14D={liquidity_delta:+.1f}BN",),
            ("NET_LIQUIDITY_CHANGE_REVERSES",), liquidity_delta, 50, 90 if liquidity_delta < 0 else 68,
        ))
    if abs(rate_delta) >= 15:
        rising = rate_delta > 0
        candidates.append(_repricing_candidate(
            "REAL_RATE_CONSTRAINT_REPRICING_UP" if rising else "REAL_RATE_CONSTRAINT_REPRICING_DOWN",
            "真实利率约束重新上升" if rising else "真实利率约束缓和", ThemeFamily.RATES_INFLATION,
            f"14日真实利率压力分数变化 {rate_delta:+.1f}。",
            (f"RATE_PRESSURE_CHANGE_14D={rate_delta:+.1f}",),
            ("RATE_PRESSURE_CHANGE_REVERSES",), rate_delta, 15, 65,
        ))
    if abs(risk_delta) >= 15:
        rising = risk_delta > 0
        candidates.append(_repricing_candidate(
            "RISK_REPRICING_UP" if rising else "RISK_REPRICING_DOWN",
            "风险约束上升" if rising else "风险约束缓和", ThemeFamily.STRESS_OVERRIDE if rising else ThemeFamily.EASING_DEFENSIVE,
            f"14日风险分数变化 {risk_delta:+.1f}。",
            (f"RISK_SCORE_CHANGE_14D={risk_delta:+.1f}",),
            ("RISK_SCORE_CHANGE_REVERSES",), risk_delta, 15, 80 if rising else 50,
        ))
    common_targets = set(first.target_absorption).intersection(current.target_absorption)
    target_changes = {target: current.target_absorption[target] - first.target_absorption[target] for target in common_targets}
    if target_changes:
        leader = max(target_changes, key=lambda key: abs(target_changes[key]))
        leader_change = target_changes[leader]
        if abs(leader_change) >= 20:
            candidates.append(_repricing_candidate(
                "TARGET_ROTATION_14D", "跨资产吸收结构轮动", ThemeFamily.DIVERGENCE,
                f"{leader} 的流动性吸收分数在14日内变化 {leader_change:+.1f}。",
                (f"TARGET_ROTATION={leader}", f"ABSORPTION_CHANGE={leader_change:+.1f}"),
                ("LEADING_TARGET_CHANGE_RETURNS_INSIDE_10",), leader_change, 20, 60,
            ))
    candidates = _rank(candidates)
    dominant = candidates[0] if candidates else None
    return MarketThemeState(
        as_of=current.as_of, valid_until=current.as_of + timedelta(days=1), horizon=ThemeHorizon.REPRICING,
        dominant_theme_id=dominant.theme_id if dominant else None,
        dominant_label=dominant.label if dominant else "14日无显著重定价主题",
        summary=dominant.summary if dominant else "14日窗口内没有变化超过物质性阈值。",
        confidence=dominant.confidence if dominant else 0.0,
        active_themes=tuple(candidates), strongest_signals=(),
        no_dominant_reason=None if dominant else "NO_MATERIAL_14D_REPRICING",
        model_version=MARKET_THEME_MODEL_VERSION,
    )


def _apply_persistence(state: MarketThemeState, history_points: Sequence[Any]) -> MarketThemeState:
    if not state.active_themes or not history_points:
        return state
    updated: list[ThemeCandidate] = []
    for candidate in state.active_themes:
        persistence = 0
        for point in reversed(history_points):
            active_ids = tuple(getattr(point, "active_theme_ids", ()))
            if candidate.theme_id not in active_ids:
                break
            persistence += 1
        persistence = max(1, persistence)
        confidence = min(0.95, candidate.confidence + 0.10 * min(1.0, persistence / 3))
        updated.append(replace(candidate, persistence_periods=persistence, confidence=confidence))
    ranked = _rank(updated)
    dominant = ranked[0]
    return replace(
        state, dominant_theme_id=dominant.theme_id, dominant_label=dominant.label,
        summary=dominant.summary, confidence=dominant.confidence, active_themes=tuple(ranked),
    )


def build_market_theme_states(
    features: dict[str, SeriesFeature],
    snapshot: MacroSnapshot,
    history_points: Sequence[Any] | None = None,
) -> tuple[MarketThemeState, ...]:
    history_points = history_points or ()
    fast = _apply_persistence(evaluate_fast_market_themes(features, snapshot), history_points)
    repricing = evaluate_repricing_market_themes(history_points)
    return (fast,) if repricing is None else (fast, repricing)
