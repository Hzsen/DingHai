from __future__ import annotations

import math

import pandas as pd


SCORE_VERSION = "cn-wave-market-behavior-v0.2.0"
TOTAL_RULES = 11


def _known(value: object) -> bool:
    return value is not None and not pd.isna(value)


def _truthy(value: object) -> bool:
    return _known(value) and bool(value)


def score_row(row: pd.Series) -> dict[str, object]:
    score = 0
    evaluated = 0
    components = {
        "price_breakout_component": 0,
        "volume_component": 0,
        "relative_strength_component": 0,
        "chip_component": 0,
        "exhaustion_component": 0,
    }
    reasons: list[str] = []
    risks: list[str] = []
    missing: list[str] = []

    def evaluate(field: str) -> bool:
        nonlocal evaluated
        value = row.get(field)
        if _known(value):
            evaluated += 1
            return True
        missing.append(field)
        return False

    if evaluate("rolling_high_120d") and float(row["close"]) >= float(row["rolling_high_120d"]) * 0.98:
        components["price_breakout_component"] += 2
        reasons.append("near_120d_high")
    if evaluate("rolling_high_250d") and float(row["close"]) >= float(row["rolling_high_250d"]) * 0.98:
        components["price_breakout_component"] += 1
        reasons.append("near_250d_high")

    if evaluate("amount_ratio_20d") and float(row["amount_ratio_20d"]) >= 2:
        components["volume_component"] += 2
        reasons.append("amount_expansion")
    if evaluate("amount_rank_market"):
        amount_rank = float(row["amount_rank_market"])
        if amount_rank <= 100:
            components["volume_component"] += 1
            reasons.append("top100_market_amount")
        if amount_rank <= 50:
            components["volume_component"] += 1
            reasons.append("top50_market_amount")

    if evaluate("rs_market_20d") and float(row["rs_market_20d"]) > 0:
        components["relative_strength_component"] += 1
        reasons.append("outperform_market")
    if evaluate("rs_industry_20d") and float(row["rs_industry_20d"]) > 0:
        components["relative_strength_component"] += 1
        reasons.append("outperform_industry")
    if evaluate("rs_rank_market_20d") and float(row["rs_rank_market_20d"]) <= 0.05:
        components["relative_strength_component"] += 2
        reasons.append("top5pct_relative_strength")

    if evaluate("base_turnover_sum_60d") and float(row["base_turnover_sum_60d"]) >= 1.0:
        components["chip_component"] += 1
        reasons.append("sufficient_base_turnover")
    if evaluate("overhead_supply_ratio") and float(row["overhead_supply_ratio"]) <= 0.2:
        components["chip_component"] += 1
        reasons.append("low_overhead_supply_proxy")

    if evaluate("high_volume_stall_flag") and _truthy(row["high_volume_stall_flag"]):
        components["exhaustion_component"] -= 2
        risks.append("high_volume_stall")
    upper_shadow_known = evaluate("upper_shadow_ratio")
    exhaustion_amount_known = _known(row.get("amount_ratio_20d"))
    if upper_shadow_known and exhaustion_amount_known:
        if float(row["upper_shadow_ratio"]) >= 0.08 and float(row["amount_ratio_20d"]) >= 2:
            components["exhaustion_component"] -= 1
            risks.append("high_volume_long_upper_shadow")

    score = sum(components.values())
    coverage = evaluated / TOTAL_RULES
    return_20d = row.get("return_20d")
    if _truthy(row.get("high_volume_stall_flag")) and _known(return_20d) and float(return_20d) > 0.5:
        stage = "exhaustion_risk"
    elif coverage < 0.65:
        stage = "insufficient_evidence"
    elif score >= 9:
        stage = "confirmed_main_uptrend"
    elif score >= 7:
        stage = "breakout_candidate"
    elif score >= 4:
        stage = "momentum_setup"
    else:
        stage = "normal"

    return {
        **components,
        "leader_score": int(score),
        "score_coverage": round(coverage, 4),
        "evaluated_rule_count": evaluated,
        "missing_components": "|".join(dict.fromkeys(missing)),
        "top_features": "|".join(reasons),
        "risk_flags": "|".join(risks),
        "stage_label": stage,
        "score_version": SCORE_VERSION,
    }


def score_daily_features(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        raise ValueError("features must not be empty")
    scored = features.copy()
    score_columns = scored.apply(score_row, axis=1, result_type="expand")
    for column in score_columns:
        scored[column] = score_columns[column]
    if not scored["score_coverage"].map(math.isfinite).all():
        raise ValueError("score coverage contains non-finite values")
    return scored
