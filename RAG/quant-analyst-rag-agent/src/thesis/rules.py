from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from domain.thesis import EvidenceValue, StockThesis, ThesisStatus, ThesisValidationResult


LLM_UPDATE_STATUSES = {
    ThesisStatus.BREAKOUT_CANDIDATE,
    ThesisStatus.MAIN_UPTREND_CONFIRMED,
    ThesisStatus.DISTRIBUTION_RISK,
    ThesisStatus.INVALIDATED,
    ThesisStatus.REACTIVATION_WATCH,
}
RESEARCH_NOTE_STATUSES = {
    ThesisStatus.MAIN_UPTREND_CONFIRMED,
    ThesisStatus.DISTRIBUTION_RISK,
    ThesisStatus.INVALIDATED,
    ThesisStatus.REACTIVATION_WATCH,
}


def _number(features: Mapping[str, Any], key: str) -> float | None:
    value = features.get(key)
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _at_least(features: Mapping[str, Any], key: str, threshold: float) -> bool:
    value = _number(features, key)
    return value is not None and value >= threshold


def _at_most(features: Mapping[str, Any], key: str, threshold: float) -> bool:
    value = _number(features, key)
    return value is not None and value <= threshold


def _greater_than(features: Mapping[str, Any], key: str, threshold: float) -> bool:
    value = _number(features, key)
    return value is not None and value > threshold


def _less_than(features: Mapping[str, Any], key: str, threshold: float) -> bool:
    value = _number(features, key)
    return value is not None and value < threshold


def _evidence(features: Mapping[str, Any]) -> dict[str, EvidenceValue]:
    evidence: dict[str, EvidenceValue] = {}
    for key, value in features.items():
        if value is None or isinstance(value, (float, int, str, bool)):
            evidence[str(key)] = value
        else:
            evidence[str(key)] = str(value)
    return evidence


def validate_thesis_state(thesis: StockThesis, features: dict) -> ThesisValidationResult:
    """Apply one deterministic state transition without mutating the thesis.

    Missing or malformed features make a rule fail closed. Risk transitions are
    evaluated before acceleration so distribution evidence cannot be hidden by
    strong trailing returns.
    """
    previous = thesis.status
    new_status = previous
    reason_codes: list[str] = []

    distribution_signal = bool(features.get("high_volume_stall_flag")) or _at_most(
        features, "drawdown_from_high", -0.18
    )

    if previous == ThesisStatus.WATCHLIST:
        if _at_least(features, "theme_heat_score", 2):
            new_status = ThesisStatus.THEME_WARMUP
            reason_codes = ["THEME_HEAT_WARMUP"]

    elif previous == ThesisStatus.THEME_WARMUP:
        if (
            _at_least(features, "distance_to_120d_high", -0.03)
            and _at_least(features, "amount_ratio_20d", 1.8)
            and _greater_than(features, "rs_market_20d", 0)
        ):
            new_status = ThesisStatus.BREAKOUT_CANDIDATE
            reason_codes = ["NEAR_120D_HIGH", "AMOUNT_EXPANSION", "MARKET_RELATIVE_STRENGTH"]

    elif previous == ThesisStatus.BREAKOUT_CANDIDATE:
        if (
            _at_most(features, "amount_rank_market", 100)
            and _greater_than(features, "rs_industry_20d", 0)
            and _at_least(features, "new_high_count_20d", 2)
        ):
            new_status = ThesisStatus.MAIN_UPTREND_CONFIRMED
            reason_codes = ["TOP100_MARKET_AMOUNT", "INDUSTRY_RELATIVE_STRENGTH", "REPEATED_NEW_HIGHS"]

    elif previous == ThesisStatus.MAIN_UPTREND_CONFIRMED:
        if distribution_signal:
            new_status = ThesisStatus.DISTRIBUTION_RISK
            reason_codes = [
                code
                for condition, code in (
                    (bool(features.get("high_volume_stall_flag")), "HIGH_VOLUME_STALL"),
                    (_at_most(features, "drawdown_from_high", -0.18), "DRAWDOWN_FROM_HIGH"),
                )
                if condition
            ]
        elif (
            _at_least(features, "return_20d", 0.35)
            and _at_most(features, "amount_rank_market", 50)
            and _at_least(features, "new_high_count_20d", 4)
        ):
            new_status = ThesisStatus.ACCELERATION
            reason_codes = ["RETURN_ACCELERATION", "TOP50_MARKET_AMOUNT", "HIGH_FREQUENCY_NEW_HIGHS"]

    elif previous == ThesisStatus.ACCELERATION:
        if distribution_signal:
            new_status = ThesisStatus.DISTRIBUTION_RISK
            reason_codes = [
                code
                for condition, code in (
                    (bool(features.get("high_volume_stall_flag")), "HIGH_VOLUME_STALL"),
                    (_at_most(features, "drawdown_from_high", -0.18), "DRAWDOWN_FROM_HIGH"),
                )
                if condition
            ]

    elif previous == ThesisStatus.DISTRIBUTION_RISK:
        if (
            bool(features.get("below_60d_ma"))
            and _less_than(features, "rs_market_20d", 0)
            and _greater_than(features, "amount_rank_market", 200)
        ):
            new_status = ThesisStatus.INVALIDATED
            reason_codes = ["BELOW_60D_MA", "NEGATIVE_MARKET_RS", "LIQUIDITY_RANK_DROPPED"]

    elif previous == ThesisStatus.INVALIDATED:
        if (
            _at_least(features, "theme_heat_score", 2)
            and _at_least(features, "amount_ratio_20d", 2)
            and _greater_than(features, "rs_market_20d", 0)
            and _at_least(features, "distance_to_120d_high", -0.05)
        ):
            new_status = ThesisStatus.REACTIVATION_WATCH
            reason_codes = ["THEME_REHEATED", "AMOUNT_REEXPANSION", "RS_RECOVERED", "NEAR_120D_HIGH_AGAIN"]

    elif previous == ThesisStatus.REACTIVATION_WATCH:
        near_120d_breakout = _at_least(features, "distance_to_120d_high", 0)
        near_250d_breakout = _at_least(features, "distance_to_250d_high", 0)
        if (near_120d_breakout or near_250d_breakout) and _at_most(features, "amount_rank_market", 100):
            reason_codes = ["NEW_THESIS_RECOMMENDED", "REBREAKOUT_WITH_LIQUIDITY"]

    changed = new_status != previous
    if not reason_codes:
        reason_codes = ["NO_STATE_CHANGE"]
    needs_llm_update = changed and new_status in LLM_UPDATE_STATUSES
    needs_research_note = changed and new_status in RESEARCH_NOTE_STATUSES

    return ThesisValidationResult(
        ticker=thesis.ticker,
        thesis_id=thesis.thesis_id,
        previous_status=previous,
        new_status=new_status,
        changed=changed,
        reason_codes=reason_codes,
        numeric_evidence=_evidence(features),
        needs_llm_update=needs_llm_update,
        needs_research_note=needs_research_note,
    )
