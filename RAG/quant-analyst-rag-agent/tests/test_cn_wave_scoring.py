from __future__ import annotations

import pandas as pd

from quant_agent.research.cn_wave.scoring import score_row


def _complete_row() -> pd.Series:
    return pd.Series(
        {
            "theme_score": 3,
            "company_relevance": "direct",
            "narrative_conflict_flag": False,
            "fundamental_score": 3,
            "close": 100.0,
            "rolling_high_120d": 101.0,
            "rolling_high_250d": 102.0,
            "amount_ratio_20d": 2.5,
            "amount_rank_market": 20,
            "rs_market_20d": 0.2,
            "rs_industry_20d": 0.1,
            "rs_rank_market_20d": 0.02,
            "base_turnover_sum_60d": 1.2,
            "overhead_supply_ratio": 0.1,
            "high_volume_stall_flag": False,
            "upper_shadow_ratio": 0.01,
            "return_20d": 0.4,
        }
    )


def test_complete_positive_row_reaches_confirmed_stage() -> None:
    result = score_row(_complete_row())

    assert result["leader_score"] == 13
    assert result["score_coverage"] == 1.0
    assert result["stage_label"] == "confirmed_main_uptrend"


def test_exhaustion_overrides_high_positive_score() -> None:
    row = _complete_row()
    row["high_volume_stall_flag"] = True
    row["upper_shadow_ratio"] = 0.10
    row["return_20d"] = 0.60

    result = score_row(row)

    assert result["stage_label"] == "exhaustion_risk"
    assert result["exhaustion_component"] == -3
    assert "high_volume_stall" in result["risk_flags"]


def test_narrative_fields_do_not_change_market_behavior_score() -> None:
    row = _complete_row()
    baseline = score_row(row)
    row["theme_score"] = 0
    row["company_relevance"] = "contradicted"
    row["narrative_conflict_flag"] = True
    row["fundamental_score"] = 0

    result = score_row(row)

    assert result["leader_score"] == baseline["leader_score"]
    assert result["stage_label"] == baseline["stage_label"]


def test_missing_market_evidence_is_not_treated_as_zero() -> None:
    row = _complete_row()
    for field in (
        "rolling_high_120d",
        "rolling_high_250d",
        "amount_rank_market",
        "rs_industry_20d",
        "rs_rank_market_20d",
    ):
        row[field] = pd.NA

    result = score_row(row)

    assert result["score_coverage"] < 0.65
    assert result["stage_label"] == "insufficient_evidence"
    assert "rolling_high_120d" in result["missing_components"]
