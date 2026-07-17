from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone

from domain.thesis import StockThesis, ThesisStatus, ThesisType
from thesis.rules import validate_thesis_state


def _thesis(status: ThesisStatus) -> StockThesis:
    now = datetime(2025, 7, 15, tzinfo=timezone.utc)
    return StockThesis(
        thesis_id="thesis-300308",
        ticker="300308.SZ",
        name="中际旭创",
        theme="AI infrastructure",
        thesis_type=ThesisType.AI_INFRASTRUCTURE_CHAIN,
        start_date=date(2025, 7, 15),
        end_date=None,
        status=status,
        key_factors=["relative strength", "amount"],
        validation_signals=["new high"],
        invalidation_signals=["distribution"],
        narrative_summary="",
        fundamental_logic="",
        capital_flow_logic="",
        risk_notes="",
        source_document_ids=[],
        created_at=now,
        updated_at=now,
    )


def test_watchlist_to_theme_warmup_without_llm() -> None:
    result = validate_thesis_state(_thesis(ThesisStatus.WATCHLIST), {"theme_heat_score": 2})

    assert result.new_status == ThesisStatus.THEME_WARMUP
    assert result.changed is True
    assert result.needs_llm_update is False


def test_theme_warmup_to_breakout_candidate() -> None:
    result = validate_thesis_state(
        _thesis(ThesisStatus.THEME_WARMUP),
        {"distance_to_120d_high": -0.02, "amount_ratio_20d": 1.8, "rs_market_20d": 0.01},
    )

    assert result.new_status == ThesisStatus.BREAKOUT_CANDIDATE
    assert result.needs_llm_update is True
    assert result.needs_research_note is False


def test_breakout_candidate_to_main_uptrend_confirmed() -> None:
    result = validate_thesis_state(
        _thesis(ThesisStatus.BREAKOUT_CANDIDATE),
        {"amount_rank_market": 100, "rs_industry_20d": 0.02, "new_high_count_20d": 2},
    )

    assert result.new_status == ThesisStatus.MAIN_UPTREND_CONFIRMED
    assert result.needs_research_note is True


def test_main_uptrend_confirmed_to_distribution_risk() -> None:
    result = validate_thesis_state(
        _thesis(ThesisStatus.MAIN_UPTREND_CONFIRMED),
        {"high_volume_stall_flag": True, "drawdown_from_high": -0.05},
    )

    assert result.new_status == ThesisStatus.DISTRIBUTION_RISK
    assert "HIGH_VOLUME_STALL" in result.reason_codes


def test_distribution_risk_to_invalidated() -> None:
    result = validate_thesis_state(
        _thesis(ThesisStatus.DISTRIBUTION_RISK),
        {"below_60d_ma": True, "rs_market_20d": -0.01, "amount_rank_market": 201},
    )

    assert result.new_status == ThesisStatus.INVALIDATED
    assert result.needs_research_note is True


def test_invalidated_to_reactivation_watch() -> None:
    result = validate_thesis_state(
        _thesis(ThesisStatus.INVALIDATED),
        {
            "theme_heat_score": 2,
            "amount_ratio_20d": 2,
            "rs_market_20d": 0.01,
            "distance_to_120d_high": -0.05,
        },
    )

    assert result.new_status == ThesisStatus.REACTIVATION_WATCH
    assert result.needs_llm_update is True


def test_reactivation_breakout_recommends_new_thesis_without_overwrite() -> None:
    old = _thesis(ThesisStatus.REACTIVATION_WATCH)
    result = validate_thesis_state(
        old,
        {"distance_to_120d_high": 0.01, "distance_to_250d_high": -0.03, "amount_rank_market": 80},
    )

    assert result.new_status == ThesisStatus.REACTIVATION_WATCH
    assert result.changed is False
    assert result.needs_llm_update is False
    assert "NEW_THESIS_RECOMMENDED" in result.reason_codes
    assert old == replace(old)
