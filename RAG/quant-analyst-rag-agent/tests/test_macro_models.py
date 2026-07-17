from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd

from domain.macro import MacroDocumentStatus, RatePressureState, SeriesFeature, Stance, StanceHorizon
from quant_agent.macro.document import build_macro_document, publish_macro_document
from quant_agent.macro.features import compute_macro_features
from quant_agent.macro.report import publish_macro_outputs
from quant_agent.macro.rules import evaluate_macro


NOW = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)


def _feature(
    series_id: str,
    value: float,
    *,
    d1: float = 0.0,
    d5: float = 0.0,
    d20: float = 0.0,
    percentile: float = 0.5,
    z5: float = 0.0,
    stale: int = 0,
    unit: str = "percent",
) -> SeriesFeature:
    return SeriesFeature(
        series_id=series_id, as_of=NOW, value=value, unit=unit, source="fixture",
        observation_date=(NOW - timedelta(days=stale)).isoformat(), available_at=NOW.isoformat(),
        is_realtime=False, stale_days=stale, delta_1d=d1, delta_5d=d5, delta_20d=d20,
        percentile_5y=percentile, z_change_5d_252=z5,
        quality_flags=("STALE_SERIES",) if stale > 8 else (),
    )


def _screenshot_like_features(stale: int = 0) -> dict[str, SeriesFeature]:
    return {
        "DFII10": _feature("DFII10", 2.35, d1=0.03, d5=0.06, d20=0.11, percentile=0.97, z5=0.78, stale=stale),
        "DGS10": _feature("DGS10", 4.62, d1=-0.008, d5=0.07, d20=0.01, percentile=0.90, z5=0.72, stale=stale),
        "DGS30": _feature("DGS30", 5.10, d1=-0.007, d5=0.08, d20=0.03, percentile=0.99, z5=1.04, stale=stale),
        "DGS2": _feature("DGS2", 4.28, d1=-0.004, d5=-0.01, d20=0.03, stale=stale),
        "T10YIE": _feature("T10YIE", 2.28, d1=0.024, d5=0.02, d20=-0.03, stale=stale),
        "DXY": _feature("DXY", 101.19, d1=-0.09, d5=0.07, d20=1.66, unit="index", stale=stale),
        "BAMLC0A0CM": _feature("BAMLC0A0CM", 0.68, d1=0.0001, d5=0.002, d20=0.0024, stale=stale),
        "VIX": _feature("VIX", 17.4, d5=0.5, unit="index", stale=stale),
        "VIX3M": _feature("VIX3M", 20.0, d5=0.3, unit="index", stale=stale),
        "MOVE": _feature("MOVE", 77.8, d5=1.0, unit="index", stale=stale),
        "SPY": _feature("SPY", 600, d1=-3, d5=-6, d20=-4, unit="usd", stale=stale),
        "QQQ": _feature("QQQ", 530, d1=-10, d5=-8, d20=-4, unit="usd", stale=stale),
        "IWM": _feature("IWM", 220, d1=-1.8, d5=-4, d20=2, unit="usd", stale=stale),
        "GLD": _feature("GLD", 367.13, d1=-0.31, d5=-14.9, d20=-19.2, unit="usd", stale=stale),
        "IEF": _feature("IEF", 92, d1=-0.4, d5=-1.2, d20=-2.0, unit="usd", stale=stale),
        "TLT": _feature("TLT", 84, d1=-0.8, d5=-2.0, d20=-3.5, unit="usd", stale=stale),
        "IWM_SPY": _feature("IWM_SPY", 0.36, d5=-0.01, d20=-0.015, unit="ratio", stale=stale),
        "KRE_SPY": _feature("KRE_SPY", 0.10, d5=-0.002, d20=-0.005, unit="ratio", stale=stale),
        "SOXX_QQQ": _feature("SOXX_QQQ", 0.45, d5=-0.02, d20=-0.03, unit="ratio", stale=stale),
        "WALCL": _feature("WALCL", 6_600_000, d20=10_000, unit="millions_usd", stale=stale),
        "WTREGEN": _feature("WTREGEN", 850_000, d20=40_000, unit="millions_usd", stale=stale),
        "RRPONTSYD": _feature("RRPONTSYD", 100_000, d20=-5_000, unit="millions_usd", stale=stale),
    }


def test_point_in_time_features_ignore_future_available_rows() -> None:
    observations = pd.DataFrame([
        {"series_id": "DGS10", "observation_date": "2026-07-14", "available_at": "2026-07-14T22:00:00Z", "value": 4.5, "unit": "percent", "source": "fixture", "is_realtime": False},
        {"series_id": "DGS10", "observation_date": "2026-07-15", "available_at": "2026-07-16T00:00:00Z", "value": 9.9, "unit": "percent", "source": "fixture", "is_realtime": False},
    ])
    result = compute_macro_features(observations, NOW)
    assert result["DGS10"].value == 4.5


def test_high_real_rate_pressure_makes_treasury_price_stance_bearish() -> None:
    snapshot = evaluate_macro(_screenshot_like_features(), NOW)
    assert snapshot.rate_pressure_state in {RatePressureState.SUSTAINED_PRESSURE, RatePressureState.EXTREME_PRESSURE}
    ust30 = next(item for item in snapshot.asset_stances if item.asset_id == "UST30_PRICE" and item.horizon is StanceHorizon.SWING)
    assert ust30.stance in {Stance.BEARISH, Stance.STRONGLY_BEARISH}
    assert "YIELDS_UP_IS_BOND_PRICE_NEGATIVE" in ust30.opposing_factors


def test_asynchronous_rate_decomposition_is_flagged() -> None:
    snapshot = evaluate_macro(_screenshot_like_features(), NOW)
    assert "ASYNCHRONOUS_RATE_DECOMPOSITION" in snapshot.quality_flags


def test_stale_data_lowers_snapshot_confidence() -> None:
    fresh = evaluate_macro(_screenshot_like_features(stale=0), NOW)
    stale = evaluate_macro(_screenshot_like_features(stale=10), NOW)
    assert stale.confidence < fresh.confidence
    assert "DFII10" in stale.stale_series


def test_liquidity_model_normalizes_billions_and_millions() -> None:
    in_millions = _screenshot_like_features()
    in_billions = dict(in_millions)
    original = in_millions["RRPONTSYD"]
    in_billions["RRPONTSYD"] = SeriesFeature(
        series_id=original.series_id, as_of=original.as_of, value=original.value / 1_000,
        unit="billions_usd", source=original.source, observation_date=original.observation_date,
        available_at=original.available_at, is_realtime=original.is_realtime, stale_days=original.stale_days,
        delta_1d=original.delta_1d / 1_000, delta_5d=original.delta_5d / 1_000,
        delta_20d=original.delta_20d / 1_000, percentile_5y=original.percentile_5y,
        z_change_5d_252=original.z_change_5d_252, quality_flags=original.quality_flags,
    )
    assert evaluate_macro(in_millions, NOW).liquidity_score == evaluate_macro(in_billions, NOW).liquidity_score


def test_input_proxy_quality_flag_reaches_snapshot() -> None:
    features = _screenshot_like_features()
    dxy = features.pop("DXY")
    features["DXY_PROXY"] = SeriesFeature(
        series_id="DXY_PROXY", as_of=dxy.as_of, value=dxy.value, unit=dxy.unit, source=dxy.source,
        observation_date=dxy.observation_date, available_at=dxy.available_at, is_realtime=dxy.is_realtime,
        stale_days=dxy.stale_days, delta_1d=dxy.delta_1d, delta_5d=dxy.delta_5d,
        delta_20d=dxy.delta_20d, percentile_5y=dxy.percentile_5y,
        z_change_5d_252=dxy.z_change_5d_252,
        quality_flags=("BROAD_DOLLAR_PROXY_NOT_ICE_DXY",),
    )
    snapshot = evaluate_macro(features, NOW)
    assert "BROAD_DOLLAR_PROXY_NOT_ICE_DXY" in snapshot.quality_flags


def test_macro_document_publish_supersedes_previous_and_is_idempotent(tmp_path) -> None:
    features = _screenshot_like_features()
    first_snapshot = evaluate_macro(features, NOW - timedelta(days=1))
    second_snapshot = evaluate_macro(features, NOW)
    first = build_macro_document(first_snapshot, features)
    second = build_macro_document(second_snapshot, features)
    db = tmp_path / "macro.db"
    publish_macro_document(db, first)
    publish_macro_document(db, second)
    publish_macro_document(db, second)
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT document_id,status FROM macro_risk_documents ORDER BY as_of").fetchall()
        active_chunks = conn.execute("SELECT COUNT(*) FROM macro_document_chunks WHERE indexable=1").fetchone()[0]
    assert rows == [(first.document_id, MacroDocumentStatus.SUPERSEDED.value), (second.document_id, MacroDocumentStatus.FINALIZED_DAILY.value)]
    assert active_chunks == 5


def test_historical_rerun_does_not_supersede_newer_document(tmp_path) -> None:
    features = _screenshot_like_features()
    older = build_macro_document(evaluate_macro(features, NOW - timedelta(days=1)), features)
    newer = build_macro_document(evaluate_macro(features, NOW), features)
    db = tmp_path / "macro.db"
    publish_macro_document(db, newer)
    publish_macro_document(db, older)
    with sqlite3.connect(db) as conn:
        statuses = dict(conn.execute("SELECT document_id,status FROM macro_risk_documents"))
        active_chunks = conn.execute("SELECT COUNT(*) FROM macro_document_chunks WHERE indexable=1").fetchone()[0]
    assert statuses[newer.document_id] == MacroDocumentStatus.FINALIZED_DAILY.value
    assert statuses[older.document_id] == MacroDocumentStatus.SUPERSEDED.value
    assert active_chunks == 5


def test_macro_outputs_include_dashboard_and_research_disclaimer(tmp_path) -> None:
    features = _screenshot_like_features()
    snapshot = evaluate_macro(features, NOW)
    document = build_macro_document(snapshot, features)
    paths = publish_macro_outputs(tmp_path, snapshot, document)
    assert all(path.exists() for path in paths.values())
    dashboard = paths["html"].read_text(encoding="utf-8")
    assert "Where liquidity is being absorbed" in dashboard
    assert "not audited ETF fund flows" in dashboard
    assert "not investment advice" in paths["markdown"].read_text(encoding="utf-8")


def test_liquidity_snapshot_separates_sources_from_target_absorption() -> None:
    snapshot = evaluate_macro(_screenshot_like_features(), NOW)
    source_ids = {item.source_id for item in snapshot.liquidity_source_flows}
    target_ids = {item.target_id for item in snapshot.liquidity_target_flows}
    assert source_ids == {"FED_BALANCE_SHEET", "TREASURY_GENERAL_ACCOUNT", "OVERNIGHT_REVERSE_REPO"}
    assert {"AI_SEMICONDUCTOR", "US_SMALL_CAP", "TREASURY_20Y_PLUS", "GOLD"} <= target_ids
    assert all("not audited ETF net fund flow" in item.measurement_note for item in snapshot.liquidity_target_flows)
