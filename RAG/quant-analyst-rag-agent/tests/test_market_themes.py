from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from domain.macro import SeriesFeature
from domain.macro_history import MacroHistoryPoint
from domain.market_theme import ThemeFamily, ThemeHorizon
from quant_agent.cli.run_macro_regime import load_feature_fixture
from quant_agent.macro.document import build_macro_document, publish_macro_document
from quant_agent.macro.report import publish_macro_outputs
from quant_agent.macro.rules import evaluate_macro
from quant_agent.macro.themes import (
    build_market_theme_states,
    evaluate_fast_market_themes,
    evaluate_repricing_market_themes,
)


ROOT = Path(__file__).resolve().parents[1]


def _fixture() -> tuple[datetime, dict[str, SeriesFeature]]:
    as_of, features, _ = load_feature_fixture(ROOT / "data/mock/macro_features_screenshot.json")
    return as_of, features


def _with_z(features: dict[str, SeriesFeature], series_id: str, z: float) -> None:
    if series_id in features:
        features[series_id] = replace(features[series_id], z_change_5d_252=z)
        return
    template = features["SPY"]
    features[series_id] = replace(
        template, series_id=series_id, value=100.0, z_change_5d_252=z,
        delta_1d=0.0, delta_5d=0.0, delta_20d=0.0,
    )


def test_fast_theme_detects_technology_concentration_with_explicit_evidence() -> None:
    _, features = _fixture()
    for series_id, z in {"QQQ": 1.5, "SPY": 0.6, "IWM": 0.0, "RSP": -0.1, "SOXX": 0.8}.items():
        _with_z(features, series_id, z)
    state = evaluate_fast_market_themes(features, evaluate_macro(features, features["SPY"].as_of))
    theme = next(item for item in state.active_themes if item.theme_id == "TECH_CONCENTRATION")
    assert state.dominant_theme_id == "TECH_CONCENTRATION"
    assert theme.confirmation_count == 3
    assert any("QQQ_MINUS_SPY" in item for item in theme.supporting_evidence)
    assert theme.invalidation_conditions


def test_stress_theme_overrides_non_stress_candidates() -> None:
    _, features = _fixture()
    for series_id, z in {
        "DXY": 1.6, "SPY": -1.2, "GLD": -0.8, "QQQ": -1.0,
        "IWM": -0.9, "IBIT": -1.1, "BAMLC0A0CM": 0.8,
    }.items():
        _with_z(features, series_id, z)
    state = evaluate_fast_market_themes(features, evaluate_macro(features, features["SPY"].as_of))
    assert state.dominant_theme_id == "USD_FUNDING_STRESS"
    assert state.active_themes[0].family is ThemeFamily.STRESS_OVERRIDE


def test_repricing_theme_detects_14_day_liquidity_acceleration() -> None:
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    first = MacroHistoryPoint(
        as_of=now - timedelta(days=14), snapshot_id="first", model_version="test",
        net_liquidity_20d_bn=-20, liquidity_score=-10, risk_score=30,
        rate_pressure_score=40, confidence=0.9, source_flows_bn={},
        target_absorption={"AI_SEMICONDUCTOR": 5}, target_states={"AI_SEMICONDUCTOR": "MIXED"},
    )
    current = replace(
        first, as_of=now, snapshot_id="current", net_liquidity_20d_bn=80,
        liquidity_score=80, target_absorption={"AI_SEMICONDUCTOR": -20},
    )
    state = evaluate_repricing_market_themes([first, current])
    assert state is not None
    assert state.horizon is ThemeHorizon.REPRICING
    assert state.dominant_theme_id == "LIQUIDITY_ACCELERATION"


def test_theme_layers_publish_to_rag_chunks_and_runnable_dashboard(tmp_path) -> None:
    _, features = _fixture()
    for series_id, z in {"QQQ": 1.5, "SPY": 0.6, "IWM": 0.0, "RSP": -0.1, "SOXX": 0.8}.items():
        _with_z(features, series_id, z)
    snapshot = evaluate_macro(features, features["SPY"].as_of)
    states = build_market_theme_states(features, snapshot)
    document = build_macro_document(snapshot, features, market_theme_states=states)
    db = tmp_path / "macro.db"
    publish_macro_document(db, document)
    with sqlite3.connect(db) as conn:
        chunk_types = {row[0] for row in conn.execute("SELECT chunk_type FROM macro_document_chunks")}
    assert "MARKET_THEME_FAST" in chunk_types
    assert "MARKET_THEME_FAST_INVALIDATION" in chunk_types
    paths = publish_macro_outputs(tmp_path, snapshot, document, market_theme_states=states)
    dashboard = paths["html"].read_text(encoding="utf-8")
    assert "Three-layer market view" in dashboard
    assert "Market theme evidence explorer" in dashboard
    assert "renderState(0)" in dashboard
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["market_theme_states"][0]["dominant_theme_id"] == "TECH_CONCENTRATION"
