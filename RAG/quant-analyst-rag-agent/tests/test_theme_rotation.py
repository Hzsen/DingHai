from __future__ import annotations

import math
import sqlite3
from datetime import timedelta

import pandas as pd
import pytest

from domain.knowledge import KnowledgeDocumentStatus, KnowledgeDocumentType, KnowledgeQuery
from quant_agent.knowledge.adapters import KnowledgeMigrationService, ThemeRotationKnowledgeAdapter
from quant_agent.knowledge.store import KnowledgeStore
from quant_agent.theme_rotation.catalog import default_theme_definitions
from quant_agent.theme_rotation.engine import evaluate_theme_rotation
from quant_agent.theme_rotation.report import publish_theme_rotation_outputs
from quant_agent.theme_rotation.repository import publish_theme_rotation


def _price_fixture(days: int = 130) -> pd.DataFrame:
    definitions = default_theme_definitions()
    symbols = sorted({"QQQ"} | {symbol for item in definitions for symbol in item.members})
    dates = pd.bdate_range("2026-01-02", periods=days)
    daily_rates = {symbol: 0.001 + (position % 8) * 0.00015 for position, symbol in enumerate(symbols)}
    daily_rates["QQQ"] = 0.001
    daily_rates["SMH"] = 0.002
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        rate = daily_rates[symbol]
        for ordinal, day in enumerate(dates):
            rows.append({
                "date": day,
                "symbol": symbol,
                "close": 100.0 * (1.0 + rate) ** ordinal,
                "volume": 1_000_000.0 * (1.0 + (ordinal % 20) / 100.0),
            })
    return pd.DataFrame(rows)


def test_theme_rotation_ports_reference_metrics_and_caps_scores() -> None:
    evaluation = evaluate_theme_rotation(_price_fixture(), default_theme_definitions())
    snapshot = evaluation.snapshot

    assert len(snapshot.themes) == 13
    assert len(snapshot.attributions) == 10
    semiconductor = next(item for item in snapshot.themes if item.theme_id == "semiconductor")
    expected_relative_5d = 100.0 * ((1.002 / 1.001) ** 5 - 1.0)
    assert semiconductor.relative_5d == pytest.approx(expected_relative_5d)
    assert semiconductor.live_members == 1
    assert semiconductor.data_coverage == 1.0
    assert all(item.score is None or 0 <= item.score <= 100 for item in snapshot.themes)
    assert all(0 <= item.score <= 100 for item in snapshot.attributions)
    assert all(len(values) == len(evaluation.dates) for values in evaluation.theme_score_history.values())


def test_theme_rotation_publishes_outputs_and_canonical_knowledge(tmp_path) -> None:
    prices = _price_fixture()
    evaluation = evaluate_theme_rotation(prices, default_theme_definitions(), selected_theme_id="software")
    db_path = tmp_path / "research.db"
    run_id = publish_theme_rotation(db_path, evaluation, prices, source="fixture")
    paths = publish_theme_rotation_outputs(tmp_path / "outputs", evaluation)

    assert run_id.startswith("theme-rotation-run/")
    assert "科技主题资金轮动" in paths["html"].read_text(encoding="utf-8")
    assert "renderTheme" in paths["html"].read_text(encoding="utf-8")
    assert "轮动归因" in paths["markdown"].read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM theme_rotation_metrics").fetchone()[0] == 13
        assert conn.execute("SELECT COUNT(*) FROM theme_rotation_attributions").fetchone()[0] == 10

    store = KnowledgeStore(db_path)
    service = KnowledgeMigrationService(store)
    first = service.migrate(ThemeRotationKnowledgeAdapter(db_path))
    second = service.migrate(ThemeRotationKnowledgeAdapter(db_path))
    assert first.migrated_documents == 1
    assert first.migrated_chunks == 5
    assert second.skipped_unchanged == 1
    chunks = store.query_chunks(KnowledgeQuery(
        query_text="软件和半导体是否强弱对调",
        as_of=evaluation.snapshot.as_of + timedelta(minutes=1),
        themes=("software",),
        document_types=(KnowledgeDocumentType.THEME_RESEARCH,),
        statuses=(KnowledgeDocumentStatus.FINALIZED,),
        top_k=10,
    ))
    assert chunks
    assert any("轮动归因" in item.chunk.section for item in chunks)


def test_partial_latest_member_data_is_reported_without_breaking_metrics() -> None:
    prices = _price_fixture()
    last_date = prices["date"].max()
    prices = prices.loc[~((prices["symbol"] == "MDB") & (prices["date"] == last_date))]
    evaluation = evaluate_theme_rotation(prices, default_theme_definitions())
    apps = next(item for item in evaluation.snapshot.themes if item.theme_id == "ai_apps")

    assert apps.live_members == 4
    assert apps.data_coverage == pytest.approx(0.8)
    assert apps.score is not None and math.isfinite(apps.score)
    assert "PARTIAL_THEME_MEMBER_COVERAGE" in evaluation.snapshot.quality_flags
