from __future__ import annotations

import sqlite3

import pandas as pd

from domain.weekly_document import WeeklyDocumentStatus
from quant_agent.research.weekly_documents import build_weekly_documents, publish_weekly_documents, weekly_incremental_start


def _scored() -> pd.DataFrame:
    dates = pd.bdate_range("2026-07-06", periods=5)
    rows = []
    for index, day in enumerate(dates):
        rows.append({"ticker":"000300.SH","name":"沪深300","date":day,"close":100+index,"feature_version":"v1","source_run_id":"run-1","wave_score":0,"risk_penalty":0,"high_volume_stall_flag":False,"benchmark_return_20d":0.0,"turnover_rate":0.0,"amount_rank_pilot":None})
        rows.append({"ticker":"300308.SZ","name":"中际旭创","date":day,"close":200+index*2,"feature_version":"v1","source_run_id":"run-1","wave_score":65,"risk_penalty":0,"high_volume_stall_flag":False,"benchmark_return_20d":0.0,"turnover_rate":0.03,"amount_rank_pilot":2.0})
    return pd.DataFrame(rows)


def test_finalized_week_creates_one_indexable_summary_not_daily_documents(tmp_path) -> None:
    documents, chunks = build_weekly_documents(_scored(), "2026-07-10")
    stats = publish_weekly_documents(tmp_path / "weekly.db", documents, chunks, tmp_path / "out")

    assert len(documents) == 1
    assert documents[0].status == WeeklyDocumentStatus.FINALIZED
    assert stats["daily_document_baseline"] == 5
    assert stats["indexable_chunk_count"] == 1
    assert stats["embedding_reduction_ratio"] == 0.8


def test_draft_week_is_updated_idempotently_and_not_indexed(tmp_path) -> None:
    db_path = tmp_path / "weekly.db"
    documents, chunks = build_weekly_documents(_scored(), "2026-07-08")
    first = publish_weekly_documents(db_path, documents, chunks, tmp_path / "out")
    second = publish_weekly_documents(db_path, documents, chunks, tmp_path / "out")

    with sqlite3.connect(db_path) as conn:
        version = conn.execute("SELECT version FROM weekly_documents").fetchone()[0]
        indexable = conn.execute("SELECT SUM(indexable) FROM weekly_document_chunks").fetchone()[0]
    assert first["weekly_document_count"] == second["weekly_document_count"] == 1
    assert version == 1
    assert indexable == 0


def test_changed_draft_increments_version(tmp_path) -> None:
    db_path = tmp_path / "weekly.db"
    scored = _scored()
    documents, chunks = build_weekly_documents(scored, "2026-07-08")
    publish_weekly_documents(db_path, documents, chunks, tmp_path / "out")
    scored.loc[(scored["ticker"] == "300308.SZ") & (scored["date"] == pd.Timestamp("2026-07-08")), "close"] += 10
    changed_documents, changed_chunks = build_weekly_documents(scored, "2026-07-08")
    publish_weekly_documents(db_path, changed_documents, changed_chunks, tmp_path / "out")

    with sqlite3.connect(db_path) as conn:
        version = conn.execute("SELECT version FROM weekly_documents").fetchone()[0]
    assert version == 2


def test_existing_store_only_rebuilds_current_and_previous_week(tmp_path) -> None:
    db_path = tmp_path / "weekly.db"
    documents, chunks = build_weekly_documents(_scored(), "2026-07-10")
    publish_weekly_documents(db_path, documents, chunks, tmp_path / "out")

    assert weekly_incremental_start(db_path, "2026-07-15").isoformat() == "2026-07-06"
