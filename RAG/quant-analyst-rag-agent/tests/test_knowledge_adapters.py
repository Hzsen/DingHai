from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from quant_agent.knowledge.adapters import (
    KnowledgeMigrationService,
    ScreeningReportAdapter,
    StaticMarkdownAdapter,
    ThesisNoteAdapter,
    WeeklyResearchAdapter,
)
from quant_agent.knowledge.store import IndexJobStatus, KnowledgeStore


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _create_weekly_source(db_path: Path, *, status: str = "DRAFT") -> None:
    content = f"# Weekly Thesis\n\nStatus: {status}"
    summary_type = "DRAFT_SUMMARY" if status == "DRAFT" else "WEEKLY_SUMMARY"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE weekly_documents (
            document_id TEXT PRIMARY KEY,ticker TEXT NOT NULL,name TEXT NOT NULL,week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,as_of TEXT NOT NULL,status TEXT NOT NULL,version INTEGER NOT NULL,
            opening_state TEXT NOT NULL,closing_state TEXT NOT NULL,daily_observation_ids_json TEXT NOT NULL,
            state_change_ids_json TEXT NOT NULL,metrics_json TEXT NOT NULL,content TEXT NOT NULL,
            source_hash TEXT NOT NULL,source_run_id TEXT NOT NULL,llm_update_required INTEGER NOT NULL,
            document_schema_version TEXT NOT NULL,updated_at TEXT NOT NULL)"""
        )
        conn.execute(
            """CREATE TABLE weekly_document_chunks (
            chunk_id TEXT PRIMARY KEY,document_id TEXT NOT NULL,chunk_type TEXT NOT NULL,event_date TEXT,
            content TEXT NOT NULL,content_hash TEXT NOT NULL,indexable INTEGER NOT NULL,
            embedding_status TEXT NOT NULL,updated_at TEXT NOT NULL)"""
        )
        conn.execute(
            "INSERT INTO weekly_documents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "weekly/300308.SZ/2026-W29", "300308.SZ", "中际旭创", "2026-07-13", "2026-07-17",
                "2026-07-15T23:59:59+00:00", status, 1, "WATCHLIST", "MAIN_UPTREND_CONFIRMED",
                '["obs-1"]', '["change-1"]', "{}", content, _hash(content), "run-1", 1,
                "weekly-research-document-v1.0.0", "2026-07-15T08:00:00+00:00",
            ),
        )
        summary = f"summary {status}"
        state = "300308.SZ 2026-07-15 state changed: WATCHLIST -> MAIN_UPTREND_CONFIRMED."
        conn.executemany(
            "INSERT INTO weekly_document_chunks VALUES (?,?,?,?,?,?,?,?,?)",
            [
                ("weekly/300308.SZ/2026-W29/summary", "weekly/300308.SZ/2026-W29", summary_type,
                 None, summary, _hash(summary), int(status == "FINALIZED"),
                 "pending" if status == "FINALIZED" else "not_indexed", "2026-07-15T08:00:00+00:00"),
                ("weekly/300308.SZ/2026-W29/state/2026-07-15", "weekly/300308.SZ/2026-W29", "STATE_CHANGE",
                 "2026-07-15", state, _hash(state), 1, "pending", "2026-07-15T08:00:00+00:00"),
            ],
        )


def test_static_markdown_adapter_is_idempotent_and_versions_changes(tmp_path) -> None:
    docs = tmp_path / "data" / "docs" / "factor_definitions"
    docs.mkdir(parents=True)
    note = docs / "liquidity.md"
    note.write_text("# Liquidity\n\nFunding cost and market depth.", encoding="utf-8")
    store = KnowledgeStore(tmp_path / "knowledge.db")
    service = KnowledgeMigrationService(store)
    adapter = StaticMarkdownAdapter(tmp_path / "data" / "docs", project_root=tmp_path)

    first = service.migrate(adapter)
    second = service.migrate(adapter)
    note.write_text("# Liquidity\n\nFunding cost, duration supply, and market depth.", encoding="utf-8")
    third = service.migrate(adapter)

    assert first.migrated_documents == 1
    assert second.migrated_documents == 0
    assert second.skipped_unchanged == 1
    assert third.migrated_documents == 1
    assert store.get_document("markdown/factor_definitions/liquidity").version == 2


def test_weekly_draft_indexes_state_change_but_not_draft_summary(tmp_path) -> None:
    db_path = tmp_path / "research.db"
    _create_weekly_source(db_path)
    store = KnowledgeStore(db_path)
    service = KnowledgeMigrationService(store)

    first = service.migrate(WeeklyResearchAdapter(db_path))
    second = service.migrate(WeeklyResearchAdapter(db_path))
    chunks = store.get_chunks("weekly/300308.SZ/2026-W29")

    assert first.migrated_documents == 1
    assert first.index_jobs_created == 1
    assert second.skipped_unchanged == 1
    assert {chunk.chunk_type.value: chunk.indexable for chunk in chunks} == {
        "DRAFT_SUMMARY": False,
        "STATE_CHANGE": True,
    }
    assert store.index_job_counts()[IndexJobStatus.PENDING.value] == 1


def test_finalized_week_queues_summary_and_new_state_version(tmp_path) -> None:
    db_path = tmp_path / "research.db"
    _create_weekly_source(db_path)
    store = KnowledgeStore(db_path)
    service = KnowledgeMigrationService(store)
    service.migrate(WeeklyResearchAdapter(db_path))
    with sqlite3.connect(db_path) as conn:
        content = "# Weekly Thesis\n\nStatus: FINALIZED"
        conn.execute(
            "UPDATE weekly_documents SET status='FINALIZED',version=2,content=?,source_hash=?",
            (content, _hash(content)),
        )
        summary = "summary FINALIZED"
        conn.execute(
            """UPDATE weekly_document_chunks SET chunk_type='WEEKLY_SUMMARY',content=?,content_hash=?,
            indexable=1,embedding_status='pending' WHERE chunk_id LIKE '%/summary'""",
            (summary, _hash(summary)),
        )

    result = service.migrate(WeeklyResearchAdapter(db_path))
    latest = store.get_document("weekly/300308.SZ/2026-W29")
    chunks = store.get_chunks("weekly/300308.SZ/2026-W29")

    assert result.migrated_documents == 1
    assert result.index_jobs_created == 2
    assert latest.version == 2
    assert latest.status.value == "FINALIZED"
    assert all(chunk.indexable for chunk in chunks)


def test_thesis_note_adapter_preserves_semantic_sections(tmp_path) -> None:
    notes = tmp_path / "outputs" / "thesis_notes"
    notes.mkdir(parents=True)
    (notes / "300308.md").write_text(
        """# Thesis Update: 300308.SZ 中际旭创

## State Change
BREAKOUT_CANDIDATE -> MAIN_UPTREND_CONFIRMED

## Numeric Evidence
- amount_rank_market: 8

## Risk Notes
Watch distribution.

## Source Thesis
thesis/300308.SZ
""",
        encoding="utf-8",
    )
    store = KnowledgeStore(tmp_path / "knowledge.db")

    result = KnowledgeMigrationService(store).migrate(
        ThesisNoteAdapter(notes, project_root=tmp_path)
    )
    document = store.get_document("thesis-note/300308")
    chunk_types = {chunk.chunk_type.value for chunk in store.get_chunks(document.document_id)}

    assert result.migrated_documents == 1
    assert document.thesis_id == "thesis/300308.SZ"
    assert document.tickers == ("300308.SZ",)
    assert {"STATE_CHANGE", "EVIDENCE", "RISK"}.issubset(chunk_types)


def test_screening_adapter_references_gold_without_copying_numeric_rows(tmp_path) -> None:
    db_path = tmp_path / "research.db"
    report = tmp_path / "outputs" / "reversal.md"
    report.parent.mkdir(parents=True)
    report.write_text("presentation only", encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE gold_cn_reversal_screen_results (
            as_of TEXT,ticker TEXT,name TEXT,reversal_score REAL,stage TEXT,focus_selected INTEGER,
            feature_version TEXT,score_version TEXT,market_regime TEXT,feature_json TEXT,
            top_reasons TEXT,risk_flags TEXT,exclusion_reasons TEXT,source_metadata_json TEXT,
            PRIMARY KEY(as_of,ticker,score_version))"""
        )
        conn.execute(
            "INSERT INTO gold_cn_reversal_screen_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("2026-07-14", "300308.SZ", "中际旭创", 94.0, "LEADER_REPAIR_CONFIRMED", 1,
             "features-v1", "reversal-v1", "SELLOFF_REPAIR", "{\"secret_number\": 12345}",
             "reason", "", "", "{}"),
        )
    store = KnowledgeStore(db_path)

    result = KnowledgeMigrationService(store).migrate(
        ScreeningReportAdapter(db_path, report, as_of="2026-07-14", project_root=tmp_path)
    )
    document = store.get_document("screening/cn-reversal/2026-07-14")

    assert result.migrated_documents == 1
    assert document.metadata["gold_table"] == "gold_cn_reversal_screen_results"
    assert document.metadata["numeric_truth_embedded"] is False
    assert "94.0" not in document.content
    assert "12345" not in document.content
    assert "gold_cn_reversal_screen_results" in document.content
