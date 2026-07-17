from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from domain.knowledge import (
    KnowledgeChunk,
    KnowledgeChunkType,
    KnowledgeDocument,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeQuery,
    KnowledgeReliability,
    KnowledgeSourceType,
    canonical_json_sha256,
    content_sha256,
)
from quant_agent.knowledge.store import (
    IndexJobOperation,
    IndexJobStatus,
    KnowledgeBundle,
    KnowledgeStore,
    VersionConflictError,
)


UTC = timezone.utc
T0 = datetime(2026, 7, 14, 7, 5, tzinfo=UTC)


def _document(
    document_id: str = "weekly/300308.SZ/2026-W29",
    version: int = 1,
    ticker: str = "300308.SZ",
    available_at: datetime = T0,
    status: KnowledgeDocumentStatus = KnowledgeDocumentStatus.DRAFT,
    content: str = "中际旭创周度研究。",
) -> KnowledgeDocument:
    return KnowledgeDocument(
        document_id=document_id, document_type=KnowledgeDocumentType.WEEKLY_RESEARCH,
        title=f"{ticker}周度研究", content=content, tickers=(ticker,), themes=("AI_INFRASTRUCTURE",),
        thesis_id=f"thesis/{ticker}", event_time=available_at - timedelta(minutes=5), as_of=available_at,
        available_at=available_at, status=status, version=version,
        source_type=KnowledgeSourceType.SYSTEM_DERIVED, source_uri=f"sqlite://weekly/{document_id}",
        source_hash=canonical_json_sha256({"document_id": document_id, "version": version}),
        content_hash=content_sha256(content), reliability=KnowledgeReliability.DERIVED,
        language="zh-CN", created_at=available_at, updated_at=available_at,
        metadata={"adapter": "fixture"},
    )


def _chunk(
    document: KnowledgeDocument,
    chunk_id: str = "state-change",
    chunk_type: KnowledgeChunkType = KnowledgeChunkType.STATE_CHANGE,
    indexable: bool = True,
    text: str = "状态变化为EARLY_STABILIZATION。",
    available_at: datetime | None = None,
) -> KnowledgeChunk:
    return KnowledgeChunk(
        chunk_id=f"{document.document_id}::{chunk_id}", document_id=document.document_id,
        document_version=document.version, chunk_type=chunk_type, section=chunk_type.value,
        text=text, ordinal=0, event_time=document.event_time,
        available_at=available_at or document.available_at, content_hash=content_sha256(text),
        token_count=5, indexable=indexable, metadata={"fixture": True},
    )


def test_repeated_ingestion_is_idempotent_and_does_not_duplicate_jobs(tmp_path) -> None:
    store = KnowledgeStore(tmp_path / "knowledge.db")
    document = _document()
    chunk = _chunk(document)
    first = store.ingest(document, (chunk,), "fixture")
    second = store.ingest(document, (chunk,), "fixture")

    assert first.index_jobs_created == 1
    assert second.index_jobs_created == 0
    assert store.table_count("knowledge_documents") == 1
    assert store.table_count("knowledge_chunks") == 1
    assert store.table_count("knowledge_index_jobs") == 1
    assert store.table_count("knowledge_ingestion_runs") == 2


def test_draft_summary_is_not_queued_but_state_change_is_queryable(tmp_path) -> None:
    store = KnowledgeStore(tmp_path / "knowledge.db")
    document = _document(status=KnowledgeDocumentStatus.DRAFT)
    event = _chunk(document)
    draft = _chunk(
        document, "draft", KnowledgeChunkType.DRAFT_SUMMARY, False, "本周尚未结束。"
    )
    result = store.ingest(document, (event, draft), "weekly-adapter")
    rows = store.query_chunks(KnowledgeQuery("止跌状态", T0, tickers=("300308.SZ",), top_k=10))

    assert result.index_jobs_created == 1
    assert [row.chunk.chunk_id for row in rows] == [event.chunk_id]


def test_temporal_and_metadata_filters_prevent_future_leakage(tmp_path) -> None:
    store = KnowledgeStore(tmp_path / "knowledge.db")
    visible = _document()
    future = _document(
        document_id="weekly/688146.SH/2026-W29", ticker="688146.SH",
        available_at=T0 + timedelta(days=1), content="中船特气未来研究。",
    )
    store.ingest_batch(
        (KnowledgeBundle(visible, (_chunk(visible),)), KnowledgeBundle(future, (_chunk(future),))),
        "fixture",
    )

    before_future = store.query_chunks(KnowledgeQuery("研究", T0, top_k=10), limit=100)
    ticker_only = store.query_chunks(KnowledgeQuery("研究", T0 + timedelta(days=2), tickers=("688146.SH",), top_k=10))
    wrong_theme = store.query_chunks(KnowledgeQuery("研究", T0 + timedelta(days=2), themes=("BIOTECH",), top_k=10))

    assert {row.document.document_id for row in before_future} == {visible.document_id}
    assert {row.document.document_id for row in ticker_only} == {future.document_id}
    assert wrong_theme == []


def test_sql_temporal_comparison_normalizes_timezone_offsets_to_utc(tmp_path) -> None:
    store = KnowledgeStore(tmp_path / "knowledge.db")
    document = _document()
    store.ingest(document, (_chunk(document),), "fixture")
    shanghai = timezone(timedelta(hours=8))
    same_instant = datetime(2026, 7, 14, 15, 5, tzinfo=shanghai)
    one_second_early = same_instant - timedelta(seconds=1)

    assert len(store.query_chunks(KnowledgeQuery("研究", same_instant, top_k=10))) == 1
    assert store.query_chunks(KnowledgeQuery("研究", one_second_early, top_k=10)) == []


def test_new_version_supersedes_old_and_creates_delete_then_upsert_jobs(tmp_path) -> None:
    store = KnowledgeStore(tmp_path / "knowledge.db")
    v1 = _document(status=KnowledgeDocumentStatus.FINALIZED)
    c1 = _chunk(v1)
    store.ingest(v1, (c1,), "fixture")
    old_job = store.claim_index_jobs("indexer", limit=1)[0]
    store.complete_index_job(old_job.job_id)
    v2 = _document(version=2, status=KnowledgeDocumentStatus.FINALIZED, content="中际旭创周度研究第二版。")
    c2 = _chunk(v2, text="状态变化为MAIN_UPTREND_CONFIRMED。")
    result = store.ingest(v2, (c2,), "fixture")

    assert result.index_jobs_created == 2
    assert store.get_document(v1.document_id).version == 2
    assert store.get_document(v1.document_id, 1).status == KnowledgeDocumentStatus.SUPERSEDED
    claimed = store.claim_index_jobs("test-worker", limit=10)
    assert [job.operation for job in claimed] == [
        IndexJobOperation.DELETE, IndexJobOperation.UPSERT
    ]


def test_superseded_pending_upsert_is_cancelled_without_wasted_delete(tmp_path) -> None:
    db_path = tmp_path / "knowledge.db"
    store = KnowledgeStore(db_path)
    v1 = _document(status=KnowledgeDocumentStatus.FINALIZED)
    store.ingest(v1, (_chunk(v1),), "fixture")
    v2 = _document(version=2, status=KnowledgeDocumentStatus.FINALIZED, content="第二版。")
    result = store.ingest(v2, (_chunk(v2, text="第二版状态。"),), "fixture")

    assert result.index_jobs_created == 1
    jobs = store.claim_index_jobs("worker", limit=10)
    assert len(jobs) == 1
    assert jobs[0].document_version == 2
    with sqlite3.connect(db_path) as conn:
        cancelled = conn.execute("SELECT COUNT(*) FROM knowledge_index_jobs WHERE status='CANCELLED'").fetchone()[0]
        deletes = conn.execute("SELECT COUNT(*) FROM knowledge_index_jobs WHERE operation='DELETE'").fetchone()[0]
    assert cancelled == 1
    assert deletes == 0


def test_same_version_document_mutation_is_rejected_and_previous_value_survives(tmp_path) -> None:
    db_path = tmp_path / "knowledge.db"
    store = KnowledgeStore(db_path)
    original = _document()
    store.ingest(original, (_chunk(original),), "fixture")
    changed = _document(content="同一版本被静默修改。")

    with pytest.raises(VersionConflictError):
        store.ingest(changed, (_chunk(changed),), "fixture")

    assert store.get_document(original.document_id).content == original.content
    with sqlite3.connect(db_path) as conn:
        failed = conn.execute("SELECT COUNT(*) FROM knowledge_ingestion_runs WHERE status='FAILED'").fetchone()[0]
    assert failed == 1


def test_invalid_bundle_rolls_back_entire_batch(tmp_path) -> None:
    db_path = tmp_path / "knowledge.db"
    store = KnowledgeStore(db_path)
    first = _document()
    second = _document(document_id="weekly/688146.SH/2026-W29", ticker="688146.SH")
    wrong_chunk = _chunk(first, "wrong")

    with pytest.raises(ValueError, match="does not match"):
        store.ingest_batch(
            (KnowledgeBundle(first, (_chunk(first),)), KnowledgeBundle(second, (wrong_chunk,))),
            "fixture",
        )

    assert store.table_count("knowledge_documents") == 0
    assert store.table_count("knowledge_chunks") == 0
    assert store.table_count("knowledge_index_jobs") == 0


def test_index_jobs_have_atomic_claim_and_terminal_state(tmp_path) -> None:
    store = KnowledgeStore(tmp_path / "knowledge.db")
    document = _document()
    store.ingest(document, (_chunk(document),), "fixture")
    claimed = store.claim_index_jobs("worker-1", limit=1)

    assert len(claimed) == 1
    assert claimed[0].status == IndexJobStatus.PROCESSING
    assert claimed[0].attempt_count == 1
    assert store.claim_index_jobs("worker-2", limit=1) == []
    store.complete_index_job(claimed[0].job_id)
    with pytest.raises(ValueError, match="PROCESSING"):
        store.complete_index_job(claimed[0].job_id)


def test_failed_job_can_be_retried(tmp_path) -> None:
    store = KnowledgeStore(tmp_path / "knowledge.db")
    document = _document()
    store.ingest(document, (_chunk(document),), "fixture")
    job = store.claim_index_jobs("worker-1", limit=1)[0]
    store.fail_index_job(job.job_id, "EmbeddingTimeout")
    store.retry_failed_index_job(job.job_id)
    retried = store.claim_index_jobs("worker-2", limit=1)[0]
    assert retried.job_id == job.job_id
    assert retried.attempt_count == 2


def test_stale_processing_job_can_be_recovered(tmp_path) -> None:
    store = KnowledgeStore(tmp_path / "knowledge.db")
    document = _document()
    store.ingest(document, (_chunk(document),), "fixture")
    claimed = store.claim_index_jobs("dead-worker", limit=1)[0]
    recovered = store.requeue_stale_index_jobs(datetime.now(UTC) + timedelta(seconds=1))
    reclaimed = store.claim_index_jobs("healthy-worker", limit=1)[0]
    assert recovered == 1
    assert reclaimed.job_id == claimed.job_id
    assert reclaimed.attempt_count == 2
