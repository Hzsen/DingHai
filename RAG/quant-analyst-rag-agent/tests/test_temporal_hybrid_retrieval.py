from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from domain.knowledge import (
    KnowledgeChunk,
    KnowledgeChunkType,
    KnowledgeDocument,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeReliability,
    KnowledgeSourceType,
    content_sha256,
)
from domain.query import RAGQueryRequest
from quant_agent.knowledge.store import KnowledgeBundle, KnowledgeStore
from quant_agent.query.service import RAGQueryService
from quant_agent.retrieval.canonical_vector import CanonicalVectorIndex
from quant_agent.retrieval.index_worker import KnowledgeIndexWorker
from quant_agent.retrieval.lexical import CanonicalLexicalIndex


UTC = timezone.utc
T0 = datetime(2026, 7, 14, 7, 10, tzinfo=UTC)


def _bundle(
    document_id: str,
    text: str,
    *,
    ticker: str = "300308.SZ",
    version: int = 1,
    available_at: datetime = T0,
) -> tuple[KnowledgeDocument, tuple[KnowledgeChunk, ...]]:
    document = KnowledgeDocument(
        document_id=document_id,
        document_type=KnowledgeDocumentType.WEEKLY_RESEARCH,
        title="研究记录",
        content=text,
        tickers=(ticker,),
        themes=("AI_INFRASTRUCTURE",),
        thesis_id=f"thesis/{ticker}",
        event_time=available_at - timedelta(minutes=10),
        as_of=available_at,
        available_at=available_at,
        status=KnowledgeDocumentStatus.FINALIZED,
        version=version,
        source_type=KnowledgeSourceType.SYSTEM_DERIVED,
        source_uri=f"fixture://{document_id}",
        source_hash=content_sha256(text),
        content_hash=content_sha256(text),
        reliability=KnowledgeReliability.DERIVED,
        language="zh-CN",
        created_at=T0,
        updated_at=max(T0, available_at),
        metadata={"fixture": True},
    )
    chunk = KnowledgeChunk(
        chunk_id=f"{document_id}::summary",
        document_id=document_id,
        document_version=version,
        chunk_type=KnowledgeChunkType.WEEKLY_SUMMARY,
        section="Summary",
        text=text,
        ordinal=0,
        event_time=document.event_time,
        available_at=available_at,
        content_hash=content_sha256(text),
        token_count=max(1, len(text)),
        indexable=True,
        metadata={"fixture": True},
    )
    return document, (chunk,)


def _runtime(tmp_path):
    db_path = tmp_path / "knowledge.db"
    store = KnowledgeStore(db_path)
    lexical = CanonicalLexicalIndex(db_path)
    vector = CanonicalVectorIndex(db_path)
    worker = KnowledgeIndexWorker(store, lexical, vector)
    return store, lexical, vector, worker


def test_semantic_concept_recalls_chinese_main_uptrend_from_english_query(tmp_path) -> None:
    store, lexical, vector, worker = _runtime(tmp_path)
    document, chunks = _bundle("weekly/leader", "资金和相对强度共同确认主升浪。")
    store.ingest(document, chunks, "fixture")
    worker.sync()

    request = RAGQueryRequest("leader trend confirmation", T0, top_k=3)
    assert lexical.search(request) == []
    response = RAGQueryService(lexical, vector).search(request)

    assert len(response.evidence) == 1
    assert response.evidence[0].document_id == document.document_id
    assert response.evidence[0].lexical_score == 0
    assert response.evidence[0].semantic_score > 0
    assert response.evidence[0].reason_codes == ("SEMANTIC_MATCH", "POINT_IN_TIME_VISIBLE")


def test_temporal_filter_blocks_future_chunk_in_both_retrieval_paths(tmp_path) -> None:
    store, lexical, vector, worker = _runtime(tmp_path)
    future_time = T0 + timedelta(days=1)
    document, chunks = _bundle(
        "weekly/future",
        "光模块主升浪出现新的价格突破。",
        available_at=future_time,
    )
    store.ingest(document, chunks, "fixture")
    worker.sync()

    before = RAGQueryService(lexical, vector).search(
        RAGQueryRequest("optical module main uptrend", T0)
    )
    after = RAGQueryService(lexical, vector).search(
        RAGQueryRequest("optical module main uptrend", future_time + timedelta(minutes=1))
    )

    assert before.evidence == ()
    assert len(after.evidence) == 1
    assert after.evidence[0].available_at == future_time


def test_ticker_filter_is_applied_before_vector_scoring(tmp_path) -> None:
    store, lexical, vector, worker = _runtime(tmp_path)
    first, first_chunks = _bundle("weekly/first", "主升浪趋势确认。", ticker="300308.SZ")
    second, second_chunks = _bundle("weekly/second", "主升浪趋势确认。", ticker="688146.SH")
    store.ingest_batch(
        (
            KnowledgeBundle(first, first_chunks),
            KnowledgeBundle(second, second_chunks),
        ),
        "fixture",
    )
    worker.sync()

    response = RAGQueryService(lexical, vector).search(
        RAGQueryRequest("leader trend", T0, tickers=("688146.SH",))
    )

    assert [item.document_id for item in response.evidence] == ["weekly/second"]


def test_vector_reconcile_backfills_jobs_completed_before_vector_index_existed(tmp_path) -> None:
    db_path = tmp_path / "knowledge.db"
    store = KnowledgeStore(db_path)
    lexical = CanonicalLexicalIndex(db_path)
    document, chunks = _bundle("weekly/backfill", "急跌修复与相对强度。")
    store.ingest(document, chunks, "fixture")
    KnowledgeIndexWorker(store, lexical).sync()
    vector = CanonicalVectorIndex(db_path)

    result = vector.reconcile(store)
    second = vector.reconcile(store)

    assert result.canonical_chunks == 1
    assert result.inserted_or_updated == 1
    assert result.indexed_vectors == 1
    assert second.inserted_or_updated == 0
    assert second.deleted_stale == 0


def test_vector_failure_does_not_ack_job_and_retry_is_idempotent(tmp_path) -> None:
    class FailOnceVectorIndex(CanonicalVectorIndex):
        def __init__(self, db_path) -> None:
            super().__init__(db_path)
            self.failed = False

        def upsert(self, stored) -> None:
            if not self.failed:
                self.failed = True
                raise RuntimeError("synthetic vector failure")
            super().upsert(stored)

    db_path = tmp_path / "knowledge.db"
    store = KnowledgeStore(db_path)
    lexical = CanonicalLexicalIndex(db_path)
    vector = FailOnceVectorIndex(db_path)
    document, chunks = _bundle("weekly/retry", "价格突破后的主升浪。")
    store.ingest(document, chunks, "fixture")

    failed = KnowledgeIndexWorker(store, lexical, vector).sync()
    with sqlite3.connect(db_path) as conn:
        job_id = conn.execute(
            "SELECT job_id FROM knowledge_index_jobs WHERE status='FAILED'"
        ).fetchone()[0]
    store.retry_failed_index_job(job_id)
    retried = KnowledgeIndexWorker(store, lexical, vector).sync()

    assert failed.failed == 1
    assert failed.completed == 0
    assert lexical.count() == 1
    assert retried.completed == 1
    assert retried.failed == 0
    assert vector.count() == 1
    assert store.index_job_counts()["COMPLETED"] == 1


def test_new_version_retires_old_version_from_both_indexes(tmp_path) -> None:
    store, lexical, vector, worker = _runtime(tmp_path)
    old, old_chunks = _bundle("weekly/versioned", "旧观点是急跌修复。")
    store.ingest(old, old_chunks, "fixture")
    worker.sync()
    new, new_chunks = _bundle(
        "weekly/versioned",
        "新观点是主升浪确认。",
        version=2,
        available_at=T0 + timedelta(hours=1),
    )
    store.ingest(new, new_chunks, "fixture")

    result = worker.sync()
    old_response = RAGQueryService(lexical, vector).search(
        RAGQueryRequest("selloff repair", T0 + timedelta(hours=2))
    )
    new_response = RAGQueryService(lexical, vector).search(
        RAGQueryRequest("leader trend", T0 + timedelta(hours=2))
    )

    assert result.deleted == 1
    assert result.upserted == 1
    assert old_response.evidence == ()
    assert len(new_response.evidence) == 1
    assert lexical.count() == vector.count() == 1
