from __future__ import annotations

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


UTC = timezone.utc
AVAILABLE = datetime(2026, 7, 14, 7, 5, tzinfo=UTC)


def _document(**overrides) -> KnowledgeDocument:
    content = overrides.pop("content", "中际旭创在急跌后出现修复，但尚未重新站上MA20。")
    values = {
        "document_id": "weekly/300308.SZ/2026-W29",
        "document_type": KnowledgeDocumentType.WEEKLY_RESEARCH,
        "title": "中际旭创周度研究",
        "content": content,
        "tickers": ("300308.SZ",),
        "themes": ("AI_INFRASTRUCTURE", "OPTICAL_MODULE"),
        "thesis_id": "thesis/300308.SZ/2025-07",
        "event_time": datetime(2026, 7, 14, 7, 0, tzinfo=UTC),
        "as_of": datetime(2026, 7, 14, 7, 0, tzinfo=UTC),
        "available_at": AVAILABLE,
        "status": KnowledgeDocumentStatus.DRAFT,
        "version": 1,
        "source_type": KnowledgeSourceType.SYSTEM_DERIVED,
        "source_uri": "sqlite://weekly_documents/weekly%2F300308.SZ%2F2026-W29",
        "source_hash": canonical_json_sha256({"source_run_id": "run-1", "version": 1}),
        "content_hash": content_sha256(content),
        "reliability": KnowledgeReliability.DERIVED,
        "language": "zh-CN",
        "metadata": {"score_version": "reversal-score-v1.0.0", "risk_flags": []},
        "created_at": AVAILABLE,
        "updated_at": AVAILABLE,
    }
    values.update(overrides)
    return KnowledgeDocument(**values)


def _chunk(**overrides) -> KnowledgeChunk:
    text = overrides.pop("text", "状态从WATCHLIST变为EARLY_STABILIZATION。")
    values = {
        "chunk_id": "weekly/300308.SZ/2026-W29::state-change::2026-07-14",
        "document_id": "weekly/300308.SZ/2026-W29",
        "document_version": 1,
        "chunk_type": KnowledgeChunkType.STATE_CHANGE,
        "section": "State Change",
        "text": text,
        "ordinal": 1,
        "event_time": datetime(2026, 7, 14, 7, 0, tzinfo=UTC),
        "available_at": AVAILABLE,
        "content_hash": content_sha256(text),
        "token_count": 8,
        "indexable": True,
        "metadata": {"old_status": "WATCHLIST", "new_status": "EARLY_STABILIZATION"},
    }
    values.update(overrides)
    return KnowledgeChunk(**values)


def test_document_enforces_content_hash_and_temporal_visibility() -> None:
    document = _document()
    assert not document.visible_at(AVAILABLE - timedelta(seconds=1))
    assert document.visible_at(AVAILABLE)
    with pytest.raises(ValueError, match="content_hash"):
        _document(content_hash="0" * 64)


def test_retracted_and_superseded_documents_are_not_retrievable() -> None:
    for status in (KnowledgeDocumentStatus.RETRACTED, KnowledgeDocumentStatus.SUPERSEDED):
        assert not _document(status=status).visible_at(AVAILABLE + timedelta(days=1))


def test_contract_rejects_naive_datetimes_and_non_json_metadata() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _document(available_at=datetime(2026, 7, 14, 15, 5))
    with pytest.raises(ValueError, match="JSON-serializable"):
        _document(metadata={"bad": {1, 2, 3}})
    with pytest.raises(ValueError, match="finite JSON"):
        _document(metadata={"bad": float("nan")})


def test_chunk_controls_visibility_independently_from_weekly_draft() -> None:
    document = _document()
    event = _chunk()
    assert event.visible_at(AVAILABLE)
    assert document.chunk_visible_at(event, AVAILABLE)
    draft_text = "本周尚未结束。"
    draft = _chunk(
        chunk_id="weekly/300308.SZ/2026-W29::draft",
        chunk_type=KnowledgeChunkType.DRAFT_SUMMARY,
        text=draft_text,
        content_hash=content_sha256(draft_text),
        indexable=False,
    )
    assert not draft.visible_at(AVAILABLE + timedelta(days=1))
    with pytest.raises(ValueError, match="must not be indexable"):
        _chunk(chunk_type=KnowledgeChunkType.DRAFT_SUMMARY, indexable=True)
    with pytest.raises(ValueError, match="does not match"):
        document.chunk_visible_at(_chunk(document_version=2), AVAILABLE)


def test_query_carries_metadata_and_point_in_time_filters() -> None:
    query = KnowledgeQuery(
        query_text="中际旭创为什么只是初步止跌？",
        as_of=AVAILABLE,
        tickers=("300308.SZ",),
        document_types=(KnowledgeDocumentType.WEEKLY_RESEARCH, KnowledgeDocumentType.THESIS_UPDATE),
        reliability=(KnowledgeReliability.DERIVED, KnowledgeReliability.PRIMARY),
        top_k=3,
    )
    assert query.as_of == AVAILABLE
    assert query.top_k == 3
    with pytest.raises(ValueError, match="RETRACTED"):
        KnowledgeQuery("bad", AVAILABLE, statuses=(KnowledgeDocumentStatus.RETRACTED,))


def test_hashing_is_deterministic_and_metadata_order_independent() -> None:
    assert canonical_json_sha256({"b": 2, "a": 1}) == canonical_json_sha256({"a": 1, "b": 2})
    assert content_sha256("研究结论") == content_sha256("研究结论")
