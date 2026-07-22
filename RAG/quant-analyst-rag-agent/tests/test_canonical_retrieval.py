from __future__ import annotations

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
from quant_agent.knowledge.store import KnowledgeStore
from quant_agent.query.service import RAGQueryService
from quant_agent.retrieval.index_worker import KnowledgeIndexWorker
from quant_agent.retrieval.lexical import CanonicalLexicalIndex, tokenize_lexical
from quant_agent.retrieval.markdown_migration import migrate_markdown_documents


UTC = timezone.utc
T0 = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)


def _document(
    content: str,
    *,
    version: int = 1,
    available_at: datetime = T0,
    status: KnowledgeDocumentStatus = KnowledgeDocumentStatus.FINALIZED,
) -> KnowledgeDocument:
    return KnowledgeDocument(
        document_id="research/korea-semiconductor",
        document_type=KnowledgeDocumentType.MACRO_VIEWPOINT,
        title="韩国加息与亚洲半导体",
        content=content,
        tickers=(),
        themes=("ASIA_SEMICONDUCTOR",),
        thesis_id=None,
        event_time=available_at - timedelta(hours=1),
        as_of=available_at,
        available_at=available_at,
        status=status,
        version=version,
        source_type=KnowledgeSourceType.MANUAL_NOTE,
        source_uri="private://fixture",
        source_hash=content_sha256(content),
        content_hash=content_sha256(content),
        reliability=KnowledgeReliability.SECONDARY,
        language="zh-CN",
        created_at=T0,
        updated_at=available_at,
        metadata={"fixture": True},
    )


def _chunk(document: KnowledgeDocument, text: str) -> KnowledgeChunk:
    return KnowledgeChunk(
        chunk_id=f"{document.document_id}::0000",
        document_id=document.document_id,
        document_version=document.version,
        chunk_type=KnowledgeChunkType.BODY,
        section="Research",
        text=text,
        ordinal=0,
        event_time=document.event_time,
        available_at=document.available_at,
        content_hash=content_sha256(text),
        token_count=max(1, len(text)),
        indexable=True,
        metadata={"fixture": True},
    )


def _runtime(tmp_path):
    db_path = tmp_path / "knowledge.db"
    store = KnowledgeStore(db_path)
    index = CanonicalLexicalIndex(db_path)
    return store, index, KnowledgeIndexWorker(store, index)


def test_bilingual_tokenizer_adds_cjk_and_domain_aliases() -> None:
    tokens = tokenize_lexical("BOK 韩国央行加息是否冲击半导体 chip？")

    assert "韩国" in tokens
    assert "半导体" in tokens
    assert "bank_of_korea" in tokens
    assert "semiconductor" in tokens
    assert "artificial_intelligence" not in tokenize_lexical("selloff repair")
    assert "artificial_intelligence" in tokenize_lexical("AI infrastructure")


def test_outbox_worker_indexes_chinese_and_query_service_returns_typed_evidence(tmp_path) -> None:
    store, index, worker = _runtime(tmp_path)
    document = _document("韩国央行加息可能通过外资去杠杆影响亚洲半导体。")
    store.ingest(document, (_chunk(document, document.content),), "fixture")

    sync = worker.sync()
    response = RAGQueryService(index).search(RAGQueryRequest("韩国加息 半导体", T0, top_k=3))

    assert sync.completed == 1
    assert sync.failed == 0
    assert len(response.evidence) == 1
    assert response.evidence[0].document_id == document.document_id
    assert response.evidence[0].reason_codes == ("LEXICAL_MATCH", "POINT_IN_TIME_VISIBLE")
    assert worker.sync().claimed == 0
    assert index.count() == 1


def test_point_in_time_filter_excludes_future_available_document(tmp_path) -> None:
    store, index, worker = _runtime(tmp_path)
    future = _document("未来才发布的韩国半导体研究。", available_at=T0 + timedelta(days=1))
    store.ingest(future, (_chunk(future, future.content),), "fixture")
    worker.sync()

    before = index.search(RAGQueryRequest("韩国半导体", T0))
    after = index.search(RAGQueryRequest("韩国半导体", T0 + timedelta(days=2)))

    assert before == []
    assert len(after) == 1


def test_new_version_removes_superseded_chunk_from_lexical_index(tmp_path) -> None:
    store, index, worker = _runtime(tmp_path)
    v1 = _document("旧主题提到存储周期。")
    store.ingest(v1, (_chunk(v1, v1.content),), "fixture")
    worker.sync()
    v2 = _document("新主题提到先进封装。", version=2, available_at=T0 + timedelta(hours=1))
    store.ingest(v2, (_chunk(v2, v2.content),), "fixture")

    sync = worker.sync()

    assert sync.deleted == 1
    assert sync.upserted == 1
    assert index.search(RAGQueryRequest("存储周期", T0 + timedelta(hours=2))) == []
    assert len(index.search(RAGQueryRequest("先进封装", T0 + timedelta(hours=2)))) == 1
    assert index.count() == 1


def test_markdown_migration_is_idempotent_and_content_change_creates_version(tmp_path) -> None:
    docs = tmp_path / "data" / "docs" / "factor_definitions"
    docs.mkdir(parents=True)
    markdown = docs / "liquidity.md"
    markdown.write_text("# 流动性\n\n资金价格与真实利率。", encoding="utf-8")
    store, index, worker = _runtime(tmp_path)

    first = migrate_markdown_documents(store, tmp_path / "data" / "docs", project_root=tmp_path)
    worker.sync()
    second = migrate_markdown_documents(store, tmp_path / "data" / "docs", project_root=tmp_path)
    markdown.write_text("# 流动性\n\n资金价格、真实利率与久期供给。", encoding="utf-8")
    third = migrate_markdown_documents(store, tmp_path / "data" / "docs", project_root=tmp_path)
    worker.sync()

    assert first.migrated_documents == 1
    assert second.migrated_documents == 0
    assert second.skipped_unchanged == 1
    assert third.migrated_documents == 1
    assert store.get_document("markdown/factor_definitions/liquidity").version == 2
    assert index.count() == 1
