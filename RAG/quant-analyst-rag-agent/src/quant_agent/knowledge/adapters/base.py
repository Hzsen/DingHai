from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from domain.knowledge import (
    JsonValue,
    KnowledgeChunk,
    KnowledgeChunkType,
    KnowledgeDocument,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeReliability,
    KnowledgeSourceType,
    content_sha256,
)
from quant_agent.knowledge.store import KnowledgeBundle, KnowledgeStore


_CJK = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")


def estimate_token_count(text: str) -> int:
    """Cheap deterministic estimate; exact model tokenization belongs at the LLM boundary."""
    latin_words = len(re.findall(r"[A-Za-z0-9_]+", text))
    return max(1, latin_words + len(_CJK.findall(text)))


@dataclass(frozen=True, slots=True)
class KnowledgeChunkDraft:
    chunk_id: str
    chunk_type: KnowledgeChunkType
    section: str
    text: str
    ordinal: int
    event_time: datetime | None
    available_at: datetime
    indexable: bool
    metadata: dict[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KnowledgeDocumentDraft:
    document_id: str
    document_type: KnowledgeDocumentType
    title: str
    content: str
    tickers: tuple[str, ...]
    themes: tuple[str, ...]
    thesis_id: str | None
    event_time: datetime | None
    as_of: datetime
    available_at: datetime
    status: KnowledgeDocumentStatus
    source_type: KnowledgeSourceType
    source_uri: str | None
    source_hash: str
    reliability: KnowledgeReliability
    language: str
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, JsonValue]
    chunks: tuple[KnowledgeChunkDraft, ...]


class KnowledgeAdapter(Protocol):
    source_name: str

    def load(self) -> tuple[KnowledgeDocumentDraft, ...]: ...


@dataclass(frozen=True, slots=True)
class AdapterIngestionResult:
    source_name: str
    discovered_documents: int
    migrated_documents: int
    skipped_unchanged: int
    migrated_chunks: int
    index_jobs_created: int
    run_id: str | None


class KnowledgeMigrationService:
    """Resolve canonical versions and atomically publish one adapter snapshot."""

    def __init__(self, store: KnowledgeStore) -> None:
        self.store = store

    def migrate(self, adapter: KnowledgeAdapter) -> AdapterIngestionResult:
        drafts = adapter.load()
        document_ids = [draft.document_id for draft in drafts]
        if len(document_ids) != len(set(document_ids)):
            raise ValueError(f"adapter {adapter.source_name} emitted duplicate document_id values")

        bundles: list[KnowledgeBundle] = []
        skipped = 0
        for draft in drafts:
            latest = self.store.get_document(draft.document_id)
            if self._is_unchanged(latest, draft):
                skipped += 1
                continue
            version = 1 if latest is None else latest.version + 1
            created_at = draft.created_at if latest is None else latest.created_at
            document = KnowledgeDocument(
                document_id=draft.document_id,
                document_type=draft.document_type,
                title=draft.title,
                content=draft.content,
                tickers=draft.tickers,
                themes=draft.themes,
                thesis_id=draft.thesis_id,
                event_time=draft.event_time,
                as_of=draft.as_of,
                available_at=draft.available_at,
                status=draft.status,
                version=version,
                source_type=draft.source_type,
                source_uri=draft.source_uri,
                source_hash=draft.source_hash,
                content_hash=content_sha256(draft.content),
                reliability=draft.reliability,
                language=draft.language,
                created_at=created_at,
                updated_at=max(draft.updated_at, created_at),
                metadata=draft.metadata,
            )
            chunks = tuple(
                KnowledgeChunk(
                    chunk_id=chunk.chunk_id,
                    document_id=draft.document_id,
                    document_version=version,
                    chunk_type=chunk.chunk_type,
                    section=chunk.section,
                    text=chunk.text,
                    ordinal=chunk.ordinal,
                    event_time=chunk.event_time,
                    available_at=chunk.available_at,
                    content_hash=content_sha256(chunk.text),
                    token_count=estimate_token_count(chunk.text),
                    indexable=chunk.indexable,
                    metadata=chunk.metadata,
                )
                for chunk in draft.chunks
            )
            bundles.append(KnowledgeBundle(document, chunks))

        if not bundles:
            return AdapterIngestionResult(
                adapter.source_name, len(drafts), 0, skipped, 0, 0, None
            )
        result = self.store.ingest_batch(tuple(bundles), adapter.source_name)
        return AdapterIngestionResult(
            source_name=adapter.source_name,
            discovered_documents=len(drafts),
            migrated_documents=len(bundles),
            skipped_unchanged=skipped,
            migrated_chunks=sum(len(bundle.chunks) for bundle in bundles),
            index_jobs_created=result.index_jobs_created,
            run_id=result.run_id,
        )

    @staticmethod
    def _is_unchanged(
        latest: KnowledgeDocument | None,
        draft: KnowledgeDocumentDraft,
    ) -> bool:
        if latest is None:
            return False
        return (
            latest.source_hash == draft.source_hash
            and latest.content_hash == content_sha256(draft.content)
            and latest.status is draft.status
            and latest.document_type is draft.document_type
            and latest.tickers == draft.tickers
            and latest.themes == draft.themes
            and latest.thesis_id == draft.thesis_id
        )
