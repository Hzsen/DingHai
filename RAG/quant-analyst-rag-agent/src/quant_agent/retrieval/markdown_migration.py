from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

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
from quant_agent.knowledge.store import KnowledgeBundle, KnowledgeStore


_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
_CJK = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
_TICKER = re.compile(r"\b(?:[036]\d{5}\.(?:SZ|SH)|[A-Z]{1,6})\b")


@dataclass(frozen=True, slots=True)
class MarkdownMigrationResult:
    discovered_files: int
    migrated_documents: int
    skipped_unchanged: int
    chunks_created: int
    index_jobs_created: int
    run_id: str | None


def _document_type(relative_path: Path) -> KnowledgeDocumentType:
    parts = {part.lower() for part in relative_path.parts}
    if "factor_definitions" in parts:
        return KnowledgeDocumentType.FACTOR_DEFINITION
    if "adr" in parts:
        return KnowledgeDocumentType.ADR
    if "macro" in parts:
        return KnowledgeDocumentType.MARKET_REGIME
    return KnowledgeDocumentType.THEME_RESEARCH


def _chunk_type(document_type: KnowledgeDocumentType) -> KnowledgeChunkType:
    if document_type is KnowledgeDocumentType.FACTOR_DEFINITION:
        return KnowledgeChunkType.FACTOR_DEFINITION
    return KnowledgeChunkType.BODY


def _title(content: str, path: Path) -> str:
    match = _HEADING.search(content)
    return match.group(2).strip() if match and len(match.group(1)) == 1 else path.stem.replace("_", " ").title()


def _sections(content: str, fallback_title: str) -> list[tuple[str, str]]:
    matches = list(_HEADING.finditer(content))
    if not matches:
        return [(fallback_title, content.strip())]
    sections: list[tuple[str, str]] = []
    prefix = content[:matches[0].start()].strip()
    if prefix:
        sections.append((fallback_title, prefix))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        text = content[match.start():end].strip()
        if text:
            sections.append((match.group(2).strip(), text))
    return sections


def _split_text(text: str, *, max_chars: int, overlap_chars: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    output: list[str] = []
    start = 0
    while start < len(text):
        target = min(start + max_chars, len(text))
        end = target
        if target < len(text):
            candidates = [text.rfind(marker, start + max_chars // 2, target) for marker in ("\n\n", "。", ". ")]
            end = max(candidates)
            if end <= start:
                end = target
            elif text[end:end + 2] == "\n\n":
                end += 2
            else:
                end += 1
        piece = text[start:end].strip()
        if piece:
            output.append(piece)
        if end >= len(text):
            break
        start = max(end - overlap_chars, start + 1)
    return output


def _token_count(text: str) -> int:
    latin_words = len(re.findall(r"[A-Za-z0-9_]+", text))
    cjk_chars = len(_CJK.findall(text))
    return max(1, latin_words + cjk_chars)


def _document_id(relative_path: Path) -> str:
    return f"markdown/{relative_path.with_suffix('').as_posix()}"


def migrate_markdown_documents(
    store: KnowledgeStore,
    docs_dir: Path | str,
    *,
    project_root: Path | str | None = None,
    max_chunk_chars: int = 2_000,
    overlap_chars: int = 200,
) -> MarkdownMigrationResult:
    """Idempotently publish Markdown files into the canonical KnowledgeStore."""
    if max_chunk_chars < 200:
        raise ValueError("max_chunk_chars must be >= 200")
    if not 0 <= overlap_chars < max_chunk_chars:
        raise ValueError("overlap_chars must be >= 0 and smaller than max_chunk_chars")
    root = Path(docs_dir).resolve()
    files = sorted(root.rglob("*.md")) if root.exists() else []
    bundles: list[KnowledgeBundle] = []
    skipped = 0
    chunk_count = 0
    source_root = Path(project_root).resolve() if project_root else root.parent.parent

    for path in files:
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            continue
        relative = path.relative_to(root)
        document_id = _document_id(relative)
        content_hash = content_sha256(content)
        latest = store.get_document(document_id)
        if latest is not None and latest.content_hash == content_hash:
            skipped += 1
            continue

        version = 1 if latest is None else latest.version + 1
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if latest is not None:
            modified_at = max(modified_at, latest.updated_at)
        created_at = latest.created_at if latest is not None else modified_at
        document_type = _document_type(relative)
        title = _title(content, path)
        try:
            source_uri = path.relative_to(source_root).as_posix()
        except ValueError:
            source_uri = path.as_posix()
        tickers = tuple(dict.fromkeys(_TICKER.findall(content)))
        document = KnowledgeDocument(
            document_id=document_id,
            document_type=document_type,
            title=title,
            content=content,
            tickers=tickers,
            themes=(),
            thesis_id=None,
            event_time=None,
            as_of=modified_at,
            available_at=modified_at,
            status=KnowledgeDocumentStatus.FINALIZED,
            version=version,
            source_type=KnowledgeSourceType.MANUAL_NOTE,
            source_uri=source_uri,
            source_hash=content_hash,
            content_hash=content_hash,
            reliability=KnowledgeReliability.SECONDARY,
            language="zh-CN" if _CJK.search(content) else "en",
            created_at=created_at,
            updated_at=modified_at,
            metadata={"adapter": "markdown-migration-v1", "relative_path": relative.as_posix()},
        )
        chunks: list[KnowledgeChunk] = []
        ordinal = 0
        for section, section_text in _sections(content, title):
            for piece in _split_text(section_text, max_chars=max_chunk_chars, overlap_chars=overlap_chars):
                chunks.append(KnowledgeChunk(
                    chunk_id=f"{document_id}::{ordinal:04d}",
                    document_id=document_id,
                    document_version=version,
                    chunk_type=_chunk_type(document_type),
                    section=section,
                    text=piece,
                    ordinal=ordinal,
                    event_time=None,
                    available_at=modified_at,
                    content_hash=content_sha256(piece),
                    token_count=_token_count(piece),
                    indexable=True,
                    metadata={"relative_path": relative.as_posix()},
                ))
                ordinal += 1
        bundles.append(KnowledgeBundle(document, tuple(chunks)))
        chunk_count += len(chunks)

    if not bundles:
        return MarkdownMigrationResult(len(files), 0, skipped, 0, 0, None)
    result = store.ingest_batch(tuple(bundles), "markdown-one-time-migration-v1")
    return MarkdownMigrationResult(
        discovered_files=len(files),
        migrated_documents=len(bundles),
        skipped_unchanged=skipped,
        chunks_created=chunk_count,
        index_jobs_created=result.index_jobs_created,
        run_id=result.run_id,
    )
