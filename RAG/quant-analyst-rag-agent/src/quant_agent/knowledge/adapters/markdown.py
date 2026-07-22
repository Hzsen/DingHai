from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from domain.knowledge import (
    KnowledgeChunkType,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeReliability,
    KnowledgeSourceType,
    content_sha256,
)
from quant_agent.knowledge.adapters.base import KnowledgeChunkDraft, KnowledgeDocumentDraft


_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
_CJK = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
_TICKER = re.compile(r"\b(?:[036]\d{5}\.(?:SZ|SH)|[A-Z]{1,6})\b")


def _document_type(relative_path: Path) -> KnowledgeDocumentType:
    parts = {part.lower() for part in relative_path.parts}
    if "factor_definitions" in parts:
        return KnowledgeDocumentType.FACTOR_DEFINITION
    if "adr" in parts:
        return KnowledgeDocumentType.ADR
    if "macro" in parts:
        return KnowledgeDocumentType.MARKET_REGIME
    return KnowledgeDocumentType.THEME_RESEARCH


def _sections(content: str, fallback_title: str) -> list[tuple[str, str]]:
    matches = list(_HEADING.finditer(content))
    if not matches:
        return [(fallback_title, content)]
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


def _split(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    output: list[str] = []
    start = 0
    while start < len(text):
        target = min(start + max_chars, len(text))
        end = target
        if target < len(text):
            candidates = [text.rfind(mark, start + max_chars // 2, target) for mark in ("\n\n", "。", ". ")]
            end = max(candidates)
            end = target if end <= start else end + (2 if text[end:end + 2] == "\n\n" else 1)
        piece = text[start:end].strip()
        if piece:
            output.append(piece)
        if end >= len(text):
            break
        start = max(end - overlap_chars, start + 1)
    return output


class StaticMarkdownAdapter:
    source_name = "static-markdown-adapter-v1"

    def __init__(
        self,
        docs_dir: Path | str,
        *,
        project_root: Path | str | None = None,
        max_chunk_chars: int = 2_000,
        overlap_chars: int = 200,
    ) -> None:
        if max_chunk_chars < 200:
            raise ValueError("max_chunk_chars must be >= 200")
        if not 0 <= overlap_chars < max_chunk_chars:
            raise ValueError("overlap_chars must be smaller than max_chunk_chars")
        self.docs_dir = Path(docs_dir).resolve()
        self.project_root = Path(project_root).resolve() if project_root else self.docs_dir.parent.parent
        self.max_chunk_chars = max_chunk_chars
        self.overlap_chars = overlap_chars

    def load(self) -> tuple[KnowledgeDocumentDraft, ...]:
        files = sorted(self.docs_dir.rglob("*.md")) if self.docs_dir.exists() else []
        drafts: list[KnowledgeDocumentDraft] = []
        for path in files:
            content = path.read_text(encoding="utf-8").strip()
            if not content:
                continue
            relative = path.relative_to(self.docs_dir)
            match = _HEADING.search(content)
            title = match.group(2).strip() if match and len(match.group(1)) == 1 else path.stem.replace("_", " ").title()
            modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            document_type = _document_type(relative)
            document_id = f"markdown/{relative.with_suffix('').as_posix()}"
            try:
                source_uri = path.relative_to(self.project_root).as_posix()
            except ValueError:
                source_uri = path.as_posix()
            chunk_type = (
                KnowledgeChunkType.FACTOR_DEFINITION
                if document_type is KnowledgeDocumentType.FACTOR_DEFINITION
                else KnowledgeChunkType.BODY
            )
            chunks: list[KnowledgeChunkDraft] = []
            ordinal = 0
            for section, section_text in _sections(content, title):
                for piece in _split(section_text, self.max_chunk_chars, self.overlap_chars):
                    chunks.append(KnowledgeChunkDraft(
                        chunk_id=f"{document_id}::{ordinal:04d}",
                        chunk_type=chunk_type,
                        section=section,
                        text=piece,
                        ordinal=ordinal,
                        event_time=None,
                        available_at=modified_at,
                        indexable=True,
                        metadata={"relative_path": relative.as_posix()},
                    ))
                    ordinal += 1
            drafts.append(KnowledgeDocumentDraft(
                document_id=document_id,
                document_type=document_type,
                title=title,
                content=content,
                tickers=tuple(dict.fromkeys(_TICKER.findall(content))),
                themes=(),
                thesis_id=None,
                event_time=None,
                as_of=modified_at,
                available_at=modified_at,
                status=KnowledgeDocumentStatus.FINALIZED,
                source_type=KnowledgeSourceType.MANUAL_NOTE,
                source_uri=source_uri,
                source_hash=content_sha256(content),
                reliability=KnowledgeReliability.SECONDARY,
                language="zh-CN" if _CJK.search(content) else "en",
                created_at=modified_at,
                updated_at=modified_at,
                metadata={
                    "adapter": self.source_name,
                    "relative_path": relative.as_posix(),
                    "availability_semantics": "filesystem_mtime",
                },
                chunks=tuple(chunks),
            ))
        return tuple(drafts)
