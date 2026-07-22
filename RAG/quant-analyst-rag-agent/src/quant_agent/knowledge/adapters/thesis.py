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
_TICKER = re.compile(r"\b[036]\d{5}\.(?:SZ|SH)\b")


def _chunk_type(section: str) -> KnowledgeChunkType:
    normalized = section.casefold()
    if "state change" in normalized:
        return KnowledgeChunkType.STATE_CHANGE
    if "numeric evidence" in normalized:
        return KnowledgeChunkType.EVIDENCE
    if "factor status" in normalized:
        return KnowledgeChunkType.THESIS
    if "risk" in normalized:
        return KnowledgeChunkType.RISK
    if "summary" in normalized:
        return KnowledgeChunkType.SUMMARY
    return KnowledgeChunkType.BODY


def _sections(content: str, title: str) -> list[tuple[str, str]]:
    matches = list(_HEADING.finditer(content))
    if not matches:
        return [(title, content)]
    output: list[tuple[str, str]] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        text = content[match.start():end].strip()
        if text:
            output.append((match.group(2).strip(), text))
    return output


class ThesisNoteAdapter:
    source_name = "thesis-note-adapter-v1"

    def __init__(self, notes_dir: Path | str, *, project_root: Path | str | None = None) -> None:
        self.notes_dir = Path(notes_dir).resolve()
        self.project_root = Path(project_root).resolve() if project_root else self.notes_dir.parent.parent

    def load(self) -> tuple[KnowledgeDocumentDraft, ...]:
        files = sorted(self.notes_dir.rglob("*.md")) if self.notes_dir.exists() else []
        output: list[KnowledgeDocumentDraft] = []
        for path in files:
            content = path.read_text(encoding="utf-8").strip()
            if not content:
                continue
            relative = path.relative_to(self.notes_dir)
            title_match = _HEADING.search(content)
            title = title_match.group(2).strip() if title_match else path.stem
            tickers = tuple(dict.fromkeys(_TICKER.findall(content)))
            thesis_match = re.search(r"^## Source Thesis\s*\n+([^\n]+)", content, re.MULTILINE)
            thesis_id = thesis_match.group(1).strip().strip("`") if thesis_match else None
            modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            document_id = f"thesis-note/{relative.with_suffix('').as_posix()}"
            try:
                source_uri = path.relative_to(self.project_root).as_posix()
            except ValueError:
                source_uri = path.as_posix()
            chunks = tuple(
                KnowledgeChunkDraft(
                    chunk_id=f"{document_id}::{ordinal:04d}",
                    chunk_type=_chunk_type(section),
                    section=section,
                    text=text,
                    ordinal=ordinal,
                    event_time=None,
                    available_at=modified_at,
                    indexable=True,
                    metadata={"relative_path": relative.as_posix()},
                )
                for ordinal, (section, text) in enumerate(_sections(content, title))
            )
            output.append(KnowledgeDocumentDraft(
                document_id=document_id,
                document_type=KnowledgeDocumentType.THESIS_UPDATE,
                title=title,
                content=content,
                tickers=tickers,
                themes=(),
                thesis_id=thesis_id,
                event_time=None,
                as_of=modified_at,
                available_at=modified_at,
                status=KnowledgeDocumentStatus.FINALIZED,
                source_type=KnowledgeSourceType.SYSTEM_DERIVED,
                source_uri=source_uri,
                source_hash=content_sha256(content),
                reliability=KnowledgeReliability.DERIVED,
                language="zh-CN",
                created_at=modified_at,
                updated_at=modified_at,
                metadata={
                    "adapter": self.source_name,
                    "relative_path": relative.as_posix(),
                    "availability_semantics": "filesystem_mtime",
                },
                chunks=chunks,
            ))
        return tuple(output)
