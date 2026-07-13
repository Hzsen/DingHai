from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class DocumentChunk:
    document_id: str
    title: str
    source_path: str
    chunk_text: str
    section: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _title_from_text(path: Path, text: str) -> str:
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return path.stem.replace("_", " ").title()


def _chunks(words: list[str], target_size: int = 450, overlap: int = 60) -> list[str]:
    if len(words) <= target_size:
        return [" ".join(words)]
    output = []
    start = 0
    while start < len(words):
        end = min(start + target_size, len(words))
        output.append(" ".join(words[start:end]))
        if end == len(words):
            break
        start = max(end - overlap, start + 1)
    return output


def load_markdown_documents(docs_dir: Path) -> list[DocumentChunk]:
    if not docs_dir.exists():
        raise FileNotFoundError(f"Document directory not found: {docs_dir}")
    chunks: list[DocumentChunk] = []
    for path in sorted(docs_dir.rglob("*.md")):
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            continue
        title = _title_from_text(path, text)
        relative = path.relative_to(docs_dir).as_posix()
        words = re.findall(r"\S+", text)
        for index, chunk_text in enumerate(_chunks(words)):
            chunks.append(DocumentChunk(f"{relative}::{index}", title, relative, chunk_text, title))
    return chunks
