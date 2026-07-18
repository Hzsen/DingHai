from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from domain.knowledge import KnowledgeDocumentType, KnowledgeReliability
from domain.query import RAGQueryRequest
from quant_agent.knowledge.store import StoredKnowledgeChunk


INDEX_VERSION = "canonical-lexical-v1.0.0"
_LATIN_TOKEN = re.compile(r"[a-z0-9]+(?:[._:/-][a-z0-9]+)*", re.IGNORECASE)
_CJK_RUN = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]+")
_ALIASES = {
    "bank_of_korea": ("韩国央行", "韩国银行", "bank of korea", "bok"),
    "semiconductor": ("半导体", "芯片", "semiconductor", "chip"),
    "funding_cost": ("资金价格", "资金成本", "funding cost"),
    "real_yield": ("实际利率", "real yield"),
    "liquidity": ("流动性", "liquidity"),
    "artificial_intelligence": ("人工智能", "ai"),
}


def tokenize_lexical(text: str) -> list[str]:
    """Language-aware deterministic tokens for English, tickers and CJK text."""
    normalized = text.lower()
    tokens: list[str] = []
    for token in _LATIN_TOKEN.findall(normalized):
        cleaned = token.replace("-", "_")
        tokens.append(cleaned)
        tokens.extend(part for part in re.split(r"[._:/]", cleaned) if part and part != cleaned)
    for run in _CJK_RUN.findall(normalized):
        chars = list(run)
        tokens.extend(chars)
        tokens.extend("".join(chars[index:index + 2]) for index in range(len(chars) - 1))
        tokens.extend("".join(chars[index:index + 3]) for index in range(len(chars) - 2))
        if len(chars) <= 8:
            tokens.append(run)
    for canonical, aliases in _ALIASES.items():
        if any(alias in normalized for alias in aliases):
            tokens.append(canonical)
    return [token for token in tokens if token.strip()]


def normalized_lexical_text(text: str) -> str:
    return " ".join(tokenize_lexical(text))


def build_match_expression(query_text: str) -> str:
    unique_tokens = list(dict.fromkeys(tokenize_lexical(query_text)))[:64]
    if not unique_tokens:
        return ""
    return " OR ".join(f'"{token}"' for token in unique_tokens)


@dataclass(frozen=True, slots=True)
class LexicalHit:
    document_id: str
    document_version: int
    chunk_id: str
    document_type: KnowledgeDocumentType
    title: str
    section: str
    text: str
    source_uri: str | None
    event_time: datetime | None
    available_at: datetime
    reliability: KnowledgeReliability
    lexical_score: float


SCHEMA = (
    """CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_lexical_index USING fts5(
        chunk_key UNINDEXED,chunk_id UNINDEXED,document_id UNINDEXED,
        document_version UNINDEXED,content_hash UNINDEXED,searchable_text,raw_text UNINDEXED,
        tokenize='unicode61 remove_diacritics 2'
    )""",
    """CREATE TABLE IF NOT EXISTS knowledge_index_state (
        index_name TEXT PRIMARY KEY,index_version TEXT NOT NULL,indexed_chunks INTEGER NOT NULL,
        last_synced_at TEXT
    )""",
)


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


class CanonicalLexicalIndex:
    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            for statement in SCHEMA:
                conn.execute(statement)
            conn.execute(
                """INSERT OR IGNORE INTO knowledge_index_state
                (index_name,index_version,indexed_chunks,last_synced_at) VALUES (?,?,0,NULL)""",
                ("lexical", INDEX_VERSION),
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @staticmethod
    def _chunk_key(chunk_id: str, document_version: int) -> str:
        return f"{chunk_id}@{document_version}"

    def upsert(self, stored: StoredKnowledgeChunk) -> None:
        document, chunk = stored.document, stored.chunk
        searchable = normalized_lexical_text(" ".join((
            document.title,
            chunk.section,
            chunk.text,
            " ".join(document.tickers),
            " ".join(document.themes),
            document.document_type.value,
        )))
        chunk_key = self._chunk_key(chunk.chunk_id, chunk.document_version)
        with self._connect() as conn:
            conn.execute("DELETE FROM knowledge_lexical_index WHERE chunk_key=?", (chunk_key,))
            conn.execute(
                "INSERT INTO knowledge_lexical_index VALUES (?,?,?,?,?,?,?)",
                (
                    chunk_key, chunk.chunk_id, chunk.document_id, str(chunk.document_version),
                    chunk.content_hash, searchable, chunk.text,
                ),
            )
            self._update_state(conn)

    def delete(self, chunk_id: str, document_version: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM knowledge_lexical_index WHERE chunk_key=?",
                (self._chunk_key(chunk_id, document_version),),
            )
            self._update_state(conn)

    def _update_state(self, conn: sqlite3.Connection) -> None:
        count = int(conn.execute("SELECT COUNT(*) FROM knowledge_lexical_index").fetchone()[0])
        conn.execute(
            """UPDATE knowledge_index_state SET index_version=?,indexed_chunks=?,last_synced_at=?
            WHERE index_name='lexical'""",
            (INDEX_VERSION, count, datetime.now(timezone.utc).isoformat()),
        )

    def count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM knowledge_lexical_index").fetchone()[0])

    def search(self, request: RAGQueryRequest, *, candidate_limit: int | None = None) -> list[LexicalHit]:
        match_expression = build_match_expression(request.query_text)
        if not match_expression:
            return []
        conditions = [
            "knowledge_lexical_index MATCH ?",
            "d.document_id=i.document_id",
            "d.version=CAST(i.document_version AS INTEGER)",
            "c.chunk_id=i.chunk_id",
            "c.document_id=i.document_id",
            "c.document_version=CAST(i.document_version AS INTEGER)",
            "d.is_latest=1",
            "c.indexable=1",
            "d.available_at<=?",
            "c.available_at<=?",
        ]
        as_of = _utc_iso(request.as_of)
        params: list[object] = [match_expression, as_of, as_of]
        self._add_in_filter(conditions, params, "d.status", tuple(item.value for item in request.statuses))
        if request.document_types:
            self._add_in_filter(
                conditions, params, "d.document_type", tuple(item.value for item in request.document_types)
            )
        if request.reliability:
            self._add_in_filter(
                conditions, params, "d.reliability", tuple(item.value for item in request.reliability)
            )
        if request.tickers:
            placeholders = ",".join("?" for _ in request.tickers)
            conditions.append(
                f"EXISTS (SELECT 1 FROM json_each(d.tickers_json) WHERE value IN ({placeholders}))"
            )
            params.extend(request.tickers)
        if request.themes:
            placeholders = ",".join("?" for _ in request.themes)
            conditions.append(
                f"EXISTS (SELECT 1 FROM json_each(d.themes_json) WHERE value IN ({placeholders}))"
            )
            params.extend(request.themes)
        event_expression = "COALESCE(c.event_time,d.event_time)"
        if request.event_time_from is not None:
            conditions.append(f"{event_expression}>=?")
            params.append(_utc_iso(request.event_time_from))
        if request.event_time_to is not None:
            conditions.append(f"{event_expression}<=?")
            params.append(_utc_iso(request.event_time_to))
        limit = candidate_limit or max(request.top_k * 5, 20)
        if not 1 <= limit <= 1_000:
            raise ValueError("candidate_limit must be between 1 and 1000")
        params.append(limit)
        sql = f"""SELECT
            d.document_id,d.version,d.document_type,d.title,d.source_uri,d.reliability,
            c.chunk_id,c.section,c.text,COALESCE(c.event_time,d.event_time) AS effective_event_time,
            c.available_at,bm25(knowledge_lexical_index,0,0,0,0,0,1.0,0.0) AS raw_rank
            FROM knowledge_lexical_index i,knowledge_documents d,knowledge_chunks c
            WHERE {' AND '.join(conditions)}
            ORDER BY raw_rank ASC,c.available_at DESC LIMIT ?"""
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        relevance = [max(0.0, -float(row["raw_rank"])) for row in rows]
        maximum = max(relevance, default=0.0) or 1.0
        return [LexicalHit(
            document_id=row["document_id"],
            document_version=int(row["version"]),
            chunk_id=row["chunk_id"],
            document_type=KnowledgeDocumentType(row["document_type"]),
            title=row["title"],
            section=row["section"],
            text=row["text"],
            source_uri=row["source_uri"],
            event_time=_parse_datetime(row["effective_event_time"]),
            available_at=_parse_datetime(row["available_at"]),
            reliability=KnowledgeReliability(row["reliability"]),
            lexical_score=round(score / maximum, 8),
        ) for row, score in zip(rows, relevance)]

    @staticmethod
    def _add_in_filter(
        conditions: list[str], params: list[object], column: str, values: tuple[str, ...]
    ) -> None:
        placeholders = ",".join("?" for _ in values)
        conditions.append(f"{column} IN ({placeholders})")
        params.extend(values)
