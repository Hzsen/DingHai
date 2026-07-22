from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from domain.knowledge import KnowledgeDocumentType, KnowledgeReliability
from domain.query import RAGQueryRequest
from quant_agent.knowledge.store import KnowledgeStore, StoredKnowledgeChunk
from quant_agent.retrieval.lexical import tokenize_lexical


VECTOR_INDEX_VERSION = "canonical-local-hash-vector-v1.0.0"
VECTOR_DIMENSION = 1_024
SEMANTIC_MIN_SCORE = 0.08
_CONCEPTS = {
    "main_uptrend": ("主升浪", "main uptrend", "leader trend"),
    "distribution_risk": ("派发风险", "出货风险", "distribution risk", "high volume stall"),
    "selloff_repair": ("急跌修复", "止跌修复", "selloff repair", "reversal repair"),
    "optical_module": ("光模块", "optical module", "cpo"),
    "relative_strength": ("相对强度", "relative strength", "relative momentum"),
    "breakout": ("价格突破", "放量突破", "price breakout", "new high"),
}


def _semantic_tokens(text: str) -> list[str]:
    normalized = text.casefold()
    tokens = tokenize_lexical(text)
    for concept, aliases in _CONCEPTS.items():
        if any(alias in normalized for alias in aliases):
            tokens.append(f"concept:{concept}")
    return tokens


def _hashed_vector(text: str) -> dict[int, float]:
    counts = Counter(_semantic_tokens(text))
    values: dict[int, float] = {}
    for token, count in counts.items():
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % VECTOR_DIMENSION
        values[index] = values.get(index, 0.0) + (1.0 + math.log(count))
    norm = math.sqrt(sum(value * value for value in values.values())) or 1.0
    return {index: round(value / norm, 12) for index, value in values.items() if value}


def _serialize_vector(vector: dict[int, float]) -> str:
    return json.dumps(
        {str(index): value for index, value in sorted(vector.items())},
        sort_keys=True,
        separators=(",", ":"),
    )


def _deserialize_vector(value: str) -> dict[int, float]:
    return {int(index): float(weight) for index, weight in json.loads(value).items()}


def _cosine(left: dict[int, float], right: dict[int, float]) -> float:
    if len(left) > len(right):
        left, right = right, left
    return max(0.0, sum(value * right.get(index, 0.0) for index, value in left.items()))


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class VectorHit:
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
    semantic_score: float


@dataclass(frozen=True, slots=True)
class VectorReconcileResult:
    canonical_chunks: int
    inserted_or_updated: int
    deleted_stale: int
    indexed_vectors: int


SCHEMA = (
    """CREATE TABLE IF NOT EXISTS knowledge_index_state (
        index_name TEXT PRIMARY KEY,index_version TEXT NOT NULL,indexed_chunks INTEGER NOT NULL,
        last_synced_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS knowledge_vector_index (
        chunk_id TEXT NOT NULL,document_id TEXT NOT NULL,document_version INTEGER NOT NULL,
        content_hash TEXT NOT NULL,embedding_version TEXT NOT NULL,dimension INTEGER NOT NULL,
        vector_json TEXT NOT NULL,updated_at TEXT NOT NULL,
        PRIMARY KEY(chunk_id,document_version)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_vector_document ON knowledge_vector_index(document_id,document_version)",
)


class CanonicalVectorIndex:
    """Offline deterministic vector baseline over canonical, point-in-time-filtered chunks."""

    def __init__(self, db_path: Path | str, *, initialize: bool = True) -> None:
        self.db_path = Path(db_path)
        if initialize:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as conn:
                for statement in SCHEMA:
                    conn.execute(statement)
                conn.execute(
                    """INSERT OR IGNORE INTO knowledge_index_state
                    (index_name,index_version,indexed_chunks,last_synced_at) VALUES (?,?,0,NULL)""",
                    ("vector", VECTOR_INDEX_VERSION),
                )
        elif not self.db_path.exists():
            raise FileNotFoundError(self.db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @staticmethod
    def _searchable_text(stored: StoredKnowledgeChunk) -> str:
        document, chunk = stored.document, stored.chunk
        return " ".join((
            document.title,
            chunk.section,
            chunk.text,
            " ".join(document.tickers),
            " ".join(document.themes),
            document.document_type.value,
        ))

    def upsert(self, stored: StoredKnowledgeChunk) -> None:
        vector = _serialize_vector(_hashed_vector(self._searchable_text(stored)))
        chunk = stored.chunk
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO knowledge_vector_index
                (chunk_id,document_id,document_version,content_hash,embedding_version,dimension,vector_json,updated_at)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(chunk_id,document_version) DO UPDATE SET
                document_id=excluded.document_id,content_hash=excluded.content_hash,
                embedding_version=excluded.embedding_version,dimension=excluded.dimension,
                vector_json=excluded.vector_json,updated_at=excluded.updated_at""",
                (
                    chunk.chunk_id,
                    chunk.document_id,
                    chunk.document_version,
                    chunk.content_hash,
                    VECTOR_INDEX_VERSION,
                    VECTOR_DIMENSION,
                    vector,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            self._update_state(conn)

    def delete(self, chunk_id: str, document_version: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM knowledge_vector_index WHERE chunk_id=? AND document_version=?",
                (chunk_id, document_version),
            )
            self._update_state(conn)

    def _update_state(self, conn: sqlite3.Connection) -> None:
        count = int(conn.execute("SELECT COUNT(*) FROM knowledge_vector_index").fetchone()[0])
        conn.execute(
            """UPDATE knowledge_index_state SET index_version=?,indexed_chunks=?,last_synced_at=?
            WHERE index_name='vector'""",
            (VECTOR_INDEX_VERSION, count, datetime.now(timezone.utc).isoformat()),
        )

    def count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM knowledge_vector_index").fetchone()[0])

    def manifest(self) -> set[tuple[str, int, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT chunk_id,document_version,content_hash FROM knowledge_vector_index"
            ).fetchall()
        return {
            (str(row["chunk_id"]), int(row["document_version"]), str(row["content_hash"]))
            for row in rows
        }

    def reconcile(self, store: KnowledgeStore) -> VectorReconcileResult:
        if store.db_path.resolve() != self.db_path.resolve():
            raise ValueError("KnowledgeStore and vector index must use the same SQLite database")
        canonical = store.list_current_indexable_chunks()
        expected = {(item.chunk.chunk_id, item.chunk.document_version): item for item in canonical}
        inserted_or_updated = 0
        deleted_stale = 0
        with self._connect() as conn:
            existing_rows = conn.execute("SELECT * FROM knowledge_vector_index").fetchall()
            existing = {
                (str(row["chunk_id"]), int(row["document_version"])): row
                for row in existing_rows
            }
            for identity in existing.keys() - expected.keys():
                conn.execute(
                    "DELETE FROM knowledge_vector_index WHERE chunk_id=? AND document_version=?",
                    identity,
                )
                deleted_stale += 1
            now = datetime.now(timezone.utc).isoformat()
            for identity, stored in expected.items():
                row = existing.get(identity)
                if (
                    row is not None
                    and row["content_hash"] == stored.chunk.content_hash
                    and row["embedding_version"] == VECTOR_INDEX_VERSION
                    and int(row["dimension"]) == VECTOR_DIMENSION
                ):
                    continue
                conn.execute(
                    """INSERT INTO knowledge_vector_index
                    (chunk_id,document_id,document_version,content_hash,embedding_version,dimension,vector_json,updated_at)
                    VALUES (?,?,?,?,?,?,?,?)
                    ON CONFLICT(chunk_id,document_version) DO UPDATE SET
                    document_id=excluded.document_id,content_hash=excluded.content_hash,
                    embedding_version=excluded.embedding_version,dimension=excluded.dimension,
                    vector_json=excluded.vector_json,updated_at=excluded.updated_at""",
                    (
                        stored.chunk.chunk_id,
                        stored.chunk.document_id,
                        stored.chunk.document_version,
                        stored.chunk.content_hash,
                        VECTOR_INDEX_VERSION,
                        VECTOR_DIMENSION,
                        _serialize_vector(_hashed_vector(self._searchable_text(stored))),
                        now,
                    ),
                )
                inserted_or_updated += 1
            self._update_state(conn)
        return VectorReconcileResult(
            canonical_chunks=len(canonical),
            inserted_or_updated=inserted_or_updated,
            deleted_stale=deleted_stale,
            indexed_vectors=self.count(),
        )

    def search(self, request: RAGQueryRequest, *, candidate_limit: int | None = None) -> list[VectorHit]:
        query_vector = _hashed_vector(request.query_text)
        if not query_vector:
            return []
        conditions = [
            "v.content_hash=c.content_hash",
            "v.embedding_version=?",
            "v.dimension=?",
            "d.is_latest=1",
            "c.indexable=1",
            "d.available_at<=?",
            "c.available_at<=?",
        ]
        as_of = _utc_iso(request.as_of)
        params: list[object] = [VECTOR_INDEX_VERSION, VECTOR_DIMENSION, as_of, as_of]
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
        sql = f"""SELECT
            d.document_id,d.version,d.document_type,d.title,d.source_uri,d.reliability,
            c.chunk_id,c.section,c.text,COALESCE(c.event_time,d.event_time) AS effective_event_time,
            c.available_at,v.vector_json
            FROM knowledge_vector_index v
            JOIN knowledge_documents d ON d.document_id=v.document_id
            AND d.version=v.document_version
            JOIN knowledge_chunks c ON c.chunk_id=v.chunk_id
            AND c.document_id=v.document_id
            AND c.document_version=v.document_version
            WHERE {' AND '.join(conditions)}"""
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        scored = [
            (row, _cosine(query_vector, _deserialize_vector(row["vector_json"])))
            for row in rows
        ]
        scored = [(row, score) for row, score in scored if score >= SEMANTIC_MIN_SCORE]
        scored.sort(key=lambda item: (item[1], item[0]["available_at"]), reverse=True)
        limit = candidate_limit or max(request.top_k * 5, 20)
        if not 1 <= limit <= 1_000:
            raise ValueError("candidate_limit must be between 1 and 1000")
        return [VectorHit(
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
            semantic_score=round(score, 8),
        ) for row, score in scored[:limit]]

    @staticmethod
    def _add_in_filter(
        conditions: list[str], params: list[object], column: str, values: tuple[str, ...]
    ) -> None:
        placeholders = ",".join("?" for _ in values)
        conditions.append(f"{column} IN ({placeholders})")
        params.extend(values)
