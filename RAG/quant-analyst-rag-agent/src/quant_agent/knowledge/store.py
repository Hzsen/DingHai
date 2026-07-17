from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from domain.knowledge import (
    JsonValue,
    KnowledgeChunk,
    KnowledgeChunkType,
    KnowledgeDocument,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeQuery,
    KnowledgeReliability,
    KnowledgeSourceType,
)


class VersionConflictError(ValueError):
    """The same immutable document identity/version was supplied with new data."""


class IndexJobOperation(str, Enum):
    UPSERT = "UPSERT"
    DELETE = "DELETE"


class IndexJobStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True, slots=True)
class KnowledgeBundle:
    document: KnowledgeDocument
    chunks: tuple[KnowledgeChunk, ...]


@dataclass(frozen=True, slots=True)
class IngestionResult:
    run_id: str
    status: str
    document_count: int
    chunk_count: int
    index_jobs_created: int


@dataclass(frozen=True, slots=True)
class IndexJob:
    job_sequence: int
    job_id: str
    operation: IndexJobOperation
    chunk_id: str
    document_id: str
    document_version: int
    content_hash: str
    state_hash: str
    status: IndexJobStatus
    attempt_count: int
    worker_id: str | None
    created_at: datetime
    claimed_at: datetime | None
    completed_at: datetime | None
    error_type: str | None


@dataclass(frozen=True, slots=True)
class StoredKnowledgeChunk:
    document: KnowledgeDocument
    chunk: KnowledgeChunk


SCHEMA = (
    """CREATE TABLE IF NOT EXISTS knowledge_ingestion_runs (
        run_id TEXT PRIMARY KEY,source_name TEXT NOT NULL,status TEXT NOT NULL,
        started_at TEXT NOT NULL,completed_at TEXT,document_count INTEGER NOT NULL,
        chunk_count INTEGER NOT NULL,index_jobs_created INTEGER NOT NULL,error_type TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS knowledge_documents (
        document_id TEXT NOT NULL,version INTEGER NOT NULL,document_type TEXT NOT NULL,
        title TEXT NOT NULL,content TEXT NOT NULL,tickers_json TEXT NOT NULL,themes_json TEXT NOT NULL,
        thesis_id TEXT,event_time TEXT,as_of TEXT NOT NULL,available_at TEXT NOT NULL,
        status TEXT NOT NULL,source_type TEXT NOT NULL,source_uri TEXT,source_hash TEXT NOT NULL,
        content_hash TEXT NOT NULL,reliability TEXT NOT NULL,language TEXT NOT NULL,
        created_at TEXT NOT NULL,updated_at TEXT NOT NULL,metadata_json TEXT NOT NULL,
        ingestion_run_id TEXT NOT NULL,is_latest INTEGER NOT NULL,
        PRIMARY KEY(document_id,version),FOREIGN KEY(ingestion_run_id) REFERENCES knowledge_ingestion_runs(run_id)
    )""",
    """CREATE TABLE IF NOT EXISTS knowledge_chunks (
        chunk_id TEXT NOT NULL,document_id TEXT NOT NULL,document_version INTEGER NOT NULL,
        chunk_type TEXT NOT NULL,section TEXT NOT NULL,text TEXT NOT NULL,ordinal INTEGER NOT NULL,
        event_time TEXT,available_at TEXT NOT NULL,content_hash TEXT NOT NULL,token_count INTEGER NOT NULL,
        indexable INTEGER NOT NULL,metadata_json TEXT NOT NULL,ingestion_run_id TEXT NOT NULL,
        PRIMARY KEY(chunk_id,document_version),
        FOREIGN KEY(document_id,document_version) REFERENCES knowledge_documents(document_id,version),
        FOREIGN KEY(ingestion_run_id) REFERENCES knowledge_ingestion_runs(run_id)
    )""",
    """CREATE TABLE IF NOT EXISTS knowledge_index_jobs (
        job_sequence INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL UNIQUE,
        operation TEXT NOT NULL,chunk_id TEXT NOT NULL,document_id TEXT NOT NULL,
        document_version INTEGER NOT NULL,content_hash TEXT NOT NULL,state_hash TEXT NOT NULL,
        status TEXT NOT NULL,attempt_count INTEGER NOT NULL,worker_id TEXT,
        created_at TEXT NOT NULL,claimed_at TEXT,completed_at TEXT,error_type TEXT
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_knowledge_latest ON knowledge_documents(document_id) WHERE is_latest=1",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_document_available ON knowledge_documents(is_latest,status,available_at)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_chunk_available ON knowledge_chunks(indexable,available_at)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_jobs_status ON knowledge_index_jobs(status,job_sequence)",
)


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _dt(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _document_values(document: KnowledgeDocument, run_id: str, is_latest: int) -> tuple[object, ...]:
    return (
        document.document_id, document.version, document.document_type.value, document.title, document.content,
        _json(list(document.tickers)), _json(list(document.themes)), document.thesis_id,
        _dt(document.event_time), _dt(document.as_of), _dt(document.available_at), document.status.value,
        document.source_type.value, document.source_uri, document.source_hash, document.content_hash,
        document.reliability.value, document.language, _dt(document.created_at), _dt(document.updated_at),
        _json(document.metadata), run_id, is_latest,
    )


def _chunk_values(chunk: KnowledgeChunk, run_id: str) -> tuple[object, ...]:
    return (
        chunk.chunk_id, chunk.document_id, chunk.document_version, chunk.chunk_type.value,
        chunk.section, chunk.text, chunk.ordinal, _dt(chunk.event_time), _dt(chunk.available_at),
        chunk.content_hash, chunk.token_count, int(chunk.indexable), _json(chunk.metadata), run_id,
    )


def _row_state_hash(row: sqlite3.Row | tuple[object, ...] | dict[str, object], fields: tuple[str, ...] | None = None) -> str:
    if isinstance(row, sqlite3.Row):
        payload = {key: row[key] for key in (fields or tuple(row.keys())) if key != "ingestion_run_id"}
    elif isinstance(row, dict):
        payload = {key: value for key, value in row.items() if key != "ingestion_run_id"}
    else:
        payload = list(row)
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _document_from_row(row: sqlite3.Row) -> KnowledgeDocument:
    return KnowledgeDocument(
        document_id=row["document_id"], document_type=KnowledgeDocumentType(row["document_type"]),
        title=row["title"], content=row["content"], tickers=tuple(json.loads(row["tickers_json"])),
        themes=tuple(json.loads(row["themes_json"])), thesis_id=row["thesis_id"],
        event_time=_parse_dt(row["event_time"]), as_of=_parse_dt(row["as_of"]),
        available_at=_parse_dt(row["available_at"]), status=KnowledgeDocumentStatus(row["status"]),
        version=int(row["version"]), source_type=KnowledgeSourceType(row["source_type"]),
        source_uri=row["source_uri"], source_hash=row["source_hash"], content_hash=row["content_hash"],
        reliability=KnowledgeReliability(row["reliability"]), language=row["language"],
        created_at=_parse_dt(row["created_at"]), updated_at=_parse_dt(row["updated_at"]),
        metadata=json.loads(row["metadata_json"]),
    )


def _chunk_from_row(row: sqlite3.Row) -> KnowledgeChunk:
    return KnowledgeChunk(
        chunk_id=row["chunk_id"], document_id=row["document_id"],
        document_version=int(row["document_version"]), chunk_type=KnowledgeChunkType(row["chunk_type"]),
        section=row["section"], text=row["text"], ordinal=int(row["ordinal"]),
        event_time=_parse_dt(row["event_time"]), available_at=_parse_dt(row["available_at"]),
        content_hash=row["content_hash"], token_count=int(row["token_count"]),
        indexable=bool(row["indexable"]), metadata=json.loads(row["metadata_json"]),
    )


def _job_from_row(row: sqlite3.Row) -> IndexJob:
    return IndexJob(
        job_sequence=int(row["job_sequence"]), job_id=row["job_id"],
        operation=IndexJobOperation(row["operation"]), chunk_id=row["chunk_id"],
        document_id=row["document_id"], document_version=int(row["document_version"]),
        content_hash=row["content_hash"], state_hash=row["state_hash"],
        status=IndexJobStatus(row["status"]), attempt_count=int(row["attempt_count"]),
        worker_id=row["worker_id"], created_at=_parse_dt(row["created_at"]),
        claimed_at=_parse_dt(row["claimed_at"]), completed_at=_parse_dt(row["completed_at"]),
        error_type=row["error_type"],
    )


class KnowledgeStore:
    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            for statement in SCHEMA:
                conn.execute(statement)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def ingest(self, document: KnowledgeDocument, chunks: tuple[KnowledgeChunk, ...], source_name: str) -> IngestionResult:
        return self.ingest_batch((KnowledgeBundle(document, chunks),), source_name)

    def ingest_batch(self, bundles: tuple[KnowledgeBundle, ...], source_name: str) -> IngestionResult:
        if not source_name.strip():
            raise ValueError("source_name must not be empty")
        if not bundles:
            raise ValueError("bundles must not be empty")
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc).isoformat()
        document_count = len(bundles)
        chunk_count = sum(len(bundle.chunks) for bundle in bundles)
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO knowledge_ingestion_runs VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, source_name, "STAGED", started_at, None, document_count, chunk_count, 0, None),
            )
        jobs_created = 0
        try:
            self._validate_bundles(bundles)
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                for bundle in bundles:
                    jobs_created += self._upsert_bundle(conn, bundle, run_id)
                completed_at = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    "UPDATE knowledge_ingestion_runs SET status='PUBLISHED',completed_at=?,index_jobs_created=? WHERE run_id=?",
                    (completed_at, jobs_created, run_id),
                )
        except Exception as exc:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE knowledge_ingestion_runs SET status='FAILED',completed_at=?,error_type=? WHERE run_id=?",
                    (datetime.now(timezone.utc).isoformat(), type(exc).__name__, run_id),
                )
            raise
        return IngestionResult(run_id, "PUBLISHED", document_count, chunk_count, jobs_created)

    @staticmethod
    def _validate_bundles(bundles: tuple[KnowledgeBundle, ...]) -> None:
        identities: set[tuple[str, int]] = set()
        for bundle in bundles:
            identity = (bundle.document.document_id, bundle.document.version)
            if identity in identities:
                raise ValueError(f"duplicate document identity in batch: {identity}")
            identities.add(identity)
            chunk_ids: set[str] = set()
            for chunk in bundle.chunks:
                if not bundle.document.accepts_chunk(chunk):
                    raise ValueError("chunk document identity/version does not match bundle document")
                if chunk.chunk_id in chunk_ids:
                    raise ValueError(f"duplicate chunk_id in bundle: {chunk.chunk_id}")
                chunk_ids.add(chunk.chunk_id)

    def _upsert_bundle(self, conn: sqlite3.Connection, bundle: KnowledgeBundle, run_id: str) -> int:
        document = bundle.document
        existing = conn.execute(
            "SELECT * FROM knowledge_documents WHERE document_id=? AND version=?",
            (document.document_id, document.version),
        ).fetchone()
        latest = conn.execute(
            "SELECT * FROM knowledge_documents WHERE document_id=? AND is_latest=1",
            (document.document_id,),
        ).fetchone()
        jobs_created = 0
        if existing is not None:
            incoming_values = _document_values(document, existing["ingestion_run_id"], int(existing["is_latest"]))
            stored_values = tuple(existing[key] for key in existing.keys())
            if incoming_values != stored_values:
                raise VersionConflictError(
                    f"document {document.document_id} version {document.version} is immutable; create a new version"
                )
        else:
            if latest is not None and document.version <= int(latest["version"]):
                raise VersionConflictError("new document version must be greater than the latest stored version")
            if latest is not None:
                old_chunks = conn.execute(
                    "SELECT * FROM knowledge_chunks WHERE document_id=? AND document_version=?",
                    (latest["document_id"], latest["version"]),
                ).fetchall()
                for old_chunk in old_chunks:
                    if bool(old_chunk["indexable"]):
                        jobs_created += self._retire_chunk(conn, old_chunk)
                conn.execute(
                    "UPDATE knowledge_documents SET is_latest=0,status=? WHERE document_id=? AND version=?",
                    (KnowledgeDocumentStatus.SUPERSEDED.value, latest["document_id"], latest["version"]),
                )
            conn.execute(
                "INSERT INTO knowledge_documents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                _document_values(document, run_id, 1),
            )
        jobs_created += self._replace_chunks(conn, bundle, run_id)
        return jobs_created

    def _replace_chunks(self, conn: sqlite3.Connection, bundle: KnowledgeBundle, run_id: str) -> int:
        document = bundle.document
        existing_rows = conn.execute(
            "SELECT * FROM knowledge_chunks WHERE document_id=? AND document_version=?",
            (document.document_id, document.version),
        ).fetchall()
        existing = {row["chunk_id"]: row for row in existing_rows}
        incoming = {chunk.chunk_id: chunk for chunk in bundle.chunks}
        jobs_created = 0
        for chunk_id, old in existing.items():
            if chunk_id not in incoming:
                if bool(old["indexable"]):
                    jobs_created += self._retire_chunk(conn, old)
                conn.execute(
                    "DELETE FROM knowledge_chunks WHERE chunk_id=? AND document_version=?",
                    (chunk_id, document.version),
                )
        for chunk in bundle.chunks:
            old = existing.get(chunk.chunk_id)
            new_values = _chunk_values(chunk, run_id)
            if old is None:
                conn.execute("INSERT INTO knowledge_chunks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", new_values)
                if chunk.indexable:
                    row = conn.execute(
                        "SELECT * FROM knowledge_chunks WHERE chunk_id=? AND document_version=?",
                        (chunk.chunk_id, chunk.document_version),
                    ).fetchone()
                    jobs_created += self._enqueue_job(conn, IndexJobOperation.UPSERT, row)
                continue
            comparable_new = _chunk_values(chunk, old["ingestion_run_id"])
            comparable_old = tuple(old[key] for key in old.keys())
            if comparable_new == comparable_old:
                continue
            was_indexable = bool(old["indexable"])
            conn.execute(
                """UPDATE knowledge_chunks SET chunk_type=?,section=?,text=?,ordinal=?,event_time=?,available_at=?,
                content_hash=?,token_count=?,indexable=?,metadata_json=?,ingestion_run_id=?
                WHERE chunk_id=? AND document_version=?""",
                (
                    chunk.chunk_type.value, chunk.section, chunk.text, chunk.ordinal, _dt(chunk.event_time),
                    _dt(chunk.available_at), chunk.content_hash, chunk.token_count, int(chunk.indexable),
                    _json(chunk.metadata), run_id, chunk.chunk_id, chunk.document_version,
                ),
            )
            if was_indexable and not chunk.indexable:
                jobs_created += self._retire_chunk(conn, old)
            elif chunk.indexable:
                row = conn.execute(
                    "SELECT * FROM knowledge_chunks WHERE chunk_id=? AND document_version=?",
                    (chunk.chunk_id, chunk.document_version),
                ).fetchone()
                jobs_created += self._enqueue_job(conn, IndexJobOperation.UPSERT, row)
        return jobs_created

    def _enqueue_job(self, conn: sqlite3.Connection, operation: IndexJobOperation, chunk_row: sqlite3.Row) -> int:
        state_hash = _row_state_hash(chunk_row)
        if operation == IndexJobOperation.UPSERT:
            conn.execute(
                """UPDATE knowledge_index_jobs SET status='CANCELLED',completed_at=?
                WHERE chunk_id=? AND document_version=? AND operation='UPSERT' AND status='PENDING'
                AND state_hash<>?""",
                (
                    datetime.now(timezone.utc).isoformat(), chunk_row["chunk_id"],
                    chunk_row["document_version"], state_hash,
                ),
            )
        job_payload = f"{operation.value}|{chunk_row['chunk_id']}|{chunk_row['document_version']}|{state_hash}"
        job_id = hashlib.sha256(job_payload.encode("utf-8")).hexdigest()
        cursor = conn.execute(
            """INSERT OR IGNORE INTO knowledge_index_jobs
            (job_id,operation,chunk_id,document_id,document_version,content_hash,state_hash,status,
            attempt_count,worker_id,created_at,claimed_at,completed_at,error_type)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                job_id, operation.value, chunk_row["chunk_id"], chunk_row["document_id"],
                chunk_row["document_version"], chunk_row["content_hash"], state_hash,
                IndexJobStatus.PENDING.value, 0, None, datetime.now(timezone.utc).isoformat(), None, None, None,
            ),
        )
        return max(cursor.rowcount, 0)

    def _retire_chunk(self, conn: sqlite3.Connection, chunk_row: sqlite3.Row) -> int:
        """Cancel never-run UPSERTs; DELETE only when an index write may have happened."""
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """UPDATE knowledge_index_jobs SET status='CANCELLED',completed_at=?
            WHERE chunk_id=? AND document_version=? AND operation='UPSERT' AND status='PENDING'""",
            (now, chunk_row["chunk_id"], chunk_row["document_version"]),
        )
        may_be_indexed = conn.execute(
            """SELECT 1 FROM knowledge_index_jobs WHERE chunk_id=? AND document_version=?
            AND operation='UPSERT' AND status IN ('PROCESSING','COMPLETED','FAILED') LIMIT 1""",
            (chunk_row["chunk_id"], chunk_row["document_version"]),
        ).fetchone()
        return self._enqueue_job(conn, IndexJobOperation.DELETE, chunk_row) if may_be_indexed else 0

    def get_document(self, document_id: str, version: int | None = None) -> KnowledgeDocument | None:
        with self._connect() as conn:
            if version is None:
                row = conn.execute(
                    "SELECT * FROM knowledge_documents WHERE document_id=? AND is_latest=1", (document_id,)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM knowledge_documents WHERE document_id=? AND version=?", (document_id, version)
                ).fetchone()
        return _document_from_row(row) if row is not None else None

    def get_chunks(self, document_id: str, version: int | None = None) -> list[KnowledgeChunk]:
        document = self.get_document(document_id, version)
        if document is None:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM knowledge_chunks WHERE document_id=? AND document_version=? ORDER BY ordinal,chunk_id",
                (document.document_id, document.version),
            ).fetchall()
        return [_chunk_from_row(row) for row in rows]

    def query_chunks(self, query: KnowledgeQuery, limit: int | None = None) -> list[StoredKnowledgeChunk]:
        conditions = [
            "d.is_latest=1", "c.indexable=1", "d.available_at<=?", "c.available_at<=?",
        ]
        params: list[object] = [_dt(query.as_of), _dt(query.as_of)]
        self._add_in_filter(conditions, params, "d.status", tuple(status.value for status in query.statuses))
        if query.document_types:
            self._add_in_filter(conditions, params, "d.document_type", tuple(value.value for value in query.document_types))
        if query.reliability:
            self._add_in_filter(conditions, params, "d.reliability", tuple(value.value for value in query.reliability))
        if query.tickers:
            placeholders = ",".join("?" for _ in query.tickers)
            conditions.append(f"EXISTS (SELECT 1 FROM json_each(d.tickers_json) WHERE value IN ({placeholders}))")
            params.extend(query.tickers)
        if query.themes:
            placeholders = ",".join("?" for _ in query.themes)
            conditions.append(f"EXISTS (SELECT 1 FROM json_each(d.themes_json) WHERE value IN ({placeholders}))")
            params.extend(query.themes)
        event_expression = "COALESCE(c.event_time,d.event_time)"
        if query.event_time_from:
            conditions.append(f"{event_expression}>=?")
            params.append(_dt(query.event_time_from))
        if query.event_time_to:
            conditions.append(f"{event_expression}<=?")
            params.append(_dt(query.event_time_to))
        row_limit = query.top_k if limit is None else limit
        if not 1 <= row_limit <= 10_000:
            raise ValueError("limit must be between 1 and 10000")
        sql = f"""SELECT
            d.document_id AS d_document_id,d.version AS d_version,d.document_type AS d_document_type,
            d.title AS d_title,d.content AS d_content,d.tickers_json AS d_tickers_json,
            d.themes_json AS d_themes_json,d.thesis_id AS d_thesis_id,d.event_time AS d_event_time,
            d.as_of AS d_as_of,d.available_at AS d_available_at,d.status AS d_status,
            d.source_type AS d_source_type,d.source_uri AS d_source_uri,d.source_hash AS d_source_hash,
            d.content_hash AS d_content_hash,d.reliability AS d_reliability,d.language AS d_language,
            d.created_at AS d_created_at,d.updated_at AS d_updated_at,d.metadata_json AS d_metadata_json,
            c.* FROM knowledge_documents d JOIN knowledge_chunks c
            ON c.document_id=d.document_id AND c.document_version=d.version
            WHERE {' AND '.join(conditions)}
            ORDER BY c.available_at DESC,c.ordinal ASC,c.chunk_id ASC LIMIT ?"""
        params.append(row_limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        output: list[StoredKnowledgeChunk] = []
        for row in rows:
            document_row = {
                "document_id": row["d_document_id"], "version": row["d_version"],
                "document_type": row["d_document_type"], "title": row["d_title"], "content": row["d_content"],
                "tickers_json": row["d_tickers_json"], "themes_json": row["d_themes_json"],
                "thesis_id": row["d_thesis_id"], "event_time": row["d_event_time"], "as_of": row["d_as_of"],
                "available_at": row["d_available_at"], "status": row["d_status"],
                "source_type": row["d_source_type"], "source_uri": row["d_source_uri"],
                "source_hash": row["d_source_hash"], "content_hash": row["d_content_hash"],
                "reliability": row["d_reliability"], "language": row["d_language"],
                "created_at": row["d_created_at"], "updated_at": row["d_updated_at"],
                "metadata_json": row["d_metadata_json"],
            }
            # sqlite.Row cannot be constructed directly; a temporary in-memory row is unnecessary.
            document = KnowledgeDocument(
                document_id=document_row["document_id"], document_type=KnowledgeDocumentType(document_row["document_type"]),
                title=document_row["title"], content=document_row["content"],
                tickers=tuple(json.loads(document_row["tickers_json"])), themes=tuple(json.loads(document_row["themes_json"])),
                thesis_id=document_row["thesis_id"], event_time=_parse_dt(document_row["event_time"]),
                as_of=_parse_dt(document_row["as_of"]), available_at=_parse_dt(document_row["available_at"]),
                status=KnowledgeDocumentStatus(document_row["status"]), version=int(document_row["version"]),
                source_type=KnowledgeSourceType(document_row["source_type"]), source_uri=document_row["source_uri"],
                source_hash=document_row["source_hash"], content_hash=document_row["content_hash"],
                reliability=KnowledgeReliability(document_row["reliability"]), language=document_row["language"],
                created_at=_parse_dt(document_row["created_at"]), updated_at=_parse_dt(document_row["updated_at"]),
                metadata=json.loads(document_row["metadata_json"]),
            )
            output.append(StoredKnowledgeChunk(document, _chunk_from_row(row)))
        return output

    @staticmethod
    def _add_in_filter(conditions: list[str], params: list[object], column: str, values: tuple[str, ...]) -> None:
        if not values:
            conditions.append("0")
            return
        placeholders = ",".join("?" for _ in values)
        conditions.append(f"{column} IN ({placeholders})")
        params.extend(values)

    def claim_index_jobs(self, worker_id: str, limit: int = 100) -> list[IndexJob]:
        if not worker_id.strip():
            raise ValueError("worker_id must not be empty")
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        claimed_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT job_id FROM knowledge_index_jobs WHERE status='PENDING' ORDER BY job_sequence LIMIT ?", (limit,)
            ).fetchall()
            job_ids = [row["job_id"] for row in rows]
            for job_id in job_ids:
                conn.execute(
                    """UPDATE knowledge_index_jobs SET status='PROCESSING',worker_id=?,claimed_at=?,
                    attempt_count=attempt_count+1 WHERE job_id=? AND status='PENDING'""",
                    (worker_id, claimed_at, job_id),
                )
            if not job_ids:
                return []
            placeholders = ",".join("?" for _ in job_ids)
            claimed = conn.execute(
                f"SELECT * FROM knowledge_index_jobs WHERE job_id IN ({placeholders}) ORDER BY job_sequence", job_ids
            ).fetchall()
        return [_job_from_row(row) for row in claimed]

    def complete_index_job(self, job_id: str) -> None:
        self._finish_job(job_id, IndexJobStatus.COMPLETED, None)

    def fail_index_job(self, job_id: str, error_type: str) -> None:
        if not error_type.strip():
            raise ValueError("error_type must not be empty")
        self._finish_job(job_id, IndexJobStatus.FAILED, error_type)

    def retry_failed_index_job(self, job_id: str) -> None:
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE knowledge_index_jobs SET status='PENDING',worker_id=NULL,claimed_at=NULL,
                completed_at=NULL,error_type=NULL WHERE job_id=? AND status='FAILED'""",
                (job_id,),
            )
            if cursor.rowcount != 1:
                raise ValueError("index job must be in FAILED state")

    def requeue_stale_index_jobs(self, stale_before: datetime) -> int:
        if stale_before.tzinfo is None or stale_before.utcoffset() is None:
            raise ValueError("stale_before must be timezone-aware")
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE knowledge_index_jobs SET status='PENDING',worker_id=NULL,claimed_at=NULL,
                error_type='STALE_PROCESSING_RECOVERED' WHERE status='PROCESSING' AND claimed_at<?""",
                (stale_before.isoformat(),),
            )
        return max(cursor.rowcount, 0)

    def _finish_job(self, job_id: str, status: IndexJobStatus, error_type: str | None) -> None:
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE knowledge_index_jobs SET status=?,completed_at=?,error_type=?
                WHERE job_id=? AND status='PROCESSING'""",
                (status.value, datetime.now(timezone.utc).isoformat(), error_type, job_id),
            )
            if cursor.rowcount != 1:
                raise ValueError("index job must be in PROCESSING state")

    def table_count(self, table: str) -> int:
        allowed = {"knowledge_ingestion_runs", "knowledge_documents", "knowledge_chunks", "knowledge_index_jobs"}
        if table not in allowed:
            raise ValueError("unsupported table")
        with self._connect() as conn:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
