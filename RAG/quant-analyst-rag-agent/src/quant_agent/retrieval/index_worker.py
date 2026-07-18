from __future__ import annotations

import uuid
from dataclasses import dataclass

from quant_agent.knowledge.store import (
    IndexJobOperation,
    KnowledgeStore,
)
from quant_agent.retrieval.lexical import CanonicalLexicalIndex


@dataclass(frozen=True, slots=True)
class IndexSyncResult:
    claimed: int
    completed: int
    failed: int
    upserted: int
    deleted: int
    skipped_stale: int
    remaining_jobs: dict[str, int]
    indexed_chunks: int


class KnowledgeIndexWorker:
    """At-least-once, idempotent consumer for KnowledgeStore index jobs."""

    def __init__(self, store: KnowledgeStore, lexical_index: CanonicalLexicalIndex) -> None:
        if store.db_path.resolve() != lexical_index.db_path.resolve():
            raise ValueError("KnowledgeStore and lexical index must use the same SQLite database")
        self.store = store
        self.lexical_index = lexical_index

    def sync(self, *, max_jobs: int = 1_000, batch_size: int = 100) -> IndexSyncResult:
        if not 1 <= max_jobs <= 100_000:
            raise ValueError("max_jobs must be between 1 and 100000")
        if not 1 <= batch_size <= 1_000:
            raise ValueError("batch_size must be between 1 and 1000")
        worker_id = f"lexical-{uuid.uuid4()}"
        claimed = completed = failed = upserted = deleted = skipped_stale = 0
        while claimed < max_jobs:
            jobs = self.store.claim_index_jobs(worker_id, limit=min(batch_size, max_jobs - claimed))
            if not jobs:
                break
            claimed += len(jobs)
            for job in jobs:
                try:
                    if job.operation is IndexJobOperation.DELETE:
                        self.lexical_index.delete(job.chunk_id, job.document_version)
                        deleted += 1
                    else:
                        stored = self.store.get_stored_chunk(
                            job.chunk_id,
                            job.document_version,
                            require_current_indexable=True,
                            content_hash=job.content_hash,
                        )
                        if stored is None:
                            self.lexical_index.delete(job.chunk_id, job.document_version)
                            skipped_stale += 1
                        else:
                            self.lexical_index.upsert(stored)
                            upserted += 1
                    self.store.complete_index_job(job.job_id)
                    completed += 1
                except Exception as exc:
                    self.store.fail_index_job(job.job_id, type(exc).__name__)
                    failed += 1
        return IndexSyncResult(
            claimed=claimed,
            completed=completed,
            failed=failed,
            upserted=upserted,
            deleted=deleted,
            skipped_stale=skipped_stale,
            remaining_jobs=self.store.index_job_counts(),
            indexed_chunks=self.lexical_index.count(),
        )
