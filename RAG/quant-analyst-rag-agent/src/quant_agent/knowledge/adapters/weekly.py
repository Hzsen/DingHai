from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from domain.knowledge import (
    KnowledgeChunkType,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeReliability,
    KnowledgeSourceType,
)
from quant_agent.knowledge.adapters.base import KnowledgeChunkDraft, KnowledgeDocumentDraft


MARKET_TZ = ZoneInfo("Asia/Shanghai")


def _market_time(day: date, hour: int = 15, minute: int = 10) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=MARKET_TZ)


class WeeklyResearchAdapter:
    source_name = "weekly-research-sqlite-adapter-v1"

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)

    def load(self) -> tuple[KnowledgeDocumentDraft, ...]:
        if not self.db_path.exists():
            return ()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='weekly_documents'"
            ).fetchone()
            if exists is None:
                return ()
            documents = conn.execute("SELECT * FROM weekly_documents ORDER BY document_id").fetchall()
            chunks = conn.execute(
                "SELECT * FROM weekly_document_chunks ORDER BY document_id,chunk_id"
            ).fetchall()
        chunks_by_document: dict[str, list[sqlite3.Row]] = {}
        for chunk in chunks:
            chunks_by_document.setdefault(str(chunk["document_id"]), []).append(chunk)

        output: list[KnowledgeDocumentDraft] = []
        for row in documents:
            week_start = date.fromisoformat(row["week_start"])
            week_end = date.fromisoformat(row["week_end"])
            source_as_of_date = date.fromisoformat(str(row["as_of"])[:10])
            finalized = row["status"] == "FINALIZED"
            as_of = _market_time(week_end if finalized else source_as_of_date, 15, 0)
            available_at = _market_time(week_end if finalized else source_as_of_date)
            canonical_chunks: list[KnowledgeChunkDraft] = []
            for ordinal, chunk in enumerate(chunks_by_document.get(str(row["document_id"]), [])):
                event_date = date.fromisoformat(chunk["event_date"]) if chunk["event_date"] else None
                chunk_available = _market_time(event_date) if event_date else available_at
                canonical_chunks.append(KnowledgeChunkDraft(
                    chunk_id=str(chunk["chunk_id"]),
                    chunk_type=KnowledgeChunkType(str(chunk["chunk_type"])),
                    section=str(chunk["chunk_type"]).replace("_", " ").title(),
                    text=str(chunk["content"]),
                    ordinal=ordinal,
                    event_time=_market_time(event_date, 15, 0) if event_date else _market_time(week_end, 15, 0),
                    available_at=chunk_available,
                    indexable=bool(chunk["indexable"]),
                    metadata={
                        "source_embedding_status": str(chunk["embedding_status"]),
                        "source_content_hash": str(chunk["content_hash"]),
                    },
                ))
            status = KnowledgeDocumentStatus.FINALIZED if finalized else KnowledgeDocumentStatus.DRAFT
            output.append(KnowledgeDocumentDraft(
                document_id=str(row["document_id"]),
                document_type=KnowledgeDocumentType.WEEKLY_RESEARCH,
                title=f"Weekly Thesis: {row['ticker']} {row['name']} — {row['week_start']}",
                content=str(row["content"]),
                tickers=(str(row["ticker"]),),
                themes=(),
                thesis_id=f"thesis/{row['ticker']}",
                event_time=_market_time(week_end if finalized else source_as_of_date, 15, 0),
                as_of=as_of,
                available_at=available_at,
                status=status,
                source_type=KnowledgeSourceType.SYSTEM_DERIVED,
                source_uri=f"sqlite://weekly_documents/{row['document_id']}",
                source_hash=str(row["source_hash"]),
                reliability=KnowledgeReliability.DERIVED,
                language="zh-CN",
                created_at=available_at,
                updated_at=available_at,
                metadata={
                    "adapter": self.source_name,
                    "source_table": "weekly_documents",
                    "source_version": int(row["version"]),
                    "source_run_id": str(row["source_run_id"]),
                    "document_schema_version": str(row["document_schema_version"]),
                    "week_start": week_start.isoformat(),
                    "week_end": week_end.isoformat(),
                    "opening_state": str(row["opening_state"]),
                    "closing_state": str(row["closing_state"]),
                    "llm_update_required": bool(row["llm_update_required"]),
                    "daily_observation_ids": json.loads(row["daily_observation_ids_json"]),
                    "state_change_ids": json.loads(row["state_change_ids_json"]),
                    "availability_semantics": "market_close_plus_10m",
                },
                chunks=tuple(canonical_chunks),
            ))
        return tuple(output)
