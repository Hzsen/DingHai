from __future__ import annotations

import sqlite3
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from domain.knowledge import (
    KnowledgeChunkType,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeReliability,
    KnowledgeSourceType,
    canonical_json_sha256,
)
from quant_agent.knowledge.adapters.base import KnowledgeChunkDraft, KnowledgeDocumentDraft


MARKET_TZ = ZoneInfo("Asia/Shanghai")


class ScreeningReportAdapter:
    """Index a compact discovery pointer; Gold remains the numeric source of truth."""

    source_name = "gold-screening-report-adapter-v1"

    def __init__(
        self,
        db_path: Path | str,
        report_path: Path | str | None = None,
        *,
        as_of: str | date | None = None,
        project_root: Path | str | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.report_path = Path(report_path) if report_path else None
        self.as_of = date.fromisoformat(as_of) if isinstance(as_of, str) else as_of
        self.project_root = Path(project_root).resolve() if project_root else self.db_path.parent.parent.parent

    def load(self) -> tuple[KnowledgeDocumentDraft, ...]:
        if not self.db_path.exists():
            return ()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='gold_cn_reversal_screen_results'"
            ).fetchone()
            if exists is None:
                return ()
            as_of_value = self.as_of.isoformat() if self.as_of else conn.execute(
                "SELECT MAX(as_of) FROM gold_cn_reversal_screen_results"
            ).fetchone()[0]
            if as_of_value is None:
                return ()
            rows = conn.execute(
                "SELECT * FROM gold_cn_reversal_screen_results WHERE as_of=? ORDER BY score_version,ticker",
                (as_of_value,),
            ).fetchall()
        if not rows:
            return ()

        serialized_rows = [{key: row[key] for key in row.keys()} for row in rows]
        score_versions = sorted({str(row["score_version"]) for row in rows})
        focus_tickers = tuple(str(row["ticker"]) for row in rows if bool(row["focus_selected"]))
        regimes = sorted({str(row["market_regime"]) for row in rows})
        dataset_hash = canonical_json_sha256(serialized_rows)
        day = date.fromisoformat(str(as_of_value))
        event_time = datetime.combine(day, time(15, 0), tzinfo=MARKET_TZ)
        if self.report_path is not None and self.report_path.exists():
            available_at = datetime.fromtimestamp(self.report_path.stat().st_mtime, tz=timezone.utc)
            try:
                report_uri = self.report_path.resolve().relative_to(self.project_root).as_posix()
            except ValueError:
                report_uri = self.report_path.resolve().as_posix()
        else:
            available_at = datetime.now(timezone.utc)
            report_uri = None
        version_label = ", ".join(score_versions)
        regime_label = ", ".join(regimes)
        ticker_label = ", ".join(focus_tickers) or "none"
        content = f"""# A-share Screening Dataset Reference — {as_of_value}

This is a retrieval pointer, not a copy of the numeric screen.

- Canonical Gold table: `gold_cn_reversal_screen_results`
- Data as of: `{as_of_value}`
- Score version: `{version_label}`
- Market regime label: `{regime_label}`
- Focus tickers: {ticker_label}

For scores, ranks, feature values, reasons, risks, and exclusions, query the Gold table by
`as_of + ticker + score_version`. The Markdown report is presentation-only and is not the
numeric source of truth.
""".strip()
        document_id = f"screening/cn-reversal/{as_of_value}"
        metadata = {
            "adapter": self.source_name,
            "gold_table": "gold_cn_reversal_screen_results",
            "gold_primary_key": ["as_of", "ticker", "score_version"],
            "dataset_hash": dataset_hash,
            "row_count": len(rows),
            "score_versions": score_versions,
            "report_path": report_uri,
            "numeric_truth_embedded": False,
            "availability_semantics": "report_filesystem_mtime" if report_uri else "migration_time",
        }
        return (KnowledgeDocumentDraft(
            document_id=document_id,
            document_type=KnowledgeDocumentType.SCREENING_REPORT,
            title=f"A-share Screening Dataset Reference — {as_of_value}",
            content=content,
            tickers=focus_tickers,
            themes=(regime_label,) if regime_label else (),
            thesis_id=None,
            event_time=event_time,
            as_of=event_time,
            available_at=available_at,
            status=KnowledgeDocumentStatus.FINALIZED,
            source_type=KnowledgeSourceType.SYSTEM_DERIVED,
            source_uri=f"sqlite://gold_cn_reversal_screen_results?as_of={as_of_value}",
            source_hash=dataset_hash,
            reliability=KnowledgeReliability.DERIVED,
            language="en",
            created_at=available_at,
            updated_at=available_at,
            metadata=metadata,
            chunks=(KnowledgeChunkDraft(
                chunk_id=f"{document_id}::reference",
                chunk_type=KnowledgeChunkType.SUMMARY,
                section="Gold Dataset Reference",
                text=content,
                ordinal=0,
                event_time=event_time,
                available_at=available_at,
                indexable=True,
                metadata={
                    "gold_table": "gold_cn_reversal_screen_results",
                    "numeric_truth_embedded": False,
                },
            ),),
        ),)
