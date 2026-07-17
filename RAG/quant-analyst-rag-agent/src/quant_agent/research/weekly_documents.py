from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from domain.weekly_document import (
    WeeklyChunkType,
    WeeklyDocumentStatus,
    WeeklyKnowledgeChunk,
    WeeklyResearchDocument,
)


WEEKLY_DOCUMENT_VERSION = "weekly-research-document-v1.0.0"


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _wave_state(row: pd.Series) -> str:
    if bool(row.get("high_volume_stall_flag")) or float(row.get("risk_penalty", 0)) >= 10:
        return "DISTRIBUTION_RISK"
    score = float(row["wave_score"])
    if score >= 75:
        return "MAIN_UPTREND_CONFIRMED"
    if score >= 60:
        return "BREAKOUT_CANDIDATE"
    return "WATCHLIST"


def _week_bounds(day: date) -> tuple[date, date]:
    start = day - timedelta(days=day.weekday())
    return start, start + timedelta(days=4)


def _render_content(
    *,
    ticker: str,
    name: str,
    week_start: date,
    week_end: date,
    status: WeeklyDocumentStatus,
    opening_state: str,
    closing_state: str,
    metrics: dict[str, float | int | str | bool | None],
    observation_ids: tuple[str, ...],
    state_changes: list[tuple[date, str, str]],
) -> str:
    changes = "\n".join(f"- {day.isoformat()}: {old} -> {new}" for day, old, new in state_changes) or "- None"
    return f"""# Weekly Thesis: {ticker} {name} — {week_start.isoformat()}

## Document Status
{status.value}

## State
- Opening: {opening_state}
- Closing: {closing_state}

## Weekly Market Evidence
- Weekly return: {float(metrics['weekly_return']):.4f}
- CSI300-relative return: {float(metrics['relative_return']):.4f}
- Turnover sum: {float(metrics['turnover_sum']):.4f}
- Maximum drawdown: {float(metrics['max_drawdown']):.4f}
- Closing WaveScore: {float(metrics['closing_wave_score']):.1f}
- Average pilot amount rank: {metrics['average_amount_rank']}

## State Changes
{changes}

## Daily Observation References
{chr(10).join(f'- {item}' for item in observation_ids)}

## Time Boundary
- Week: {week_start.isoformat()} to {week_end.isoformat()}
- Data remains structured in SQLite; this document is a derived research summary.
"""


def build_weekly_documents(
    scored: pd.DataFrame,
    as_of: str | date | pd.Timestamp,
    *,
    start_week: date | None = None,
) -> tuple[list[WeeklyResearchDocument], list[WeeklyKnowledgeChunk]]:
    as_of_date = pd.Timestamp(as_of).date()
    scored = scored.copy()
    scored["date"] = pd.to_datetime(scored["date"])
    benchmark_close = scored.loc[scored["ticker"] == "000300.SH"].set_index("date")["close"]
    frame = scored.loc[(scored["ticker"] != "000300.SH") & (scored["date"].dt.date <= as_of_date)].copy()
    if start_week is not None:
        frame = frame.loc[frame["date"].dt.date >= start_week].copy()
    iso = frame["date"].dt.isocalendar()
    frame["iso_year"] = iso.year
    frame["iso_week"] = iso.week
    frame["wave_state"] = frame.apply(_wave_state, axis=1)
    documents: list[WeeklyResearchDocument] = []
    chunks: list[WeeklyKnowledgeChunk] = []
    as_of_dt = datetime.combine(as_of_date, datetime.max.time(), tzinfo=timezone.utc)
    for (ticker, year, week), group in frame.groupby(["ticker", "iso_year", "iso_week"], sort=True):
        group = group.sort_values("date")
        week_start, week_end = _week_bounds(group.iloc[0]["date"].date())
        status = WeeklyDocumentStatus.FINALIZED if as_of_date >= week_end else WeeklyDocumentStatus.DRAFT
        states = group[["date", "wave_state"]].reset_index(drop=True)
        state_changes: list[tuple[date, str, str]] = []
        for index in range(1, len(states)):
            old, new = states.iloc[index - 1]["wave_state"], states.iloc[index]["wave_state"]
            if old != new:
                state_changes.append((states.iloc[index]["date"].date(), old, new))
        observation_ids = tuple(f"{row.ticker}:{row.date.date().isoformat()}:{row.feature_version}" for row in group.itertuples(index=False))
        state_change_ids = tuple(f"{ticker}:{day.isoformat()}:{old}->{new}" for day, old, new in state_changes)
        first_close, last_close = float(group.iloc[0]["close"]), float(group.iloc[-1]["close"])
        benchmark_path = benchmark_close.reindex(group["date"]).dropna()
        benchmark_weekly_return = float(benchmark_path.iloc[-1] / benchmark_path.iloc[0] - 1) if len(benchmark_path) else 0.0
        path = group["close"] / first_close
        metrics: dict[str, float | int | str | bool | None] = {
            "weekly_return": last_close / first_close - 1,
            "relative_return": (last_close / first_close - 1) - benchmark_weekly_return,
            "turnover_sum": float(group["turnover_rate"].sum()),
            "max_drawdown": float((path / path.cummax() - 1).min()),
            "closing_wave_score": float(group.iloc[-1]["wave_score"]),
            "average_amount_rank": None if group["amount_rank_pilot"].isna().all() else round(float(group["amount_rank_pilot"].mean()), 2),
            "trading_days": len(group),
        }
        document_id = f"weekly/{ticker}/{int(year)}-W{int(week):02d}"
        content = _render_content(
            ticker=ticker,
            name=str(group.iloc[-1]["name"]),
            week_start=week_start,
            week_end=week_end,
            status=status,
            opening_state=str(group.iloc[0]["wave_state"]),
            closing_state=str(group.iloc[-1]["wave_state"]),
            metrics=metrics,
            observation_ids=observation_ids,
            state_changes=state_changes,
        )
        important_change = any(new in {"MAIN_UPTREND_CONFIRMED", "DISTRIBUTION_RISK"} for _, _, new in state_changes)
        document = WeeklyResearchDocument(
            document_id=document_id,
            ticker=ticker,
            name=str(group.iloc[-1]["name"]),
            week_start=week_start,
            week_end=week_end,
            as_of=as_of_dt,
            status=status,
            version=1,
            opening_state=str(group.iloc[0]["wave_state"]),
            closing_state=str(group.iloc[-1]["wave_state"]),
            daily_observation_ids=observation_ids,
            state_change_ids=state_change_ids,
            metrics=metrics,
            content=content,
            source_hash=_hash(content),
            source_run_id=str(group.iloc[-1]["source_run_id"]),
            llm_update_required=important_change,
        )
        documents.append(document)
        summary_type = WeeklyChunkType.WEEKLY_SUMMARY if status == WeeklyDocumentStatus.FINALIZED else WeeklyChunkType.DRAFT_SUMMARY
        chunks.append(
            WeeklyKnowledgeChunk(
                chunk_id=f"{document_id}/summary",
                document_id=document_id,
                chunk_type=summary_type,
                event_date=None,
                content=content,
                content_hash=_hash(content),
                indexable=status == WeeklyDocumentStatus.FINALIZED,
            )
        )
        for day, old, new in state_changes:
            event_content = f"{ticker} {day.isoformat()} state changed: {old} -> {new}."
            chunks.append(
                WeeklyKnowledgeChunk(
                    chunk_id=f"{document_id}/state/{day.isoformat()}/{old}-{new}",
                    document_id=document_id,
                    chunk_type=WeeklyChunkType.STATE_CHANGE,
                    event_date=day,
                    content=event_content,
                    content_hash=_hash(event_content),
                    indexable=new in {"MAIN_UPTREND_CONFIRMED", "DISTRIBUTION_RISK"},
                )
            )
    return documents, chunks


def publish_weekly_documents(
    db_path: Path | str,
    documents: list[WeeklyResearchDocument],
    chunks: list[WeeklyKnowledgeChunk],
    output_dir: Path | str,
) -> dict[str, float | int]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS weekly_documents (
            document_id TEXT PRIMARY KEY,ticker TEXT NOT NULL,name TEXT NOT NULL,week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,as_of TEXT NOT NULL,status TEXT NOT NULL,version INTEGER NOT NULL,
            opening_state TEXT NOT NULL,closing_state TEXT NOT NULL,daily_observation_ids_json TEXT NOT NULL,
            state_change_ids_json TEXT NOT NULL,metrics_json TEXT NOT NULL,content TEXT NOT NULL,
            source_hash TEXT NOT NULL,source_run_id TEXT NOT NULL,llm_update_required INTEGER NOT NULL,
            document_schema_version TEXT NOT NULL,updated_at TEXT NOT NULL)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS weekly_document_chunks (
            chunk_id TEXT PRIMARY KEY,document_id TEXT NOT NULL,chunk_type TEXT NOT NULL,event_date TEXT,
            content TEXT NOT NULL,content_hash TEXT NOT NULL,indexable INTEGER NOT NULL,
            embedding_status TEXT NOT NULL,updated_at TEXT NOT NULL)"""
        )
        now = datetime.now(timezone.utc).isoformat()
        for document in documents:
            existing = conn.execute("SELECT version,source_hash FROM weekly_documents WHERE document_id=?", (document.document_id,)).fetchone()
            version = existing[0] if existing and existing[1] == document.source_hash else (existing[0] + 1 if existing else 1)
            document = replace(document, version=version)
            conn.execute(
                """INSERT OR REPLACE INTO weekly_documents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (document.document_id,document.ticker,document.name,document.week_start.isoformat(),document.week_end.isoformat(),document.as_of.isoformat(),document.status.value,document.version,document.opening_state,document.closing_state,json.dumps(document.daily_observation_ids,ensure_ascii=False),json.dumps(document.state_change_ids,ensure_ascii=False),json.dumps(document.metrics,ensure_ascii=False,sort_keys=True),document.content,document.source_hash,document.source_run_id,int(document.llm_update_required),WEEKLY_DOCUMENT_VERSION,now),
            )
            conn.execute("DELETE FROM weekly_document_chunks WHERE document_id=?", (document.document_id,))
        for chunk in chunks:
            conn.execute(
                "INSERT OR REPLACE INTO weekly_document_chunks VALUES (?,?,?,?,?,?,?,?,?)",
                (chunk.chunk_id,chunk.document_id,chunk.chunk_type.value,chunk.event_date.isoformat() if chunk.event_date else None,chunk.content,chunk.content_hash,int(chunk.indexable),"pending" if chunk.indexable else "not_indexed",now),
            )
        latest_week = max((document.week_start for document in documents), default=None)
        if latest_week:
            latest_dir = output_dir / latest_week.isoformat()
            latest_dir.mkdir(parents=True, exist_ok=True)
            for document in documents:
                if document.week_start == latest_week:
                    (latest_dir / f"{document.ticker.replace('.', '_')}.md").write_text(document.content, encoding="utf-8")
        all_observation_json = conn.execute("SELECT daily_observation_ids_json FROM weekly_documents").fetchall()
        daily_baseline = sum(len(json.loads(row[0])) for row in all_observation_json)
        document_count = conn.execute("SELECT COUNT(*) FROM weekly_documents").fetchone()[0]
        chunk_count = conn.execute("SELECT COUNT(*) FROM weekly_document_chunks").fetchone()[0]
        indexable = conn.execute("SELECT COUNT(*) FROM weekly_document_chunks WHERE indexable=1").fetchone()[0]
        llm_events = conn.execute("SELECT COUNT(*) FROM weekly_documents WHERE llm_update_required=1").fetchone()[0]
        return {
            "weekly_document_count": document_count,
            "chunk_count": chunk_count,
            "indexable_chunk_count": indexable,
            "daily_document_baseline": daily_baseline,
            "embedding_reduction_ratio": 0.0 if daily_baseline == 0 else 1 - indexable / daily_baseline,
            "llm_update_event_count": llm_events,
            "documents_touched_this_run": len(documents),
        }


def weekly_incremental_start(db_path: Path | str, as_of: str | date | pd.Timestamp) -> date | None:
    path = Path(db_path)
    if not path.exists():
        return None
    with sqlite3.connect(path) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='weekly_documents'"
        ).fetchone()
        if not exists or conn.execute("SELECT COUNT(*) FROM weekly_documents").fetchone()[0] == 0:
            return None
    as_of_date = pd.Timestamp(as_of).date()
    current_monday = as_of_date - timedelta(days=as_of_date.weekday())
    return current_monday - timedelta(days=7)
