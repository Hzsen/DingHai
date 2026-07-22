from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime
from enum import Enum
from pathlib import Path

from domain.macro import MacroDocumentStatus, MacroRiskDocument, MacroSnapshot, SeriesFeature
from domain.market_theme import MarketThemeState, ThemeHorizon


def build_macro_document(
    snapshot: MacroSnapshot,
    features: dict[str, SeriesFeature],
    status: MacroDocumentStatus = MacroDocumentStatus.FINALIZED_DAILY,
    market_theme_states: tuple[MarketThemeState, ...] = (),
) -> MacroRiskDocument:
    source_ids = tuple(sorted(f"{key}/{feature.observation_date}" for key, feature in features.items()))
    now = snapshot.as_of
    return MacroRiskDocument(
        document_id=f"macro-risk/{snapshot.as_of.date().isoformat()}", as_of=snapshot.as_of,
        valid_from=snapshot.as_of, valid_until=snapshot.valid_until, status=status,
        primary_regime=snapshot.primary_regime, risk_state=snapshot.risk_state,
        liquidity_state=snapshot.liquidity_state, inflation_quadrant=snapshot.inflation_quadrant,
        rate_pressure_state=snapshot.rate_pressure_state,
        liquidity_source_flows=snapshot.liquidity_source_flows,
        liquidity_target_flows=snapshot.liquidity_target_flows,
        asset_stances=snapshot.asset_stances,
        main_drivers=snapshot.main_drivers, confirming_signals=snapshot.confirming_signals,
        conflicting_signals=snapshot.conflicting_signals,
        risk_triggers=("CREDIT_SPREAD_WIDENING", "VIX_BACKWARDATION", "MOVE_SPIKE"),
        invalidation_conditions=("REGIME_STATE_CHANGES", "DOCUMENT_EXPIRES", "DATA_QUALITY_DEGRADES"),
        data_coverage=snapshot.data_coverage, confidence=snapshot.confidence,
        stale_series=snapshot.stale_series, source_observation_ids=source_ids,
        created_at=now, updated_at=now,
        market_theme_states=market_theme_states,
        metadata={"model_version": snapshot.model_version, "snapshot_id": snapshot.snapshot_id},
    )


def _jsonable(value):
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


def document_to_dict(document: MacroRiskDocument) -> dict[str, object]:
    return _jsonable(asdict(document))


def publish_macro_document(db_path: Path | str, document: MacroRiskDocument) -> None:
    """Publish active macro document and compact chunks with supersede semantics."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    payload = document_to_dict(document)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS macro_risk_documents (
            document_id TEXT PRIMARY KEY,as_of TEXT NOT NULL,valid_from TEXT NOT NULL,valid_until TEXT NOT NULL,
            status TEXT NOT NULL,primary_regime TEXT NOT NULL,risk_state TEXT NOT NULL,
            liquidity_state TEXT NOT NULL,confidence REAL NOT NULL,data_coverage REAL NOT NULL,
            payload_json TEXT NOT NULL,updated_at TEXT NOT NULL)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS macro_document_chunks (
            chunk_id TEXT PRIMARY KEY,document_id TEXT NOT NULL,chunk_type TEXT NOT NULL,
            valid_until TEXT NOT NULL,indexable INTEGER NOT NULL,content TEXT NOT NULL)"""
        )
        conn.execute("BEGIN IMMEDIATE")
        effective_status = document.status
        if document.status is MacroDocumentStatus.FINALIZED_DAILY:
            active = conn.execute(
                "SELECT document_id,as_of FROM macro_risk_documents WHERE status=? AND document_id<>?",
                (MacroDocumentStatus.FINALIZED_DAILY.value, document.document_id),
            ).fetchall()
            newer_exists = any(datetime.fromisoformat(row[1]) > document.as_of for row in active)
            if newer_exists:
                effective_status = MacroDocumentStatus.SUPERSEDED
            previous = [row for row in active if datetime.fromisoformat(row[1]) <= document.as_of]
            for row in previous:
                conn.execute(
                    "UPDATE macro_risk_documents SET status=? WHERE document_id=?",
                    (MacroDocumentStatus.SUPERSEDED.value, row[0]),
                )
                conn.execute("UPDATE macro_document_chunks SET indexable=0 WHERE document_id=?", (row[0],))
        payload["status"] = effective_status.value
        conn.execute(
            """INSERT INTO macro_risk_documents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(document_id) DO UPDATE SET as_of=excluded.as_of,valid_from=excluded.valid_from,
            valid_until=excluded.valid_until,status=excluded.status,primary_regime=excluded.primary_regime,
            risk_state=excluded.risk_state,liquidity_state=excluded.liquidity_state,
            confidence=excluded.confidence,data_coverage=excluded.data_coverage,
            payload_json=excluded.payload_json,updated_at=excluded.updated_at""",
            (
                document.document_id, document.as_of.isoformat(), document.valid_from.isoformat(),
                document.valid_until.isoformat(), effective_status.value, document.primary_regime.value,
                document.risk_state.value, document.liquidity_state.value, document.confidence,
                document.data_coverage, json.dumps(payload, ensure_ascii=False, sort_keys=True),
                document.updated_at.isoformat(),
            ),
        )
        conn.execute("DELETE FROM macro_document_chunks WHERE document_id=?", (document.document_id,))
        source_flow_text = "; ".join(
            f"{flow.source_id}: {flow.flow_billions_usd_20d:+.2f}bn ({flow.direction})"
            for flow in document.liquidity_source_flows
        )
        target_flow_text = "; ".join(
            f"{flow.target_id}: {flow.state.value} ({flow.absorption_score:+.1f}, confidence {flow.confidence:.0%})"
            for flow in document.liquidity_target_flows
        )
        equity_flow_text = "; ".join(
            f"{flow.target_id}: {flow.state.value} ({flow.absorption_score:+.1f})"
            for flow in document.liquidity_target_flows
            if flow.target_id in {"US_LARGE_CAP", "AI_SEMICONDUCTOR", "US_SMALL_CAP", "US_BANKS_CREDIT"}
        )
        defensive_flow_text = "; ".join(
            f"{flow.target_id}: {flow.state.value} ({flow.absorption_score:+.1f})"
            for flow in document.liquidity_target_flows
            if flow.target_id in {"TREASURY_7_10Y", "TREASURY_20Y_PLUS", "GOLD", "DOLLAR_CASH"}
        )
        chunks = {
            "RISK_SUMMARY": f"Risk {document.risk_state.value}; regime {document.primary_regime.value}; drivers: {', '.join(document.main_drivers)}",
            "LIQUIDITY_SOURCES": f"Liquidity {document.liquidity_state.value}; sources: {source_flow_text}",
            "LIQUIDITY_TARGETS": target_flow_text,
            "EQUITY_TRANSMISSION": equity_flow_text,
            "DEFENSIVE_TRANSMISSION": defensive_flow_text,
        }
        for state in document.market_theme_states:
            horizon_name = "FAST" if state.horizon is ThemeHorizon.FAST else "REPRICING"
            active = "; ".join(
                f"{item.theme_id} ({item.confidence:.0%}, confirmations {item.confirmation_count}/{item.confirmation_total}, persistence {item.persistence_periods})"
                for item in state.active_themes
            ) or "none"
            chunks[f"MARKET_THEME_{horizon_name}"] = (
                f"Dominant {state.dominant_theme_id or 'NONE'}: {state.summary} Active themes: {active}"
            )
            if state.active_themes:
                dominant = state.active_themes[0]
                chunks[f"MARKET_THEME_{horizon_name}_INVALIDATION"] = (
                    f"Theme {dominant.theme_id}; supporting evidence: {', '.join(dominant.supporting_evidence)}; "
                    f"conflicting evidence: {', '.join(dominant.conflicting_evidence) or 'none'}; "
                    f"invalidation: {', '.join(dominant.invalidation_conditions)}"
                )
        indexable = int(effective_status is MacroDocumentStatus.FINALIZED_DAILY)
        for chunk_type, content in chunks.items():
            conn.execute(
                "INSERT INTO macro_document_chunks VALUES (?,?,?,?,?,?)",
                (f"{document.document_id}/{chunk_type.lower()}", document.document_id, chunk_type,
                 document.valid_until.isoformat(), indexable, content),
            )
