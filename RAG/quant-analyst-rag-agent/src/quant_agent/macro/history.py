from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd

from domain.macro import MacroSnapshot
from domain.macro_history import MacroChangeEvent, MacroHistoryPoint
from quant_agent.macro.data import build_live_macro_features
from quant_agent.macro.rules import evaluate_macro


DEFAULT_HISTORY_DAYS = 14
MINIMUM_HISTORY_COVERAGE = 0.5


def history_point_from_snapshot(snapshot: MacroSnapshot) -> MacroHistoryPoint:
    source_flows = {item.source_id: item.flow_billions_usd_20d for item in snapshot.liquidity_source_flows}
    targets = {item.target_id: item.absorption_score for item in snapshot.liquidity_target_flows}
    states = {item.target_id: item.state.value for item in snapshot.liquidity_target_flows}
    return MacroHistoryPoint(
        as_of=snapshot.as_of,
        snapshot_id=snapshot.snapshot_id,
        model_version=snapshot.model_version,
        net_liquidity_20d_bn=sum(source_flows.values()),
        liquidity_score=snapshot.liquidity_score,
        risk_score=snapshot.risk_score,
        rate_pressure_score=snapshot.rate_pressure_score,
        confidence=snapshot.confidence,
        source_flows_bn=source_flows,
        target_absorption=targets,
        target_states=states,
    )


def build_macro_history(
    observations: pd.DataFrame,
    end_as_of: datetime,
    window_days: int = DEFAULT_HISTORY_DAYS,
) -> list[MacroHistoryPoint]:
    """Reconstruct one point-in-time snapshot per weekday over a sensitive half-month window."""
    if window_days < 2:
        raise ValueError("window_days must be at least 2")
    end = pd.Timestamp(end_as_of)
    end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
    start_date = (end - pd.Timedelta(days=window_days)).date()
    dates = pd.date_range(start=start_date, end=end.date(), freq="B")
    points: list[MacroHistoryPoint] = []
    for day in dates:
        point_as_of = datetime.combine(day.date(), time(23, 59), tzinfo=timezone.utc)
        features = build_live_macro_features(observations, point_as_of)
        snapshot = evaluate_macro(features, point_as_of)
        if snapshot.data_coverage >= MINIMUM_HISTORY_COVERAGE:
            points.append(history_point_from_snapshot(snapshot))
    if len(points) < 2:
        raise RuntimeError("not enough point-in-time macro snapshots for change detection")
    return points


def _event(
    current: MacroHistoryPoint,
    previous: MacroHistoryPoint,
    *,
    event_type: str,
    entity_id: str,
    previous_value: float | str | bool | None,
    current_value: float | str | bool | None,
    magnitude: float | None,
    direction: str,
    reason_codes: tuple[str, ...],
    needs_kimi_analysis: bool = True,
) -> MacroChangeEvent:
    material = "|".join([
        current.as_of.isoformat(), previous.as_of.isoformat(), event_type, entity_id,
        str(previous_value), str(current_value), ",".join(reason_codes),
    ])
    event_id = "macro-change/" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    return MacroChangeEvent(
        event_id=event_id,
        as_of=current.as_of,
        window_start=previous.as_of,
        window_days=max(1, (current.as_of.date() - previous.as_of.date()).days),
        event_type=event_type,
        entity_id=entity_id,
        previous_value=previous_value,
        current_value=current_value,
        magnitude=magnitude,
        direction=direction,
        reason_codes=reason_codes,
        needs_kimi_analysis=needs_kimi_analysis,
    )


def detect_macro_changes(points: list[MacroHistoryPoint]) -> list[MacroChangeEvent]:
    if len(points) < 2:
        return []
    previous, current = points[0], points[-1]
    events: list[MacroChangeEvent] = []
    liquidity_delta = current.net_liquidity_20d_bn - previous.net_liquidity_20d_bn
    if abs(liquidity_delta) >= 50 or previous.net_liquidity_20d_bn * current.net_liquidity_20d_bn < 0:
        events.append(_event(
            current, previous, event_type="SYSTEM_LIQUIDITY_SHIFT", entity_id="NET_USD_LIQUIDITY",
            previous_value=round(previous.net_liquidity_20d_bn, 3),
            current_value=round(current.net_liquidity_20d_bn, 3), magnitude=abs(liquidity_delta),
            direction="EXPANDING" if liquidity_delta > 0 else "CONTRACTING",
            reason_codes=("NET_LIQUIDITY_CHANGE_14D", "TGA_RRP_WALCL_DECOMPOSITION"),
        ))
    for metric, previous_value, current_value in (
        ("RISK", previous.risk_score, current.risk_score),
        ("REAL_RATE_PRESSURE", previous.rate_pressure_score, current.rate_pressure_score),
    ):
        delta = current_value - previous_value
        if abs(delta) >= 15:
            events.append(_event(
                current, previous, event_type="MACRO_CONSTRAINT_SHIFT", entity_id=metric,
                previous_value=round(previous_value, 3), current_value=round(current_value, 3),
                magnitude=abs(delta), direction="RISING" if delta > 0 else "FALLING",
                reason_codes=(f"{metric}_CHANGE_14D",),
            ))
    target_ids = sorted(set(previous.target_absorption).intersection(current.target_absorption))
    for target_id in target_ids:
        before = previous.target_absorption[target_id]
        after = current.target_absorption[target_id]
        delta = after - before
        state_changed = previous.target_states.get(target_id) != current.target_states.get(target_id)
        if abs(delta) >= 15 or state_changed:
            reason_codes = ["TARGET_ABSORPTION_CHANGE_14D"]
            if state_changed:
                reason_codes.append("TARGET_STATE_CHANGED")
            events.append(_event(
                current, previous, event_type="TARGET_ROTATION", entity_id=target_id,
                previous_value=round(before, 3), current_value=round(after, 3), magnitude=abs(delta),
                direction="ABSORPTION_STRENGTHENING" if delta > 0 else "ABSORPTION_WEAKENING",
                reason_codes=tuple(reason_codes),
            ))
    risk_targets = ("US_LARGE_CAP", "AI_SEMICONDUCTOR", "US_SMALL_CAP", "US_BANKS_CREDIT")
    current_risk_scores = [current.target_absorption[target] for target in risk_targets if target in current.target_absorption]
    rejecting_count = sum(score < 0 for score in current_risk_scores)
    if current.liquidity_score >= 60 and current_risk_scores and rejecting_count >= len(current_risk_scores) / 2:
        events.append(_event(
            current, previous, event_type="CROSS_ASSET_DIVERGENCE", entity_id="RISK_ASSET_TRANSMISSION",
            previous_value=None, current_value=True, magnitude=None, direction="DIVERGING",
            reason_codes=("LIQUIDITY_EXPANDING", "RISK_ASSET_ABSORPTION_WEAK"),
        ))
    ai = current.target_absorption.get("AI_SEMICONDUCTOR")
    large = current.target_absorption.get("US_LARGE_CAP")
    if ai is not None and large is not None and large - ai >= 20:
        events.append(_event(
            current, previous, event_type="CROSS_ASSET_DIVERGENCE", entity_id="AI_VS_LARGE_CAP",
            previous_value=round(previous.target_absorption.get("AI_SEMICONDUCTOR", 0.0), 3),
            current_value=round(ai, 3), magnitude=large - ai, direction="AI_LAGGING",
            reason_codes=("LARGE_CAP_ABSORPTION_EXCEEDS_AI", "POSSIBLE_AI_CAPEX_OR_DURATION_CONSTRAINT"),
        ))
    return sorted(events, key=lambda item: (item.event_type, item.entity_id))


def publish_macro_history(
    db_path: Path | str,
    points: list[MacroHistoryPoint],
    events: list[MacroChangeEvent],
) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS macro_snapshots_history (
            as_of TEXT NOT NULL,model_version TEXT NOT NULL,snapshot_id TEXT NOT NULL,
            net_liquidity_20d_bn REAL NOT NULL,liquidity_score REAL NOT NULL,risk_score REAL NOT NULL,
            rate_pressure_score REAL NOT NULL,confidence REAL NOT NULL,payload_json TEXT NOT NULL,
            PRIMARY KEY(as_of,model_version)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS macro_target_history (
            as_of TEXT NOT NULL,model_version TEXT NOT NULL,target_id TEXT NOT NULL,
            absorption_score REAL NOT NULL,state TEXT NOT NULL,snapshot_id TEXT NOT NULL,
            PRIMARY KEY(as_of,model_version,target_id)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS macro_change_events (
            event_id TEXT PRIMARY KEY,as_of TEXT NOT NULL,window_start TEXT NOT NULL,
            window_days INTEGER NOT NULL,event_type TEXT NOT NULL,entity_id TEXT NOT NULL,
            needs_kimi_analysis INTEGER NOT NULL,payload_json TEXT NOT NULL
        )""")
        conn.execute("BEGIN IMMEDIATE")
        for point in points:
            payload = json.dumps(asdict(point), ensure_ascii=False, sort_keys=True, default=str)
            conn.execute("""INSERT INTO macro_snapshots_history VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(as_of,model_version) DO UPDATE SET snapshot_id=excluded.snapshot_id,
                net_liquidity_20d_bn=excluded.net_liquidity_20d_bn,liquidity_score=excluded.liquidity_score,
                risk_score=excluded.risk_score,rate_pressure_score=excluded.rate_pressure_score,
                confidence=excluded.confidence,payload_json=excluded.payload_json""",
                (point.as_of.isoformat(), point.model_version, point.snapshot_id, point.net_liquidity_20d_bn,
                 point.liquidity_score, point.risk_score, point.rate_pressure_score, point.confidence, payload),
            )
            for target_id, score in point.target_absorption.items():
                conn.execute("""INSERT INTO macro_target_history VALUES (?,?,?,?,?,?)
                    ON CONFLICT(as_of,model_version,target_id) DO UPDATE SET
                    absorption_score=excluded.absorption_score,state=excluded.state,snapshot_id=excluded.snapshot_id""",
                    (point.as_of.isoformat(), point.model_version, target_id, score,
                     point.target_states.get(target_id, "UNKNOWN"), point.snapshot_id),
                )
        for event in events:
            conn.execute(
                "INSERT OR REPLACE INTO macro_change_events VALUES (?,?,?,?,?,?,?,?)",
                (event.event_id, event.as_of.isoformat(), event.window_start.isoformat(), event.window_days,
                 event.event_type, event.entity_id, int(event.needs_kimi_analysis),
                 json.dumps(asdict(event), ensure_ascii=False, sort_keys=True, default=str)),
            )

