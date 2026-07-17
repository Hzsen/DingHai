from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from domain.macro_history import MacroHistoryPoint
from quant_agent.macro.history import build_macro_history, detect_macro_changes, publish_macro_history
from quant_agent.macro.kimi_analysis import (
    MacroAnalysisCache,
    build_macro_analysis_cache_key,
    build_macro_analysis_packet,
    build_macro_pricing_prompt,
    publish_macro_pricing_inference,
    request_macro_pricing_analysis,
)


AS_OF = datetime(2026, 7, 15, 23, 59, tzinfo=timezone.utc)


def _observations() -> pd.DataFrame:
    rows = []
    start = datetime(2026, 5, 20, tzinfo=timezone.utc)
    specs = {
        "DFII10": (2.1, 0.002, "percent", False), "DGS10": (4.4, 0.002, "percent", False),
        "DGS30": (4.8, 0.002, "percent", False), "DGS2": (4.0, 0.001, "percent", False),
        "T10YIE": (2.25, 0.0, "percent", False), "DXY_PROXY": (121.0, -0.01, "index", False),
        "BAMLC0A0CM": (0.8, 0.0, "percent", False), "VIX": (18.0, -0.01, "index", True),
        "VIX3M": (20.0, -0.005, "index", True), "WALCL": (6_700_000, 0.0, "millions_usd", False),
        "WTREGEN": (900_000, -5_000.0, "millions_usd", False),
        "RRPONTSYD": (20.0, -0.1, "billions_usd", False),
        "SPY": (700.0, 0.5, "usd", True), "QQQ": (650.0, 0.2, "usd", True),
        "IWM": (280.0, -0.1, "usd", True), "KRE": (75.0, 0.0, "usd", True),
        "SOXX": (550.0, -0.2, "usd", True), "GLD": (360.0, 0.1, "usd", True),
        "IEF": (94.0, -0.02, "usd", True), "TLT": (86.0, -0.05, "usd", True),
    }
    for offset in range(58):
        observed = start + timedelta(days=offset)
        for series_id, (base, slope, unit, realtime) in specs.items():
            rows.append({
                "series_id": series_id, "observation_date": observed.isoformat(),
                "available_at": (observed + timedelta(hours=20)).isoformat(),
                "value": max(0.01, base + slope * offset), "unit": unit,
                "source": "offline-fixture", "is_realtime": realtime,
            })
    return pd.DataFrame(rows)


def test_half_month_history_is_point_in_time_and_has_about_ten_sessions() -> None:
    points = build_macro_history(_observations(), AS_OF, 14)
    assert 9 <= len(points) <= 11
    assert points[0].as_of.date().isoformat() == "2026-07-01"
    assert points[-1].as_of.date().isoformat() == "2026-07-15"
    assert all(point.snapshot_id for point in points)


def test_change_detection_finds_liquidity_shift_and_target_rotation() -> None:
    points = build_macro_history(_observations(), AS_OF, 14)
    first, last = points[0], points[-1]
    changed_last = MacroHistoryPoint(
        as_of=last.as_of, snapshot_id=last.snapshot_id, model_version=last.model_version,
        net_liquidity_20d_bn=first.net_liquidity_20d_bn + 80,
        liquidity_score=100, risk_score=last.risk_score, rate_pressure_score=last.rate_pressure_score,
        confidence=last.confidence, source_flows_bn=last.source_flows_bn,
        target_absorption={**last.target_absorption, "AI_SEMICONDUCTOR": first.target_absorption["AI_SEMICONDUCTOR"] - 25},
        target_states={**last.target_states, "AI_SEMICONDUCTOR": "REJECTING"},
    )
    events = detect_macro_changes([first, changed_last])
    assert any(event.event_type == "SYSTEM_LIQUIDITY_SHIFT" for event in events)
    assert any(event.event_type == "TARGET_ROTATION" and event.entity_id == "AI_SEMICONDUCTOR" for event in events)


def test_history_publish_is_idempotent(tmp_path) -> None:
    points = build_macro_history(_observations(), AS_OF, 14)
    events = detect_macro_changes(points)
    db = tmp_path / "history.db"
    publish_macro_history(db, points, events)
    publish_macro_history(db, points, events)
    import sqlite3
    with sqlite3.connect(db) as conn:
        snapshots = conn.execute("SELECT COUNT(*) FROM macro_snapshots_history").fetchone()[0]
        targets = conn.execute("SELECT COUNT(*) FROM macro_target_history").fetchone()[0]
    assert snapshots == len(points)
    assert targets == sum(len(point.target_absorption) for point in points)


def test_macro_prompt_is_compact_sanitized_and_cacheable(tmp_path) -> None:
    points = build_macro_history(_observations(), AS_OF, 14)
    events = detect_macro_changes(points)
    packet = build_macro_analysis_packet(points, events)
    secret = "sk-thismustnotappear123456"
    contexts = [f"MOONSHOT_API_KEY={secret}", "official context", "third", "ignored fourth"]
    messages = build_macro_pricing_prompt(packet, contexts)
    prompt = "\n".join(message["content"] for message in messages)
    assert secret not in prompt
    assert "ignored fourth" not in prompt
    key = build_macro_analysis_cache_key(packet, contexts, "mock-model")
    cache = MacroAnalysisCache(tmp_path / "cache")
    calls = 0
    def factory() -> dict:
        nonlocal calls
        calls += 1
        return {"ok": True}
    assert cache.get_or_compute(key, factory)[1] is False
    assert cache.get_or_compute(key, factory)[1] is True
    assert calls == 1


def test_kimi_macro_response_schema() -> None:
    points = build_macro_history(_observations(), AS_OF, 14)
    packet = build_macro_analysis_packet(points, detect_macro_changes(points))
    payload = {
        "packet_id": packet.packet_id, "analysis_window_days": packet.window_days,
        "dominant_pricing_hypothesis": {
            "risk_type": "LIQUIDITY_PLUMBING", "hypothesis": "TGA flow dominates the impulse.",
            "confidence": 0.6, "supporting_evidence": ["NET_LIQUIDITY_CHANGE_14D"],
            "contradicting_evidence": [], "invalidation_conditions": ["TGA contribution reverses"],
        },
        "alternative_hypotheses": [], "flow_interpretation": "Transmission remains selective.",
        "target_rotation": [], "unknowns": [], "research_note_needed": True,
    }
    class Client:
        def complete_json(self, *args, **kwargs):
            return payload
    result = request_macro_pricing_analysis(Client(), "mock", [], packet.packet_id)
    assert result["dominant_pricing_hypothesis"]["risk_type"] == "LIQUIDITY_PLUMBING"


def test_historical_kimi_inference_does_not_supersede_newer_packet(tmp_path) -> None:
    points = build_macro_history(_observations(), AS_OF, 14)
    newer = build_macro_analysis_packet(points, detect_macro_changes(points))
    older = build_macro_analysis_packet(points[:-1], detect_macro_changes(points[:-1]))
    db = tmp_path / "inference.db"
    publish_macro_pricing_inference(db, newer, {"packet_id": newer.packet_id}, model="mock", cache_key="new")
    publish_macro_pricing_inference(db, older, {"packet_id": older.packet_id}, model="mock", cache_key="old")
    import sqlite3
    with sqlite3.connect(db) as conn:
        statuses = dict(conn.execute("SELECT packet_id,status FROM macro_pricing_inferences"))
    assert statuses[newer.packet_id] == "ACTIVE"
    assert statuses[older.packet_id] == "SUPERSEDED"
