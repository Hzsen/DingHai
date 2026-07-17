from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from domain.macro_history import MacroAnalysisPacket, MacroChangeEvent, MacroHistoryPoint


PROMPT_VERSION = "macro-pricing-inference-v1.0.0"
ALLOWED_RISK_TYPES = {
    "MONETARY_POLICY_REPRICING",
    "INFLATION_REPRICING",
    "GROWTH_SLOWDOWN",
    "FISCAL_OR_TREASURY_SUPPLY",
    "LIQUIDITY_PLUMBING",
    "CREDIT_STRESS",
    "GEOPOLITICAL_OR_COMMODITY",
    "POSITIONING_UNWIND",
    "AI_CAPEX_CROWDING_OUT",
    "INSUFFICIENT_EVIDENCE",
}
_SECRET_PATTERN = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")


class MacroPricingAnalysisError(RuntimeError):
    """Safe schema or provider error for macro pricing inference."""


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _safe_context(value: str) -> str:
    lines: list[str] = []
    for line in str(value).splitlines():
        if "MOONSHOT_API_KEY" in line:
            lines.append("[secret configuration omitted]")
        else:
            lines.append(_SECRET_PATTERN.sub("[secret omitted]", line))
    return "\n".join(lines)[:1000]


def build_macro_analysis_packet(
    points: list[MacroHistoryPoint],
    events: list[MacroChangeEvent],
) -> MacroAnalysisPacket:
    if len(points) < 2:
        raise ValueError("at least two history points are required")
    first, current = points[0], points[-1]
    material = f"{current.snapshot_id}|{first.as_of.isoformat()}|{','.join(event.event_id for event in events)}"
    packet_id = "macro-packet/" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    target_change = {
        target: round(current.target_absorption[target] - first.target_absorption.get(target, 0.0), 3)
        for target in current.target_absorption
    }
    daily_history = tuple({
        "date": point.as_of.date().isoformat(),
        "net_liquidity_20d_bn": round(point.net_liquidity_20d_bn, 3),
        "liquidity_score": round(point.liquidity_score, 2),
        "risk_score": round(point.risk_score, 2),
        "rate_pressure_score": round(point.rate_pressure_score, 2),
        "target_absorption": {key: round(value, 2) for key, value in point.target_absorption.items()},
    } for point in points)
    compact_events = tuple({
        "event_id": event.event_id,
        "event_type": event.event_type,
        "entity_id": event.entity_id,
        "previous_value": event.previous_value,
        "current_value": event.current_value,
        "magnitude": event.magnitude,
        "direction": event.direction,
        "reason_codes": list(event.reason_codes),
    } for event in events)
    return MacroAnalysisPacket(
        packet_id=packet_id,
        as_of=current.as_of,
        window_start=first.as_of,
        window_days=(current.as_of.date() - first.as_of.date()).days,
        model_version=current.model_version,
        current_snapshot_id=current.snapshot_id,
        current_state={
            "net_liquidity_20d_bn": round(current.net_liquidity_20d_bn, 3),
            "liquidity_score": round(current.liquidity_score, 2),
            "risk_score": round(current.risk_score, 2),
            "rate_pressure_score": round(current.rate_pressure_score, 2),
            "source_flows_bn": {key: round(value, 3) for key, value in current.source_flows_bn.items()},
            "target_absorption": {key: round(value, 2) for key, value in current.target_absorption.items()},
            "target_states": current.target_states,
        },
        window_change={
            "net_liquidity_change_bn": round(current.net_liquidity_20d_bn - first.net_liquidity_20d_bn, 3),
            "risk_score_change": round(current.risk_score - first.risk_score, 3),
            "rate_pressure_change": round(current.rate_pressure_score - first.rate_pressure_score, 3),
            "target_absorption_change": target_change,
        },
        daily_history=daily_history,
        change_events=compact_events,
        candidate_risk_types=tuple(sorted(ALLOWED_RISK_TYPES)),
        data_quality={
            "current_confidence": round(current.confidence, 3),
            "minimum_window_confidence": round(min(point.confidence for point in points), 3),
            "history_points": len(points),
        },
    )


def build_macro_pricing_prompt(
    packet: MacroAnalysisPacket,
    retrieved_contexts: list[str],
) -> list[dict[str, str]]:
    contexts = [_safe_context(context) for context in retrieved_contexts[:3]]
    packet_payload = asdict(packet)
    packet_payload["retrieved_contexts"] = contexts
    output_schema = {
        "packet_id": packet.packet_id,
        "analysis_window_days": packet.window_days,
        "dominant_pricing_hypothesis": {
            "risk_type": "INSUFFICIENT_EVIDENCE",
            "hypothesis": "",
            "confidence": 0.0,
            "supporting_evidence": [],
            "contradicting_evidence": [],
            "invalidation_conditions": [],
        },
        "alternative_hypotheses": [],
        "flow_interpretation": "",
        "target_rotation": [],
        "unknowns": [],
        "research_note_needed": True,
    }
    system = (
        "你是全球宏观资金流研究分析器。只分析提供的14日 point-in-time packet 和最多3条 context。"
        "任务是提出市场可能正在计价的事件或风险假设，而不是断言因果。只能输出合法 JSON object。"
        "必须区分系统流动性 source flow、资产 transmission proxy 和 inference。"
        "不得修改 numeric evidence，不得提供投资建议，不得预测价格，不得补充 context 中不存在的具体新闻事实。"
        "每个假设必须引用 packet 中的 event_id/reason_code/metric 名称，同时列出反证和失效条件。"
        "证据不足时 risk_type 必须为 INSUFFICIENT_EVIDENCE。"
    )
    user = (
        "INPUT_PACKET:\n" + _canonical_json(packet_payload)
        + "\nOUTPUT_SCHEMA:\n" + _canonical_json(output_schema)
        + "\nALLOWED_RISK_TYPES:\n" + _canonical_json(sorted(ALLOWED_RISK_TYPES))
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _validate_hypothesis(value: object) -> None:
    if not isinstance(value, Mapping):
        raise MacroPricingAnalysisError("pricing hypothesis must be an object")
    required = {"risk_type", "hypothesis", "confidence", "supporting_evidence", "contradicting_evidence", "invalidation_conditions"}
    missing = required - set(value)
    if missing:
        raise MacroPricingAnalysisError(f"pricing hypothesis missing fields: {sorted(missing)}")
    if value["risk_type"] not in ALLOWED_RISK_TYPES:
        raise MacroPricingAnalysisError("pricing hypothesis has unsupported risk_type")
    confidence = value["confidence"]
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool) or not 0 <= confidence <= 1:
        raise MacroPricingAnalysisError("pricing hypothesis confidence must be between 0 and 1")
    for field in ("supporting_evidence", "contradicting_evidence", "invalidation_conditions"):
        if not isinstance(value[field], list):
            raise MacroPricingAnalysisError(f"pricing hypothesis {field} must be a list")


def validate_macro_pricing_analysis(payload: Mapping[str, Any], packet_id: str) -> None:
    required = {
        "packet_id", "analysis_window_days", "dominant_pricing_hypothesis", "alternative_hypotheses",
        "flow_interpretation", "target_rotation", "unknowns", "research_note_needed",
    }
    missing = required - set(payload)
    if missing:
        raise MacroPricingAnalysisError(f"macro pricing JSON missing fields: {sorted(missing)}")
    if payload["packet_id"] != packet_id:
        raise MacroPricingAnalysisError("macro pricing JSON packet_id mismatch")
    _validate_hypothesis(payload["dominant_pricing_hypothesis"])
    alternatives = payload["alternative_hypotheses"]
    if not isinstance(alternatives, list) or len(alternatives) > 3:
        raise MacroPricingAnalysisError("alternative_hypotheses must be a list of at most 3 items")
    for hypothesis in alternatives:
        _validate_hypothesis(hypothesis)
    if not isinstance(payload["target_rotation"], list) or not isinstance(payload["unknowns"], list):
        raise MacroPricingAnalysisError("target_rotation and unknowns must be lists")
    if not isinstance(payload["research_note_needed"], bool):
        raise MacroPricingAnalysisError("research_note_needed must be boolean")


def request_macro_pricing_analysis(client: Any, model: str, messages: list[dict[str, str]], packet_id: str) -> dict:
    try:
        response = client.complete_json(messages, model=model, temperature=0, max_tokens=800)
        payload = response.data if hasattr(response, "data") else response
        if isinstance(payload, str):
            payload = json.loads(payload)
    except (json.JSONDecodeError, TypeError, ValueError, AttributeError) as exc:
        raise MacroPricingAnalysisError("Kimi macro pricing response is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise MacroPricingAnalysisError("Kimi macro pricing response must be an object")
    validate_macro_pricing_analysis(payload, packet_id)
    return dict(payload)


def build_macro_analysis_cache_key(
    packet: MacroAnalysisPacket,
    retrieved_contexts: list[str],
    model: str,
) -> str:
    material = {
        "packet": asdict(packet),
        "contexts": [_safe_context(context) for context in retrieved_contexts[:3]],
        "model": model,
        "prompt_version": PROMPT_VERSION,
    }
    return hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()


class MacroAnalysisCache:
    def __init__(self, cache_dir: Path | str = ".cache/macro_kimi") -> None:
        self.cache_dir = Path(cache_dir)

    def get(self, key: str) -> dict | None:
        path = self.cache_dir / f"{key}.json"
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise MacroPricingAnalysisError("invalid macro Kimi cache entry")
        return payload

    def set(self, key: str, value: dict) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        destination = self.cache_dir / f"{key}.json"
        temporary = self.cache_dir / f".{key}.tmp"
        temporary.write_text(_canonical_json(value), encoding="utf-8")
        temporary.replace(destination)

    def get_or_compute(self, key: str, factory) -> tuple[dict, bool]:
        cached = self.get(key)
        if cached is not None:
            return cached, True
        value = factory()
        self.set(key, value)
        return value, False


def publish_macro_pricing_inference(
    db_path: Path | str,
    packet: MacroAnalysisPacket,
    inference: dict,
    *,
    model: str,
    cache_key: str,
) -> None:
    db_path = Path(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS macro_pricing_inferences (
            packet_id TEXT PRIMARY KEY,snapshot_id TEXT NOT NULL,as_of TEXT NOT NULL,
            window_start TEXT NOT NULL,window_days INTEGER NOT NULL,model TEXT NOT NULL,
            prompt_version TEXT NOT NULL,cache_key TEXT NOT NULL,status TEXT NOT NULL,
            payload_json TEXT NOT NULL,created_at TEXT NOT NULL
        )""")
        conn.execute("BEGIN IMMEDIATE")
        active = conn.execute("SELECT packet_id,as_of FROM macro_pricing_inferences WHERE status='ACTIVE' AND packet_id<>?", (packet.packet_id,)).fetchall()
        effective_status = "SUPERSEDED" if any(datetime.fromisoformat(old_as_of) > packet.as_of for _, old_as_of in active) else "ACTIVE"
        for old_packet_id, old_as_of in active:
            if effective_status == "ACTIVE" and datetime.fromisoformat(old_as_of) <= packet.as_of:
                conn.execute("UPDATE macro_pricing_inferences SET status='SUPERSEDED' WHERE packet_id=?", (old_packet_id,))
        conn.execute("""INSERT INTO macro_pricing_inferences VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(packet_id) DO UPDATE SET model=excluded.model,prompt_version=excluded.prompt_version,
            cache_key=excluded.cache_key,status=excluded.status,payload_json=excluded.payload_json,
            created_at=excluded.created_at""",
            (packet.packet_id, packet.current_snapshot_id, packet.as_of.isoformat(), packet.window_start.isoformat(),
             packet.window_days, model, PROMPT_VERSION, cache_key, effective_status,
             _canonical_json(inference), datetime.now(timezone.utc).isoformat()),
        )


def load_macro_pricing_inference(db_path: Path | str, packet_id: str) -> dict | None:
    db_path = Path(db_path)
    if not db_path.exists():
        return None
    with sqlite3.connect(db_path) as conn:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='macro_pricing_inferences'").fetchone()
        if exists is None:
            return None
        row = conn.execute("SELECT payload_json FROM macro_pricing_inferences WHERE packet_id=?", (packet_id,)).fetchone()
    return None if row is None else json.loads(row[0])
