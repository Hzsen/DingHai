from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from typing import Any
from pathlib import Path

from domain.private_material import EgressDecision


PROMPT_VERSION = "private-macro-material-v1.0.0"
ALLOWED_ASSESSMENT_STATES = {"SUPPORTED", "CONTRADICTED", "MIXED", "INSUFFICIENT_EVIDENCE"}


class PrivateMaterialAnalysisError(RuntimeError):
    """Safe schema/provider error that does not include private context."""


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _packet_payload(packet: object) -> dict[str, object]:
    if is_dataclass(packet):
        value = asdict(packet)
    elif isinstance(packet, Mapping):
        value = dict(packet)
    else:
        raise TypeError("numeric_packet must be a mapping or dataclass")
    return value


def build_material_analysis_prompt(
    numeric_packet: object,
    decision: EgressDecision,
) -> list[dict[str, str]]:
    if not decision.allowed or not decision.contexts:
        raise PrivateMaterialAnalysisError("egress policy did not approve any context")
    packet = _packet_payload(numeric_packet)
    packet_id = str(packet.get("packet_id", "local-packet"))
    input_payload = {
        "numeric_packet": packet,
        "approved_contexts": [json.loads(context.text) if context.mode.value == "ABSTRACTED_CLAIMS_ONLY" else {
            "context_id": context.context_id,
            "text": context.text,
            "verbatim_excerpt": True,
        } for context in decision.contexts],
        "context_hash": decision.context_hash,
        "rights_notice": "Use for transient analysis only; do not reproduce or quote source material.",
    }
    schema = {
        "packet_id": packet_id,
        "context_hash": decision.context_hash,
        "viewpoint_assessments": [{
            "viewpoint_id": "",
            "status": "INSUFFICIENT_EVIDENCE",
            "supporting_numeric_evidence": [],
            "contradicting_numeric_evidence": [],
            "confidence": 0.0,
        }],
        "dominant_pricing_hypothesis": "",
        "cross_source_consensus": [],
        "cross_source_conflicts": [],
        "unknowns": [],
        "invalidation_watch": [],
        "short_summary": "",
    }
    system = (
        "你是宏观研究状态验证器。只使用 numeric_packet 与 approved_contexts，且 numeric_packet 是事实边界。"
        "只输出合法 JSON object。不得复述或引用原始材料，不得声称看过未提供的原文，不得提供投资建议，"
        "不得预测未来价格。你的任务是判断观点被数值证据支持、反驳、部分支持还是证据不足。"
        "所有 supporting/contradicting evidence 必须引用 packet 中真实存在的 metric/event/reason code。"
        "不要补充输入中不存在的新闻事实。"
    )
    user = "INPUT:\n" + _json(input_payload) + "\nOUTPUT_SCHEMA:\n" + _json(schema)
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def validate_material_analysis(payload: Mapping[str, Any], packet_id: str, context_hash: str) -> None:
    required = {
        "packet_id", "context_hash", "viewpoint_assessments", "dominant_pricing_hypothesis",
        "cross_source_consensus", "cross_source_conflicts", "unknowns", "invalidation_watch", "short_summary",
    }
    missing = required - set(payload)
    if missing:
        raise PrivateMaterialAnalysisError(f"material analysis JSON missing fields: {sorted(missing)}")
    if payload["packet_id"] != packet_id or payload["context_hash"] != context_hash:
        raise PrivateMaterialAnalysisError("material analysis identity mismatch")
    assessments = payload["viewpoint_assessments"]
    if not isinstance(assessments, list) or len(assessments) > 10:
        raise PrivateMaterialAnalysisError("viewpoint_assessments must be a list of at most 10 items")
    for assessment in assessments:
        if not isinstance(assessment, Mapping):
            raise PrivateMaterialAnalysisError("viewpoint assessment must be an object")
        assessment_required = {
            "viewpoint_id", "status", "supporting_numeric_evidence",
            "contradicting_numeric_evidence", "confidence",
        }
        if assessment_required - set(assessment):
            raise PrivateMaterialAnalysisError("viewpoint assessment is missing required fields")
        if assessment.get("status") not in ALLOWED_ASSESSMENT_STATES:
            raise PrivateMaterialAnalysisError("viewpoint assessment has invalid status")
        confidence = assessment.get("confidence")
        if not isinstance(confidence, (int, float)) or isinstance(confidence, bool) or not 0 <= confidence <= 1:
            raise PrivateMaterialAnalysisError("viewpoint assessment confidence must be between 0 and 1")
        for field in ("supporting_numeric_evidence", "contradicting_numeric_evidence"):
            if not isinstance(assessment[field], list):
                raise PrivateMaterialAnalysisError(f"viewpoint assessment {field} must be a list")
    for field in ("cross_source_consensus", "cross_source_conflicts", "unknowns", "invalidation_watch"):
        if not isinstance(payload[field], list):
            raise PrivateMaterialAnalysisError(f"{field} must be a list")


def request_material_analysis(
    client: Any,
    model: str,
    messages: list[dict[str, str]],
    *,
    packet_id: str,
    context_hash: str,
) -> dict[str, object]:
    try:
        response = client.complete_json(messages, model=model, temperature=0, max_tokens=800)
        payload = response.data if hasattr(response, "data") else response
        if isinstance(payload, str):
            payload = json.loads(payload)
    except (json.JSONDecodeError, TypeError, ValueError, AttributeError) as exc:
        raise PrivateMaterialAnalysisError("Kimi private-material response is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise PrivateMaterialAnalysisError("Kimi private-material response must be an object")
    validate_material_analysis(payload, packet_id, context_hash)
    return dict(payload)


def build_analysis_cache_key(numeric_packet: object, decision: EgressDecision, model: str) -> str:
    material = {
        "numeric_packet": _packet_payload(numeric_packet),
        "context_hash": decision.context_hash,
        "model": model,
        "prompt_version": PROMPT_VERSION,
    }
    return hashlib.sha256(_json(material).encode("utf-8")).hexdigest()


def response_hash(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


class PrivateAnalysisCache:
    """Caches derived JSON only; approved source contexts are never written."""

    def __init__(self, cache_dir: Path | str = ".cache/private_material_kimi") -> None:
        self.cache_dir = Path(cache_dir)

    def get(self, key: str) -> dict[str, object] | None:
        path = self.cache_dir / f"{key}.json"
        if not path.exists():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise PrivateMaterialAnalysisError("invalid private-material analysis cache entry")
        return value

    def set(self, key: str, value: dict[str, object]) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.cache_dir, 0o700)
        except OSError:
            pass
        destination = self.cache_dir / f"{key}.json"
        temporary = self.cache_dir / f".{key}.tmp"
        temporary.write_text(_json(value), encoding="utf-8")
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        temporary.replace(destination)

    def get_or_compute(self, key: str, factory) -> tuple[dict[str, object], bool]:
        cached = self.get(key)
        if cached is not None:
            return cached, True
        value = factory()
        self.set(key, value)
        return value, False
