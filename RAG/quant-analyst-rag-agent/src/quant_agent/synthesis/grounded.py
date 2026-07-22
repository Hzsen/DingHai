from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from domain.evidence import EvidencePacket, GroundedSynthesisResult
from quant_agent.synthesis.cache import GroundedSynthesisCache, grounded_cache_key
from quant_agent.synthesis.evidence_packet import evidence_packet_payload


GROUNDED_PROMPT_VERSION = "grounded-synthesis-v1.0.0"
_CONFIDENCE = {"low", "medium", "high"}


class GroundedSynthesisError(RuntimeError):
    pass


def build_grounded_prompt(packet: EvidencePacket) -> list[dict[str, str]]:
    schema = {
        "answer": "short research synthesis",
        "claims": [{"claim": "grounded claim", "evidence_ids": ["evidence id"]}],
        "contradictions": [],
        "unknowns": [],
        "confidence": "low|medium|high",
    }
    system = (
        "You are a financial research synthesis component. Use only EVIDENCE_PACKET. "
        "Return one valid JSON object and no markdown. Every factual claim must cite at least one "
        "evidence_id that exists in the packet. Preserve numeric_evidence values exactly. "
        "Do not provide investment advice, trading instructions, target prices, or future-price "
        "predictions. Separate contradictions and unknowns instead of guessing. "
        f"Required schema: {json.dumps(schema, ensure_ascii=False, separators=(',', ':'))}"
    )
    user = "EVIDENCE_PACKET:\n" + json.dumps(
        evidence_packet_payload(packet), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def validate_grounded_synthesis(payload: Mapping[str, Any], packet: EvidencePacket) -> None:
    required = {"answer", "claims", "contradictions", "unknowns", "confidence"}
    missing = required - set(payload)
    if missing:
        raise GroundedSynthesisError(f"grounded synthesis missing fields: {sorted(missing)}")
    if not isinstance(payload["answer"], str) or not payload["answer"].strip():
        raise GroundedSynthesisError("grounded synthesis answer must be a non-empty string")
    for field in ("claims", "contradictions", "unknowns"):
        if not isinstance(payload[field], list):
            raise GroundedSynthesisError(f"grounded synthesis {field} must be a list")
    if payload["confidence"] not in _CONFIDENCE:
        raise GroundedSynthesisError("grounded synthesis confidence is invalid")
    allowed_ids = {context.evidence_id for context in packet.contexts}
    for index, claim in enumerate(payload["claims"]):
        if not isinstance(claim, Mapping):
            raise GroundedSynthesisError(f"claim {index} must be an object")
        if not isinstance(claim.get("claim"), str) or not claim["claim"].strip():
            raise GroundedSynthesisError(f"claim {index} text must be non-empty")
        evidence_ids = claim.get("evidence_ids")
        if not isinstance(evidence_ids, list) or not evidence_ids:
            raise GroundedSynthesisError(f"claim {index} must cite evidence")
        if any(not isinstance(value, str) or value not in allowed_ids for value in evidence_ids):
            raise GroundedSynthesisError(f"claim {index} cites evidence outside the packet")


def request_grounded_synthesis(
    client: Any,
    model: str,
    messages: list[dict[str, str]],
    packet: EvidencePacket,
) -> dict[str, object]:
    try:
        response = client.complete_json(messages, model=model, temperature=0, max_tokens=800)
    except Exception as exc:
        raise GroundedSynthesisError("grounded synthesis provider request failed") from exc
    data = response.data if hasattr(response, "data") else response
    if not isinstance(data, Mapping):
        raise GroundedSynthesisError("grounded synthesis response is not a JSON object")
    payload = dict(data)
    validate_grounded_synthesis(payload, packet)
    return payload


def build_extractive_fallback(
    packet: EvidencePacket,
    *,
    provider_unavailable: bool = True,
) -> dict[str, object]:
    claims = [
        {
            "claim": f"{context.title} — {context.text[:240].strip()}",
            "evidence_ids": [context.evidence_id],
        }
        for context in packet.contexts[:3]
        if context.text.strip()
    ]
    return {
        "answer": (
            "Provider synthesis was unavailable; returning evidence excerpts without inference."
            if provider_unavailable
            else "Extractive-only mode; returning evidence excerpts without LLM inference."
        ),
        "claims": claims,
        "contradictions": [],
        "unknowns": ["Cross-evidence synthesis was not generated."],
        "confidence": "low",
    }


def synthesize_grounded(
    *,
    packet: EvidencePacket,
    client: Any,
    model: str,
    cache: GroundedSynthesisCache | None = None,
) -> GroundedSynthesisResult:
    selected_cache = cache or GroundedSynthesisCache()
    key = grounded_cache_key(packet.packet_id, model, GROUNDED_PROMPT_VERSION)
    cached = selected_cache.get(key)
    if cached is not None:
        validate_grounded_synthesis(cached, packet)
        return GroundedSynthesisResult(cached, "KIMI_GROUNDED", True)
    messages = build_grounded_prompt(packet)
    try:
        payload = request_grounded_synthesis(client, model, messages, packet)
    except GroundedSynthesisError:
        fallback = build_extractive_fallback(packet)
        validate_grounded_synthesis(fallback, packet)
        return GroundedSynthesisResult(
            fallback,
            "EXTRACTIVE_FALLBACK",
            False,
            "KIMI_GROUNDED_UNAVAILABLE",
        )
    selected_cache.put(key, payload)
    return GroundedSynthesisResult(payload, "KIMI_GROUNDED", False)
