from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from domain.knowledge import KnowledgeDocumentType, KnowledgeReliability
from domain.query import RetrievedEvidence
from quant_agent.synthesis.cache import GroundedSynthesisCache
from quant_agent.synthesis.evidence_packet import build_evidence_packet
from quant_agent.synthesis.grounded import (
    GroundedSynthesisError,
    build_grounded_prompt,
    request_grounded_synthesis,
    synthesize_grounded,
    validate_grounded_synthesis,
)
from quant_agent.cli.main import build_parser


NOW = datetime(2026, 7, 22, tzinfo=timezone.utc)


def _packet():
    evidence = RetrievedEvidence(
        evidence_id="knowledge:weekly/leader::summary@1",
        document_id="weekly/leader",
        document_version=1,
        chunk_id="weekly/leader::summary",
        document_type=KnowledgeDocumentType.WEEKLY_RESEARCH,
        title="Weekly leader state",
        section="Summary",
        text="成交额排名进入前五十，相对强度保持为正。",
        source_uri=None,
        event_time=NOW,
        available_at=NOW,
        reliability=KnowledgeReliability.DERIVED,
        lexical_score=1,
        semantic_score=1,
        fusion_score=1,
        reason_codes=("POINT_IN_TIME_VISIBLE",),
    )
    return build_evidence_packet(
        query="状态发生了什么变化？",
        as_of=NOW,
        numeric_evidence={"amount_rank_market": 42, "rs_market_20d": 0.12},
        retrieved_evidence=[evidence],
    )


def _valid_payload(packet):
    return {
        "answer": "量价与相对强度证据支持状态改善。",
        "claims": [{
            "claim": "成交额排名进入前五十。",
            "evidence_ids": [packet.contexts[0].evidence_id],
        }],
        "contradictions": [],
        "unknowns": [],
        "confidence": "medium",
    }


class FakeClient:
    def __init__(self, payload=None, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.calls = 0
        self.kwargs = None

    def complete_json(self, messages, **kwargs):
        self.calls += 1
        self.kwargs = kwargs
        if self.error:
            raise self.error
        return self.payload


def test_prompt_contains_only_packet_evidence_and_json_guardrails() -> None:
    packet = _packet()
    messages = build_grounded_prompt(packet)
    combined = "\n".join(message["content"] for message in messages)
    provider_packet = json.loads(messages[1]["content"].split("\n", 1)[1])

    assert packet.contexts[0].evidence_id in combined
    assert provider_packet["numeric_evidence"] == packet.numeric_evidence
    assert "investment advice" in combined
    assert "future-price" in combined
    assert "dropped" not in provider_packet


def test_grounded_request_uses_deterministic_low_token_settings() -> None:
    packet = _packet()
    client = FakeClient(_valid_payload(packet))
    payload = request_grounded_synthesis(client, "mock-model", build_grounded_prompt(packet), packet)

    assert payload["confidence"] == "medium"
    assert client.kwargs == {"model": "mock-model", "temperature": 0, "max_tokens": 800}


def test_validator_rejects_uncited_and_unknown_evidence_claims() -> None:
    packet = _packet()
    uncited = _valid_payload(packet)
    uncited["claims"][0]["evidence_ids"] = []
    with pytest.raises(GroundedSynthesisError, match="must cite"):
        validate_grounded_synthesis(uncited, packet)
    unknown = _valid_payload(packet)
    unknown["claims"][0]["evidence_ids"] = ["knowledge:not-in-packet@1"]
    with pytest.raises(GroundedSynthesisError, match="outside the packet"):
        validate_grounded_synthesis(unknown, packet)


def test_cache_hit_skips_provider_and_provider_error_falls_back(tmp_path) -> None:
    packet = _packet()
    cache = GroundedSynthesisCache(tmp_path / "cache")
    client = FakeClient(_valid_payload(packet))

    first = synthesize_grounded(packet=packet, client=client, model="mock", cache=cache)
    second = synthesize_grounded(packet=packet, client=client, model="mock", cache=cache)
    fallback = synthesize_grounded(
        packet=packet,
        client=FakeClient(error=RuntimeError("offline")),
        model="different-model",
        cache=cache,
    )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert client.calls == 1
    assert fallback.mode == "EXTRACTIVE_FALLBACK"
    assert fallback.warning == "KIMI_GROUNDED_UNAVAILABLE"
    validate_grounded_synthesis(fallback.payload, packet)


def test_answer_cli_requires_explicit_kimi_opt_in() -> None:
    parser = build_parser()
    default = parser.parse_args(["answer", "test query"])
    enabled = parser.parse_args(["answer", "test query", "--use-kimi"])

    assert default.use_kimi is False
    assert enabled.use_kimi is True
