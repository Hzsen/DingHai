from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from domain.knowledge import KnowledgeDocumentType, KnowledgeReliability
from domain.query import RetrievedEvidence
from quant_agent.synthesis.evidence_packet import build_evidence_packet, evidence_packet_payload


NOW = datetime(2026, 7, 22, 8, tzinfo=timezone.utc)


def _evidence(index: int, text: str, *, document_id: str | None = None) -> RetrievedEvidence:
    document = document_id or f"doc/{index}"
    return RetrievedEvidence(
        evidence_id=f"knowledge:{document}::chunk-{index}@1",
        document_id=document,
        document_version=1,
        chunk_id=f"{document}::chunk-{index}",
        document_type=KnowledgeDocumentType.WEEKLY_RESEARCH,
        title=f"Evidence {index}",
        section="Summary",
        text=text,
        source_uri=f"fixture://{index}",
        event_time=NOW - timedelta(days=1),
        available_at=NOW,
        reliability=KnowledgeReliability.DERIVED,
        lexical_score=1.0,
        semantic_score=0.5,
        fusion_score=1.0,
        reason_codes=("POINT_IN_TIME_VISIBLE",),
    )


def test_packet_deduplicates_exact_and_near_duplicate_contexts() -> None:
    contexts = [
        _evidence(1, "成交额放大并突破一百二十日新高。"),
        _evidence(2, "成交额放大并突破一百二十日新高。"),
        _evidence(3, "成交额放大并突破一百二十日新高。 新增"),
        _evidence(4, "相对强度领先行业，价格完成修复。"),
    ]
    packet = build_evidence_packet(
        query="主升浪是否确认？",
        as_of=NOW,
        numeric_evidence={"amount_rank_market": 25},
        retrieved_evidence=contexts,
        near_duplicate_threshold=0.75,
    )

    assert [item.evidence_id for item in packet.contexts] == [
        contexts[0].evidence_id,
        contexts[3].evidence_id,
    ]
    assert [item.reason_code for item in packet.dropped] == [
        "EXACT_DUPLICATE",
        "NEAR_DUPLICATE",
    ]


def test_packet_enforces_document_chunk_and_token_limits_without_changing_numbers() -> None:
    numeric = {"return_20d": 0.351, "amount_rank_market": 25, "flag": True}
    contexts = [
        _evidence(index, f"独立证据 {index} " + ("不同内容" * 30), document_id=f"doc/{index // 3}")
        for index in range(21)
    ]
    packet = build_evidence_packet(
        query="验证状态",
        as_of=NOW,
        numeric_evidence=numeric,
        retrieved_evidence=contexts,
        token_budget=900,
    )

    counts: dict[str, int] = {}
    for item in packet.contexts:
        counts[item.document_id] = counts.get(item.document_id, 0) + 1
    assert len(counts) <= 6
    assert max(counts.values(), default=0) <= 2
    assert packet.estimated_tokens <= packet.token_budget
    assert packet.numeric_evidence == numeric
    assert evidence_packet_payload(packet)["numeric_evidence"] == numeric
    assert packet.dropped


def test_packet_rejects_secret_like_values_and_drops_future_evidence() -> None:
    with pytest.raises(ValueError, match="secret-like"):
        build_evidence_packet(
            query="MOONSHOT_API_KEY=sk-super-secret-value",
            as_of=NOW,
            numeric_evidence={},
            retrieved_evidence=[],
        )
    future = _evidence(1, "未来证据")
    future = RetrievedEvidence(
        **{
            field: getattr(future, field)
            for field in future.__dataclass_fields__
            if field != "available_at"
        },
        available_at=NOW + timedelta(seconds=1),
    )
    packet = build_evidence_packet(
        query="test",
        as_of=NOW,
        numeric_evidence={},
        retrieved_evidence=[future],
    )
    assert packet.contexts == ()
    assert packet.dropped[0].reason_code == "FUTURE_EVIDENCE"

