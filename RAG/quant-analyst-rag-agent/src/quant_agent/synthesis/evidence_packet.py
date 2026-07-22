from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from datetime import datetime
from typing import Mapping, Sequence

from domain.evidence import DroppedEvidence, EvidenceExcerpt, EvidencePacket, JSONScalar
from domain.query import RetrievedEvidence
from quant_agent.retrieval.lexical import tokenize_lexical


EVIDENCE_POLICY_VERSION = "evidence-packet-v1.0.0"
_CJK = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
_LATIN_WORD = re.compile(r"[A-Za-z0-9_./:%+-]+")
_SECRET_PATTERNS = (
    re.compile(r"(?i)MOONSHOT_API_KEY\s*[:=]\s*\S+"),
    re.compile(r"(?i)authorization\s*:\s*bearer\s+\S+"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{8,}\b"),
)


def estimate_tokens(text: str) -> int:
    """Conservative dependency-free estimate for mixed Chinese/English packets."""
    cjk_count = len(_CJK.findall(text))
    latin_pieces = _LATIN_WORD.findall(_CJK.sub(" ", text))
    latin_tokens = sum(max(1, math.ceil(len(piece) / 4)) for piece in latin_pieces)
    structural = max(1, text.count("\n") + text.count(":") // 2)
    return cjk_count + latin_tokens + structural


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _has_secret(value: str) -> bool:
    return any(pattern.search(value) for pattern in _SECRET_PATTERNS)


def _normalized_text(value: str) -> str:
    return " ".join(value.casefold().split())


def _token_set(value: str) -> set[str]:
    return set(tokenize_lexical(_normalized_text(value)))


def _near_duplicate(left: set[str], right: set[str], threshold: float) -> bool:
    if not left or not right:
        return False
    return len(left & right) / len(left | right) >= threshold


def _base_payload(
    query: str,
    as_of: datetime,
    numeric_evidence: Mapping[str, JSONScalar],
) -> dict[str, object]:
    return {
        "query": query,
        "as_of": as_of.isoformat(),
        "numeric_evidence": dict(numeric_evidence),
        "policy_version": EVIDENCE_POLICY_VERSION,
    }


def build_evidence_packet(
    *,
    query: str,
    as_of: datetime,
    numeric_evidence: Mapping[str, JSONScalar],
    retrieved_evidence: Sequence[RetrievedEvidence],
    token_budget: int = 2_400,
    max_documents: int = 6,
    max_chunks_per_document: int = 2,
    max_chars_per_context: int = 1_600,
    near_duplicate_threshold: float = 0.88,
) -> EvidencePacket:
    if not query.strip():
        raise ValueError("query must not be empty")
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    if token_budget < 128:
        raise ValueError("token_budget must be >= 128")
    if not 1 <= max_documents <= 6:
        raise ValueError("max_documents must be between 1 and 6")
    if not 1 <= max_chunks_per_document <= 2:
        raise ValueError("max_chunks_per_document must be between 1 and 2")
    numeric_copy = dict(numeric_evidence)
    if any(
        not isinstance(key, str)
        or not key.strip()
        or not isinstance(value, (str, int, float, bool, type(None)))
        for key, value in numeric_copy.items()
    ):
        raise ValueError("numeric_evidence must contain non-empty string keys and scalar values")
    numeric_json = _canonical_json(numeric_copy)
    if _has_secret(query) or _has_secret(numeric_json):
        raise ValueError("EvidencePacket input contains a secret-like value")

    base = _base_payload(query, as_of, numeric_copy)
    used_tokens = estimate_tokens(_canonical_json(base))
    if used_tokens > token_budget:
        raise ValueError("numeric evidence and packet metadata exceed token_budget")

    accepted: list[EvidenceExcerpt] = []
    dropped: list[DroppedEvidence] = []
    exact_hashes: set[str] = set()
    token_sets: list[set[str]] = []
    document_counts: Counter[str] = Counter()
    document_ids: set[str] = set()

    for evidence in retrieved_evidence:
        if evidence.available_at > as_of:
            dropped.append(DroppedEvidence(evidence.evidence_id, "FUTURE_EVIDENCE"))
            continue
        normalized = _normalized_text(evidence.text)
        if _has_secret(normalized):
            dropped.append(DroppedEvidence(evidence.evidence_id, "SECRET_REDACTED"))
            continue
        exact_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        if exact_hash in exact_hashes:
            dropped.append(DroppedEvidence(evidence.evidence_id, "EXACT_DUPLICATE"))
            continue
        tokens = _token_set(normalized)
        if any(_near_duplicate(tokens, existing, near_duplicate_threshold) for existing in token_sets):
            dropped.append(DroppedEvidence(evidence.evidence_id, "NEAR_DUPLICATE"))
            continue
        if document_counts[evidence.document_id] >= max_chunks_per_document:
            dropped.append(DroppedEvidence(evidence.evidence_id, "PER_DOCUMENT_LIMIT"))
            continue
        if evidence.document_id not in document_ids and len(document_ids) >= max_documents:
            dropped.append(DroppedEvidence(evidence.evidence_id, "DOCUMENT_LIMIT"))
            continue

        text = evidence.text[:max_chars_per_context]
        truncated = len(text) < len(evidence.text)
        excerpt_payload = {
            "evidence_id": evidence.evidence_id,
            "document_id": evidence.document_id,
            "title": evidence.title,
            "section": evidence.section,
            "text": text,
            "available_at": evidence.available_at.isoformat(),
        }
        excerpt_tokens = estimate_tokens(_canonical_json(excerpt_payload))
        if used_tokens + excerpt_tokens > token_budget:
            dropped.append(DroppedEvidence(evidence.evidence_id, "TOKEN_BUDGET"))
            continue

        accepted.append(EvidenceExcerpt(
            evidence_id=evidence.evidence_id,
            document_id=evidence.document_id,
            document_version=evidence.document_version,
            chunk_id=evidence.chunk_id,
            title=evidence.title,
            section=evidence.section,
            text=text,
            source_uri=evidence.source_uri,
            available_at=evidence.available_at,
            reliability=evidence.reliability.value,
            token_estimate=excerpt_tokens,
            truncated=truncated,
        ))
        used_tokens += excerpt_tokens
        exact_hashes.add(exact_hash)
        token_sets.append(tokens)
        document_counts[evidence.document_id] += 1
        document_ids.add(evidence.document_id)

    packet_material = {
        **base,
        "contexts": [
            {
                "evidence_id": context.evidence_id,
                "document_id": context.document_id,
                "document_version": context.document_version,
                "chunk_id": context.chunk_id,
                "title": context.title,
                "section": context.section,
                "text": context.text,
                "source_uri": context.source_uri,
                "available_at": context.available_at.isoformat(),
                "reliability": context.reliability,
                "truncated": context.truncated,
            }
            for context in accepted
        ],
    }
    packet_id = "evidence/" + hashlib.sha256(
        _canonical_json(packet_material).encode("utf-8")
    ).hexdigest()[:20]
    return EvidencePacket(
        packet_id=packet_id,
        query=query,
        as_of=as_of,
        numeric_evidence=numeric_copy,
        contexts=tuple(accepted),
        dropped=tuple(dropped),
        token_budget=token_budget,
        estimated_tokens=used_tokens,
        policy_version=EVIDENCE_POLICY_VERSION,
    )


def evidence_packet_payload(packet: EvidencePacket) -> dict[str, object]:
    """Minimal provider-bound payload; dropped evidence never leaves the process."""
    return {
        "packet_id": packet.packet_id,
        "query": packet.query,
        "as_of": packet.as_of.isoformat(),
        "numeric_evidence": packet.numeric_evidence,
        "contexts": [
            {
                "evidence_id": context.evidence_id,
                "document_id": context.document_id,
                "title": context.title,
                "section": context.section,
                "text": context.text,
                "available_at": context.available_at.isoformat(),
                "reliability": context.reliability,
            }
            for context in packet.contexts
        ],
    }
