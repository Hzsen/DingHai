from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from domain.private_material import ExternalContextMode, MaterialSensitivity, RightsScope
from quant_agent.knowledge.store import KnowledgeStore
from quant_agent.private_materials.analysis import (
    build_material_analysis_prompt,
    request_material_analysis,
)
from quant_agent.private_materials.ingestion import load_manifest_metadata, load_viewpoint, register_local_material
from quant_agent.private_materials.knowledge import viewpoint_to_knowledge_bundle
from quant_agent.private_materials.policy import evaluate_egress
from quant_agent.private_materials.store import PrivateMaterialStore


FIXTURES = Path(__file__).parent / "fixtures" / "private_materials"
NOW = datetime(2026, 7, 15, 21, 0, tzinfo=timezone.utc)


def _objects():
    source = FIXTURES / "synthetic_paid_note.md"
    manifest = register_local_material(source, load_manifest_metadata(FIXTURES / "manifest.json"), now=NOW)
    viewpoint = load_viewpoint(FIXTURES / "viewpoint.json", manifest.material_id, now=NOW)
    return source, manifest, viewpoint


def test_abstracted_policy_never_egresses_original_material(monkeypatch: pytest.MonkeyPatch) -> None:
    source, manifest, viewpoint = _objects()
    raw = source.read_text(encoding="utf-8")
    monkeypatch.setenv("MOONSHOT_API_KEY", "sk-this-must-never-enter-a-prompt")

    decision = evaluate_egress(manifest, (viewpoint,), raw_text=raw, now=NOW)
    assert decision.allowed is True
    assert decision.mode is ExternalContextMode.ABSTRACTED_CLAIMS_ONLY
    assert len(decision.contexts) == 1
    joined = "\n".join(context.text for context in decision.contexts)
    assert "This is a fabricated fixture" not in joined
    assert "sk-this-must-never-enter-a-prompt" not in joined
    assert json.loads(decision.contexts[0].text)["verbatim_text_included"] is False


def test_deny_mode_blocks_all_context() -> None:
    _, manifest, viewpoint = _objects()
    manifest = replace(manifest, external_context_mode=ExternalContextMode.DENY, max_external_chars=0)
    decision = evaluate_egress(manifest, (viewpoint,), now=NOW)
    assert decision.allowed is False
    assert decision.contexts == ()
    assert decision.reason_codes == ("POLICY_DENY",)


def test_allowlisted_excerpt_requires_explicit_rights_and_exact_source_text() -> None:
    source, manifest, viewpoint = _objects()
    with pytest.raises(ValueError, match="EXTERNAL_LLM_ALLOWED"):
        replace(manifest, external_context_mode=ExternalContextMode.ALLOWLISTED_EXCERPTS)

    manifest = replace(
        manifest,
        sensitivity=MaterialSensitivity.EXTERNAL_LLM_ALLOWED,
        rights_scope=RightsScope.EXTERNAL_PROCESSING_ALLOWED,
        external_context_mode=ExternalContextMode.ALLOWLISTED_EXCERPTS,
        max_external_chars=3000,
    )
    raw = source.read_text(encoding="utf-8")
    excerpt = "AI infrastructure spending is mentioned as a possible competing"
    decision = evaluate_egress(
        manifest, (viewpoint,), allowlisted_excerpts=(excerpt,), raw_text=raw, now=NOW
    )
    assert decision.allowed is True
    assert len(decision.contexts) == 2
    assert decision.contexts[1].text == excerpt

    blocked = evaluate_egress(
        manifest, (viewpoint,), allowlisted_excerpts=("not in the source",), raw_text=raw, now=NOW
    )
    assert blocked.allowed is False
    assert blocked.reason_codes == ("EXCERPT_NOT_VERIFIED",)


def test_manifest_and_viewpoint_store_are_idempotent_and_raw_free(tmp_path: Path) -> None:
    source, manifest, viewpoint = _objects()
    store = PrivateMaterialStore(tmp_path / "private.db")
    assert store.register_manifest(manifest) is True
    assert store.register_manifest(manifest) is False
    assert store.save_viewpoint(viewpoint) is True
    assert store.save_viewpoint(viewpoint) is False

    raw = source.read_text(encoding="utf-8")
    with sqlite3.connect(store.db_path) as conn:
        schema = " ".join(row[0] or "" for row in conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table'"
        ))
        payloads = " ".join(str(row[0]) for row in conn.execute(
            "SELECT payload_json FROM private_material_manifests UNION ALL "
            "SELECT payload_json FROM private_material_viewpoints"
        ))
    assert "raw_content" not in schema
    assert raw not in payloads


def test_egress_audit_stores_hashes_not_context_or_response(tmp_path: Path) -> None:
    _, manifest, viewpoint = _objects()
    decision = evaluate_egress(manifest, (viewpoint,), now=NOW)
    store = PrivateMaterialStore(tmp_path / "audit.db")
    store.register_manifest(manifest)
    store.save_viewpoint(viewpoint)
    store.record_egress(
        decision, purpose="test", provider="mock", model="mock", outcome="SENT",
        response_hash=hashlib.sha256(b"response").hexdigest(),
    )
    row = store.audit_rows()[0]
    assert row["characters_sent"] == decision.total_characters
    assert row["context_hash"] == decision.context_hash
    assert "claim" not in row
    with sqlite3.connect(store.db_path) as conn:
        columns = {item[1] for item in conn.execute("PRAGMA table_info(llm_egress_audit)")}
    assert "prompt" not in columns
    assert "response" not in columns


def test_viewpoint_enters_knowledge_store_without_original_source(tmp_path: Path) -> None:
    source, manifest, viewpoint = _objects()
    bundle = viewpoint_to_knowledge_bundle(viewpoint, manifest)
    store = KnowledgeStore(tmp_path / "knowledge.db")
    first = store.ingest(bundle.document, bundle.chunks, "test-private-viewpoint")
    second = store.ingest(bundle.document, bundle.chunks, "test-private-viewpoint")
    assert first.index_jobs_created == 3
    assert second.index_jobs_created == 0
    assert store.table_count("knowledge_documents") == 1
    assert source.read_text(encoding="utf-8") not in bundle.document.content
    assert all(chunk.indexable for chunk in bundle.chunks)


def test_prompt_contains_numeric_packet_unchanged_but_no_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    _, manifest, viewpoint = _objects()
    decision = evaluate_egress(manifest, (viewpoint,), now=NOW)
    packet = {
        "packet_id": "macro-packet/test",
        "window_change": {"net_liquidity_change_bn": 123.45},
        "change_events": [{"event_id": "event/1", "reason_codes": ["TREASURY_REJECTION"]}],
    }
    secret = "sk-never-include-this-value"
    monkeypatch.setenv("MOONSHOT_API_KEY", secret)
    messages = build_material_analysis_prompt(packet, decision)
    prompt = json.dumps(messages, ensure_ascii=False)
    assert '"net_liquidity_change_bn":123.45' in messages[1]["content"]
    assert secret not in prompt
    assert "不得提供投资建议" in prompt
    assert "不得预测未来价格" in prompt


def test_kimi_boundary_uses_zero_temperature_and_bounded_output() -> None:
    _, manifest, viewpoint = _objects()
    decision = evaluate_egress(manifest, (viewpoint,), now=NOW)
    packet = {"packet_id": "macro-packet/test", "window_change": {"risk_score": 2.0}}
    messages = build_material_analysis_prompt(packet, decision)

    class Client:
        kwargs = None

        def complete_json(self, supplied_messages, **kwargs):
            self.kwargs = kwargs
            assert supplied_messages == messages
            return {
                "packet_id": "macro-packet/test",
                "context_hash": decision.context_hash,
                "viewpoint_assessments": [{
                    "viewpoint_id": viewpoint.viewpoint_id,
                    "status": "INSUFFICIENT_EVIDENCE",
                    "supporting_numeric_evidence": [],
                    "contradicting_numeric_evidence": [],
                    "confidence": 0.3,
                }],
                "dominant_pricing_hypothesis": "",
                "cross_source_consensus": [],
                "cross_source_conflicts": [],
                "unknowns": ["More history is required."],
                "invalidation_watch": [],
                "short_summary": "Evidence remains insufficient.",
            }

    client = Client()
    result = request_material_analysis(
        client, "mock-model", messages,
        packet_id="macro-packet/test", context_hash=decision.context_hash,
    )
    assert result["packet_id"] == "macro-packet/test"
    assert client.kwargs == {"model": "mock-model", "temperature": 0, "max_tokens": 800}
