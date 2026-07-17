from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_agent.config import Paths
from quant_agent.knowledge.store import KnowledgeStore
from quant_agent.llm.kimi_client import KimiClient, KimiConfig
from quant_agent.private_materials.analysis import (
    PrivateAnalysisCache,
    build_analysis_cache_key,
    build_material_analysis_prompt,
    request_material_analysis,
    response_hash,
)
from quant_agent.private_materials.ingestion import (
    load_manifest_metadata,
    load_viewpoint,
    register_local_material,
)
from quant_agent.private_materials.knowledge import viewpoint_to_knowledge_bundle
from quant_agent.private_materials.policy import evaluate_egress
from quant_agent.private_materials.store import PrivateMaterialStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Register a local private material, publish an abstracted viewpoint, and optionally ask Kimi."
    )
    parser.add_argument("--source", required=True, help="Local .md/.txt source. Content is never copied to SQLite.")
    parser.add_argument("--manifest", required=True, help="Rights/provenance JSON file.")
    parser.add_argument("--viewpoint", required=True, help="Human-reviewed abstracted viewpoint JSON file.")
    parser.add_argument("--numeric-packet", required=True, help="Macro numeric packet JSON file.")
    parser.add_argument("--db", default="data/processed/phase1_research.db")
    parser.add_argument("--kimi-model", default=None)
    parser.add_argument("--with-kimi", action="store_true")
    return parser.parse_args()


def _resolve(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def main() -> None:
    args = parse_args()
    root = Paths().project_root
    source_path = _resolve(root, args.source)
    manifest_path = _resolve(root, args.manifest)
    viewpoint_path = _resolve(root, args.viewpoint)
    packet_path = _resolve(root, args.numeric_packet)
    db_path = _resolve(root, args.db)

    manifest = register_local_material(source_path, load_manifest_metadata(manifest_path))
    viewpoint = load_viewpoint(viewpoint_path, manifest.material_id)
    material_store = PrivateMaterialStore(db_path)
    manifest_created = material_store.register_manifest(manifest)
    viewpoint_created = material_store.save_viewpoint(viewpoint)

    bundle = viewpoint_to_knowledge_bundle(viewpoint, manifest)
    knowledge_result = KnowledgeStore(db_path).ingest(
        bundle.document, bundle.chunks, source_name="private-material-viewpoint"
    )
    decision = evaluate_egress(manifest, (viewpoint,))
    numeric_packet = json.loads(packet_path.read_text(encoding="utf-8"))
    if not isinstance(numeric_packet, dict):
        raise ValueError("numeric packet must be a JSON object")

    analysis_status = "not_requested"
    analysis_hash = None
    cache_hit = False
    audit_id = None
    prompt_ready = False
    if decision.allowed:
        messages = build_material_analysis_prompt(numeric_packet, decision)
        prompt_ready = bool(messages)
        if args.with_kimi:
            config = KimiConfig.from_env()
            model = args.kimi_model or config.model
            cache = PrivateAnalysisCache(root / ".cache" / "private_material_kimi")
            cache_key = build_analysis_cache_key(numeric_packet, decision, model)
            try:
                result, cache_hit = cache.get_or_compute(
                    cache_key,
                    lambda: request_material_analysis(
                        KimiClient(config), model, messages,
                        packet_id=str(numeric_packet.get("packet_id", "local-packet")),
                        context_hash=decision.context_hash,
                    ),
                )
                analysis_hash = response_hash(result)
                analysis_status = "cache_hit" if cache_hit else "generated"
                audit_id = material_store.record_egress(
                    decision, purpose="macro_viewpoint_validation", provider="Moonshot/Kimi", model=model,
                    outcome="CACHE_HIT" if cache_hit else "SENT", response_hash=analysis_hash,
                )
            except Exception as exc:
                material_store.record_egress(
                    decision, purpose="macro_viewpoint_validation", provider="Moonshot/Kimi", model=model,
                    outcome="FAILED", error_type=type(exc).__name__,
                )
                raise
        else:
            audit_id = material_store.record_egress(
                decision, purpose="macro_viewpoint_validation", provider="Moonshot/Kimi", model="not_called",
                outcome="DRY_RUN",
            )
            analysis_status = "dry_run"
    else:
        audit_id = material_store.record_egress(
            decision, purpose="macro_viewpoint_validation", provider="Moonshot/Kimi", model="not_called",
            outcome="BLOCKED",
        )
        analysis_status = "blocked_by_policy"

    # Deliberately exclude local source content, prompt content, excerpts, API configuration and response payload.
    output = {
        "material_id": manifest.material_id,
        "manifest_created": manifest_created,
        "viewpoint_id": viewpoint.viewpoint_id,
        "viewpoint_created": viewpoint_created,
        "knowledge_document_id": bundle.document.document_id,
        "knowledge_ingestion_status": knowledge_result.status,
        "knowledge_index_jobs_created": knowledge_result.index_jobs_created,
        "egress_allowed": decision.allowed,
        "egress_mode": decision.mode.value,
        "egress_reason_codes": list(decision.reason_codes),
        "approved_context_count": len(decision.contexts),
        "approved_context_characters": decision.total_characters,
        "approved_context_hash": decision.context_hash,
        "prompt_ready": prompt_ready,
        "analysis_status": analysis_status,
        "analysis_result_hash": analysis_hash,
        "cache_hit": cache_hit,
        "audit_id": audit_id,
        "raw_source_copied_to_database": False,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
