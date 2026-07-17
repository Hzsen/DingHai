from __future__ import annotations

from datetime import datetime

from domain.knowledge import (
    KnowledgeChunk,
    KnowledgeChunkType,
    KnowledgeDocument,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeReliability,
    KnowledgeSourceType,
    content_sha256,
)
from domain.private_material import MacroViewpoint, MaterialManifest, ViewpointStatus
from quant_agent.knowledge.store import KnowledgeBundle


def render_viewpoint_markdown(viewpoint: MacroViewpoint) -> str:
    evidence = "\n".join(f"- {item}" for item in viewpoint.evidence_summary) or "- 未提供"
    implications = "\n".join(f"- {item}" for item in viewpoint.market_implications) or "- 未提供"
    invalidations = "\n".join(f"- {item}" for item in viewpoint.invalidation_conditions) or "- 未提供"
    return f"""# Macro Viewpoint: {viewpoint.title}

## Claim

{viewpoint.claim}

## Horizon

{viewpoint.horizon}

## Evidence Summary

{evidence}

## Market Implications

{implications}

## Invalidation Conditions

{invalidations}

## Provenance

- Source disclosure: {viewpoint.source_disclosure}
- Confidence: {viewpoint.confidence:.2f}
- Raw source included: false
"""


def viewpoint_to_knowledge_bundle(
    viewpoint: MacroViewpoint,
    manifest: MaterialManifest,
    *,
    version: int = 1,
) -> KnowledgeBundle:
    """Publish only the derived viewpoint; the original local file never enters RAG."""
    if viewpoint.material_id != manifest.material_id:
        raise ValueError("viewpoint and manifest material_id mismatch")
    content = render_viewpoint_markdown(viewpoint)
    finalized = viewpoint.status is ViewpointStatus.APPROVED
    status = KnowledgeDocumentStatus.FINALIZED if finalized else KnowledgeDocumentStatus.DRAFT
    document_id = f"macro-viewpoint/{viewpoint.viewpoint_id}"
    source_uri = f"private-material://{manifest.material_id}"
    metadata = {
        "material_id": manifest.material_id,
        "source_disclosure": viewpoint.source_disclosure,
        "external_context_mode": manifest.external_context_mode.value,
        "raw_source_included": False,
        "verbatim_text_included": viewpoint.verbatim_text_included,
        "approved_for_external": viewpoint.approved_for_external,
    }
    document = KnowledgeDocument(
        document_id=document_id,
        document_type=KnowledgeDocumentType.MACRO_VIEWPOINT,
        title=viewpoint.title,
        content=content,
        tickers=(),
        themes=(viewpoint.topic,),
        thesis_id=None,
        event_time=viewpoint.as_of,
        as_of=viewpoint.as_of,
        available_at=viewpoint.updated_at,
        status=status,
        version=version,
        source_type=KnowledgeSourceType.SYSTEM_DERIVED,
        source_uri=source_uri,
        source_hash=viewpoint.content_hash,
        content_hash=content_sha256(content),
        reliability=KnowledgeReliability.DERIVED,
        language="zh-CN",
        created_at=viewpoint.created_at,
        updated_at=viewpoint.updated_at,
        metadata=metadata,
    )
    sections: tuple[tuple[KnowledgeChunkType, str, str], ...] = (
        (KnowledgeChunkType.VIEWPOINT, "Claim", viewpoint.claim),
        (KnowledgeChunkType.EVIDENCE, "Evidence Summary", "\n".join(viewpoint.evidence_summary)),
        (KnowledgeChunkType.INVALIDATION, "Invalidation Conditions", "\n".join(viewpoint.invalidation_conditions)),
    )
    chunks: list[KnowledgeChunk] = []
    for ordinal, (chunk_type, section, text) in enumerate(sections):
        if not text.strip():
            continue
        chunks.append(KnowledgeChunk(
            chunk_id=f"{document_id}/{section.lower().replace(' ', '-')}",
            document_id=document_id,
            document_version=version,
            chunk_type=chunk_type,
            section=section,
            text=text,
            ordinal=ordinal,
            event_time=viewpoint.as_of,
            available_at=viewpoint.updated_at,
            content_hash=content_sha256(text),
            token_count=max(1, len(text) // 4),
            indexable=finalized,
            metadata={"material_id": manifest.material_id, "raw_source_included": False},
        ))
    return KnowledgeBundle(document=document, chunks=tuple(chunks))
