from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone

from domain.private_material import (
    ApprovedContext,
    EgressDecision,
    ExternalContextMode,
    MacroViewpoint,
    MaterialManifest,
    ViewpointStatus,
    canonical_hash,
)


MAX_CONTEXTS = 3
DEFAULT_ABSTRACTED_LIMIT = 3_000
_SECRET_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"(?i)(MOONSHOT_API_KEY\s*[=:]\s*)\S+"),
)
_PII_PATTERNS = (
    re.compile(r"\b1[3-9]\d{9}\b"),
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
)


def redact_external_text(text: str, *, redact_personal_data: bool) -> str:
    output = text
    for pattern in _SECRET_PATTERNS:
        output = pattern.sub("[SECRET_REDACTED]", output)
    if redact_personal_data:
        for pattern in _PII_PATTERNS:
            output = pattern.sub("[PII_REDACTED]", output)
    return output


def _context(
    context_id: str,
    manifest: MaterialManifest,
    viewpoint_id: str | None,
    text: str,
    *,
    mode: ExternalContextMode,
) -> ApprovedContext:
    return ApprovedContext(
        context_id=context_id,
        material_id=manifest.material_id,
        viewpoint_id=viewpoint_id,
        mode=mode,
        text=text,
        content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )


def _abstracted_text(viewpoint: MacroViewpoint) -> str:
    payload = {
        "viewpoint_id": viewpoint.viewpoint_id,
        "topic": viewpoint.topic,
        "claim": viewpoint.claim,
        "horizon": viewpoint.horizon,
        "evidence_summary": list(viewpoint.evidence_summary),
        "market_implications": list(viewpoint.market_implications),
        "invalidation_conditions": list(viewpoint.invalidation_conditions),
        "confidence": viewpoint.confidence,
        "source_disclosure": viewpoint.source_disclosure,
        "verbatim_text_included": False,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _blocked(manifest: MaterialManifest, reason_codes: tuple[str, ...], now: datetime) -> EgressDecision:
    material = {
        "material_id": manifest.material_id,
        "mode": manifest.external_context_mode.value,
        "reasons": reason_codes,
        "source_hash": manifest.source_hash,
    }
    return EgressDecision(
        decision_id="egress/" + canonical_hash(material)[:20],
        allowed=False,
        mode=manifest.external_context_mode,
        material_ids=(manifest.material_id,),
        viewpoint_ids=(),
        reason_codes=reason_codes,
        contexts=(),
        total_characters=0,
        context_hash=canonical_hash([]),
        decided_at=now,
    )


def evaluate_egress(
    manifest: MaterialManifest,
    viewpoints: tuple[MacroViewpoint, ...],
    *,
    allowlisted_excerpts: tuple[str, ...] = (),
    raw_text: str | None = None,
    now: datetime | None = None,
) -> EgressDecision:
    """Create the only object that may cross the external LLM boundary.

    Original material is never returned in ABSTRACTED_CLAIMS_ONLY mode. Verbatim
    text is accepted only when the manifest explicitly allows external processing
    and each excerpt is an exact substring of the locally supplied source.
    """
    decided_at = now or datetime.now(timezone.utc)
    if manifest.external_context_mode is ExternalContextMode.DENY:
        return _blocked(manifest, ("POLICY_DENY",), decided_at)
    if manifest.license_expires_on is not None and manifest.license_expires_on < decided_at.date():
        return _blocked(manifest, ("LICENSE_EXPIRED",), decided_at)
    if not viewpoints and not allowlisted_excerpts:
        return _blocked(manifest, ("NO_APPROVED_CONTEXT",), decided_at)

    eligible = [
        viewpoint for viewpoint in viewpoints
        if viewpoint.material_id == manifest.material_id
        and viewpoint.status is ViewpointStatus.APPROVED
        and viewpoint.approved_for_external
        and not viewpoint.verbatim_text_included
    ]
    contexts: list[ApprovedContext] = []
    limit = manifest.max_external_chars or DEFAULT_ABSTRACTED_LIMIT
    used = 0
    for viewpoint in eligible[:MAX_CONTEXTS]:
        text = redact_external_text(_abstracted_text(viewpoint), redact_personal_data=manifest.redaction_required)
        if used + len(text) > limit:
            break
        contexts.append(_context(
            f"abstract/{viewpoint.viewpoint_id}", manifest, viewpoint.viewpoint_id, text,
            mode=ExternalContextMode.ABSTRACTED_CLAIMS_ONLY,
        ))
        used += len(text)

    reason_codes = ["ABSTRACTED_VIEWPOINT_APPROVED"] if contexts else []
    if manifest.external_context_mode is ExternalContextMode.ALLOWLISTED_EXCERPTS:
        if raw_text is None and allowlisted_excerpts:
            return _blocked(manifest, ("RAW_TEXT_REQUIRED_FOR_EXCERPT_VERIFICATION",), decided_at)
        for index, excerpt in enumerate(allowlisted_excerpts):
            if len(contexts) >= MAX_CONTEXTS:
                break
            candidate = excerpt.strip()
            if not candidate or raw_text is None or candidate not in raw_text:
                return _blocked(manifest, ("EXCERPT_NOT_VERIFIED",), decided_at)
            candidate = redact_external_text(candidate, redact_personal_data=manifest.redaction_required)
            if used + len(candidate) > limit:
                break
            contexts.append(_context(
                f"excerpt/{manifest.material_id}/{index}", manifest, None, candidate,
                mode=ExternalContextMode.ALLOWLISTED_EXCERPTS,
            ))
            used += len(candidate)
        if allowlisted_excerpts:
            reason_codes.append("VERBATIM_EXCERPT_ALLOWLISTED")

    if not contexts:
        return _blocked(manifest, ("NO_APPROVED_CONTEXT",), decided_at)
    context_hash = canonical_hash([context.content_hash for context in contexts])
    decision_material = {
        "material_id": manifest.material_id,
        "mode": manifest.external_context_mode.value,
        "viewpoints": [context.viewpoint_id for context in contexts if context.viewpoint_id],
        "context_hash": context_hash,
    }
    return EgressDecision(
        decision_id="egress/" + canonical_hash(decision_material)[:20],
        allowed=True,
        mode=manifest.external_context_mode,
        material_ids=(manifest.material_id,),
        viewpoint_ids=tuple(context.viewpoint_id for context in contexts if context.viewpoint_id is not None),
        reason_codes=tuple(reason_codes),
        contexts=tuple(contexts),
        total_characters=used,
        context_hash=context_hash,
        decided_at=decided_at,
    )
