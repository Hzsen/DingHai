from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

from domain.private_material import (
    ExternalContextMode,
    MacroViewpoint,
    MaterialManifest,
    MaterialSensitivity,
    RightsScope,
    ViewpointStatus,
)


SUPPORTED_LOCAL_SUFFIXES = {".md", ".txt"}


def register_local_material(
    source_path: Path | str,
    metadata: dict[str, object],
    *,
    now: datetime | None = None,
) -> MaterialManifest:
    """Hash and register a local text source without copying its content."""
    path = Path(source_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"private material does not exist: {path}")
    if path.suffix.lower() not in SUPPORTED_LOCAL_SUFFIXES:
        raise ValueError("MVP supports local .md and .txt files only")
    source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    fallback_timestamp = now or datetime.now(timezone.utc)
    created_at = datetime.fromisoformat(str(metadata.get("created_at", fallback_timestamp.isoformat())))
    updated_at = datetime.fromisoformat(str(metadata.get("updated_at", created_at.isoformat())))
    material_id = str(metadata.get("material_id") or f"material/{source_hash[:20]}")
    expiry = metadata.get("license_expires_on")
    return MaterialManifest(
        material_id=material_id,
        title=str(metadata["title"]),
        local_path=str(path),
        source_hash=source_hash,
        source_label=str(metadata.get("source_label", "private-source")),
        sensitivity=MaterialSensitivity(str(metadata.get("sensitivity", "LICENSED_LOCAL_ONLY"))),
        rights_scope=RightsScope(str(metadata.get("rights_scope", "PERSONAL_RESEARCH_ONLY"))),
        external_context_mode=ExternalContextMode(
            str(metadata.get("external_context_mode", "ABSTRACTED_CLAIMS_ONLY"))
        ),
        max_external_chars=int(metadata.get("max_external_chars", 3000)),
        redaction_required=bool(metadata.get("redaction_required", True)),
        owner=str(metadata.get("owner", "local-user")),
        as_of=datetime.fromisoformat(str(metadata["as_of"])),
        license_expires_on=date.fromisoformat(str(expiry)) if expiry else None,
        created_at=created_at,
        updated_at=updated_at,
    )


def load_manifest_metadata(path: Path | str) -> dict[str, object]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("manifest metadata must be a JSON object")
    return payload


def load_viewpoint(path: Path | str, material_id: str, *, now: datetime | None = None) -> MacroViewpoint:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("viewpoint file must be a JSON object")
    fallback_timestamp = now or datetime.now(timezone.utc)
    created_at = datetime.fromisoformat(str(value.get("created_at", fallback_timestamp.isoformat())))
    updated_at = datetime.fromisoformat(str(value.get("updated_at", created_at.isoformat())))
    as_of = datetime.fromisoformat(str(value["as_of"]))
    return MacroViewpoint(
        viewpoint_id=str(value["viewpoint_id"]), material_id=material_id, title=str(value["title"]),
        topic=str(value["topic"]), claim=str(value["claim"]), horizon=str(value["horizon"]),
        evidence_summary=tuple(str(item) for item in value.get("evidence_summary", [])),
        market_implications=tuple(str(item) for item in value.get("market_implications", [])),
        invalidation_conditions=tuple(str(item) for item in value.get("invalidation_conditions", [])),
        confidence=float(value["confidence"]), source_disclosure=str(value["source_disclosure"]),
        verbatim_text_included=bool(value.get("verbatim_text_included", False)),
        status=ViewpointStatus(str(value.get("status", "DRAFT"))),
        approved_for_external=bool(value.get("approved_for_external", False)),
        as_of=as_of, created_at=created_at, updated_at=updated_at,
    )
