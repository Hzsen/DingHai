from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path

from domain.private_material import (
    EgressDecision,
    ExternalContextMode,
    MacroViewpoint,
    MaterialManifest,
    MaterialSensitivity,
    RightsScope,
    ViewpointStatus,
)


class PrivateMaterialConflictError(ValueError):
    """An immutable material/viewpoint identity was reused with different data."""


def _jsonable(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


def _json(value: object) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


SCHEMA = (
    """CREATE TABLE IF NOT EXISTS private_material_manifests (
        material_id TEXT PRIMARY KEY,title TEXT NOT NULL,local_path TEXT NOT NULL,
        source_hash TEXT NOT NULL,sensitivity TEXT NOT NULL,rights_scope TEXT NOT NULL,
        external_context_mode TEXT NOT NULL,as_of TEXT NOT NULL,payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS private_material_viewpoints (
        viewpoint_id TEXT PRIMARY KEY,material_id TEXT NOT NULL,content_hash TEXT NOT NULL,
        status TEXT NOT NULL,approved_for_external INTEGER NOT NULL,as_of TEXT NOT NULL,
        payload_json TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
        FOREIGN KEY(material_id) REFERENCES private_material_manifests(material_id)
    )""",
    """CREATE TABLE IF NOT EXISTS llm_egress_audit (
        audit_id TEXT PRIMARY KEY,decision_id TEXT NOT NULL,purpose TEXT NOT NULL,provider TEXT NOT NULL,
        model TEXT NOT NULL,outcome TEXT NOT NULL,context_mode TEXT NOT NULL,
        material_ids_json TEXT NOT NULL,viewpoint_ids_json TEXT NOT NULL,reason_codes_json TEXT NOT NULL,
        characters_sent INTEGER NOT NULL,context_hash TEXT NOT NULL,response_hash TEXT,
        error_type TEXT,created_at TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_private_viewpoints_material ON private_material_viewpoints(material_id,status)",
    "CREATE INDEX IF NOT EXISTS idx_private_material_source_hash ON private_material_manifests(source_hash)",
    "CREATE INDEX IF NOT EXISTS idx_llm_egress_created ON llm_egress_audit(created_at)",
)


class PrivateMaterialStore:
    """Metadata/viewpoint/audit store. It intentionally has no raw-content column."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            for statement in SCHEMA:
                conn.execute(statement)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def register_manifest(self, manifest: MaterialManifest) -> bool:
        payload = _json(asdict(manifest))
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT source_hash,payload_json FROM private_material_manifests WHERE material_id=?",
                (manifest.material_id,),
            ).fetchone()
            if existing is not None:
                if existing["source_hash"] != manifest.source_hash or existing["payload_json"] != payload:
                    raise PrivateMaterialConflictError(
                        "material_id is immutable; create a new manifest for changed source or policy"
                    )
                return False
            conn.execute(
                """INSERT INTO private_material_manifests VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    manifest.material_id, manifest.title, manifest.local_path, manifest.source_hash,
                    manifest.sensitivity.value, manifest.rights_scope.value,
                    manifest.external_context_mode.value, manifest.as_of.isoformat(), payload,
                    manifest.created_at.isoformat(), manifest.updated_at.isoformat(),
                ),
            )
        return True

    def save_viewpoint(self, viewpoint: MacroViewpoint) -> bool:
        payload = _json(asdict(viewpoint))
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT content_hash,payload_json FROM private_material_viewpoints WHERE viewpoint_id=?",
                (viewpoint.viewpoint_id,),
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] != viewpoint.content_hash or existing["payload_json"] != payload:
                    raise PrivateMaterialConflictError(
                        "viewpoint_id is immutable; create a new viewpoint version for changed content"
                    )
                return False
            conn.execute(
                """INSERT INTO private_material_viewpoints VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    viewpoint.viewpoint_id, viewpoint.material_id, viewpoint.content_hash,
                    viewpoint.status.value, int(viewpoint.approved_for_external), viewpoint.as_of.isoformat(),
                    payload, viewpoint.created_at.isoformat(), viewpoint.updated_at.isoformat(),
                ),
            )
        return True

    def get_manifest(self, material_id: str) -> MaterialManifest | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM private_material_manifests WHERE material_id=?", (material_id,)
            ).fetchone()
        if row is None:
            return None
        value = json.loads(row["payload_json"])
        return MaterialManifest(
            material_id=value["material_id"], title=value["title"], local_path=value["local_path"],
            source_hash=value["source_hash"], source_label=value["source_label"],
            sensitivity=MaterialSensitivity(value["sensitivity"]), rights_scope=RightsScope(value["rights_scope"]),
            external_context_mode=ExternalContextMode(value["external_context_mode"]),
            max_external_chars=int(value["max_external_chars"]), redaction_required=bool(value["redaction_required"]),
            owner=value["owner"], as_of=datetime.fromisoformat(value["as_of"]),
            license_expires_on=date.fromisoformat(value["license_expires_on"]) if value["license_expires_on"] else None,
            created_at=datetime.fromisoformat(value["created_at"]), updated_at=datetime.fromisoformat(value["updated_at"]),
        )

    def list_viewpoints(self, material_id: str, *, approved_only: bool = False) -> list[MacroViewpoint]:
        sql = "SELECT payload_json FROM private_material_viewpoints WHERE material_id=?"
        params: list[object] = [material_id]
        if approved_only:
            sql += " AND status=? AND approved_for_external=1"
            params.append(ViewpointStatus.APPROVED.value)
        sql += " ORDER BY as_of,viewpoint_id"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        output: list[MacroViewpoint] = []
        for row in rows:
            value = json.loads(row["payload_json"])
            output.append(MacroViewpoint(
                viewpoint_id=value["viewpoint_id"], material_id=value["material_id"], title=value["title"],
                topic=value["topic"], claim=value["claim"], horizon=value["horizon"],
                evidence_summary=tuple(value["evidence_summary"]),
                market_implications=tuple(value["market_implications"]),
                invalidation_conditions=tuple(value["invalidation_conditions"]),
                confidence=float(value["confidence"]), source_disclosure=value["source_disclosure"],
                verbatim_text_included=bool(value["verbatim_text_included"]),
                status=ViewpointStatus(value["status"]),
                approved_for_external=bool(value["approved_for_external"]),
                as_of=datetime.fromisoformat(value["as_of"]), created_at=datetime.fromisoformat(value["created_at"]),
                updated_at=datetime.fromisoformat(value["updated_at"]),
            ))
        return output

    def record_egress(
        self,
        decision: EgressDecision,
        *,
        purpose: str,
        provider: str,
        model: str,
        outcome: str,
        response_hash: str | None = None,
        error_type: str | None = None,
    ) -> str:
        """Record hashes and counts only; raw contexts and model responses are excluded."""
        audit_id = str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO llm_egress_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    audit_id, decision.decision_id, purpose, provider, model, outcome, decision.mode.value,
                    _json(decision.material_ids), _json(decision.viewpoint_ids), _json(decision.reason_codes),
                    decision.total_characters if outcome in {"SENT", "CACHE_HIT"} else 0,
                    decision.context_hash, response_hash, error_type, datetime.now(timezone.utc).isoformat(),
                ),
            )
        return audit_id

    def audit_rows(self) -> list[dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM llm_egress_audit ORDER BY created_at,audit_id").fetchall()
        return [dict(row) for row in rows]
