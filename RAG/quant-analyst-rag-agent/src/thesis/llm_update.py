from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


class ThesisUpdateError(RuntimeError):
    """Raised when a thesis update cannot be parsed as a JSON object."""


REQUIRED_FIELDS = {
    "ticker",
    "thesis_id",
    "state_change",
    "reason_codes",
    "factor_status",
    "short_summary",
    "risk_notes",
    "research_note_needed",
}
FACTOR_STATUS_FIELDS = {"still_valid", "weakening", "invalidated", "newly_emerged"}


def _validate_schema(payload: Mapping[str, Any]) -> None:
    missing = REQUIRED_FIELDS - set(payload)
    if missing:
        raise ThesisUpdateError(f"Thesis update JSON is missing fields: {sorted(missing)}")
    factor_status = payload.get("factor_status")
    if not isinstance(factor_status, Mapping):
        raise ThesisUpdateError("Thesis update factor_status must be an object")
    missing_factor_fields = FACTOR_STATUS_FIELDS - set(factor_status)
    if missing_factor_fields:
        raise ThesisUpdateError(
            f"Thesis update factor_status is missing fields: {sorted(missing_factor_fields)}"
        )


def request_thesis_update(client: Any, model: str, messages: list[dict]) -> dict:
    """Request a deterministic, short JSON thesis update.

    The function returns only the parsed object. It never prints prompts, headers,
    credentials, or the raw provider response.
    """
    try:
        response = client.complete_json(
            messages,
            model=model,
            temperature=0,
            max_tokens=800,
        )
        payload = response.data if hasattr(response, "data") else response
        if isinstance(payload, str):
            payload = json.loads(payload)
    except (json.JSONDecodeError, TypeError, ValueError, AttributeError) as exc:
        raise ThesisUpdateError("Thesis update response is not valid JSON") from exc

    if not isinstance(payload, Mapping):
        raise ThesisUpdateError("Thesis update JSON must be an object")
    _validate_schema(payload)
    return dict(payload)
