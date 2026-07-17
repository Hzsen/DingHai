from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

from domain.thesis import ThesisValidationResult


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def effective_contexts(retrieved_contexts: list[str]) -> list[str]:
    return [str(context)[:1000] for context in retrieved_contexts[:3]]


def build_cache_key(validation: ThesisValidationResult, retrieved_contexts: list[str]) -> str:
    context_hash = hashlib.sha256(_canonical_json(effective_contexts(retrieved_contexts)).encode("utf-8")).hexdigest()
    material = {
        "thesis_id": validation.thesis_id,
        "previous_status": validation.previous_status.value,
        "new_status": validation.new_status.value,
        "reason_codes": sorted(validation.reason_codes),
        "numeric_evidence": validation.numeric_evidence,
        "retrieved_context_hash": context_hash,
    }
    return hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()


class ThesisUpdateCache:
    def __init__(self, cache_dir: Path | str = ".cache/thesis_llm") -> None:
        self.cache_dir = Path(cache_dir)

    def get(self, key: str) -> dict | None:
        path = self.cache_dir / f"{key}.json"
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Invalid thesis cache entry: {key}") from exc
        if not isinstance(value, dict):
            raise RuntimeError(f"Invalid thesis cache entry: {key}")
        return value

    def set(self, key: str, value: dict) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        destination = self.cache_dir / f"{key}.json"
        temporary = self.cache_dir / f".{key}.tmp"
        temporary.write_text(_canonical_json(value), encoding="utf-8")
        temporary.replace(destination)

    def get_or_compute(self, key: str, factory: Callable[[], dict]) -> tuple[dict, bool]:
        cached = self.get(key)
        if cached is not None:
            return cached, True
        value = factory()
        self.set(key, value)
        return value, False
