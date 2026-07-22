from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Mapping


def grounded_cache_key(packet_id: str, model: str, prompt_version: str) -> str:
    material = json.dumps(
        {"packet_id": packet_id, "model": model, "prompt_version": prompt_version},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class GroundedSynthesisCache:
    def __init__(self, cache_dir: Path | str = ".cache/grounded_synthesis") -> None:
        self.cache_dir = Path(cache_dir)

    def get(self, key: str) -> dict[str, object] | None:
        path = self.cache_dir / f"{key}.json"
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("grounded synthesis cache payload must be an object")
        return payload

    def put(self, key: str, payload: Mapping[str, object]) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, indent=2)
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=self.cache_dir, prefix=f".{key}.", delete=False
        ) as handle:
            handle.write(serialized)
            temporary = Path(handle.name)
        temporary.replace(self.cache_dir / f"{key}.json")

