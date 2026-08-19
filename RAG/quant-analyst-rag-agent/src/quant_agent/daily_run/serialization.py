"""JSON serialization helpers for the daily-run snapshot."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any


def jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {key: jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [jsonable(item) for item in value]
    return value


def dumps_safe(value: Any) -> str:
    """Dump JSON that is safe to embed inside a ``<script>`` block."""

    return json.dumps(jsonable(value), ensure_ascii=False, allow_nan=False).replace("</", "<\\/")
