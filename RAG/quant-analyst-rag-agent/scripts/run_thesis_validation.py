#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from domain.thesis import StockThesis, ThesisStatus, ThesisType, ThesisValidationResult  # noqa: E402
from quant_agent.llm import KimiClient, KimiConfig  # noqa: E402
from thesis.cache import ThesisUpdateCache, build_cache_key  # noqa: E402
from thesis.llm_update import request_thesis_update  # noqa: E402
from thesis.prompting import build_thesis_update_prompt  # noqa: E402
from thesis.research_note import render_research_note  # noqa: E402
from thesis.rules import validate_thesis_state  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate one A-share thesis lifecycle state.")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--date", required=True, dest="as_of")
    parser.add_argument("--model", default="kimi-k2.6")
    return parser.parse_args()


def _read_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"Mock input not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_thesis(path: Path) -> StockThesis:
    raw = _read_json(path)
    return StockThesis(
        thesis_id=raw["thesis_id"],
        ticker=raw["ticker"],
        name=raw["name"],
        theme=raw["theme"],
        thesis_type=ThesisType(raw["thesis_type"]),
        start_date=date.fromisoformat(raw["start_date"]),
        end_date=date.fromisoformat(raw["end_date"]) if raw.get("end_date") else None,
        status=ThesisStatus(raw["status"]),
        key_factors=list(raw.get("key_factors", [])),
        validation_signals=list(raw.get("validation_signals", [])),
        invalidation_signals=list(raw.get("invalidation_signals", [])),
        narrative_summary=raw.get("narrative_summary", ""),
        fundamental_logic=raw.get("fundamental_logic", ""),
        capital_flow_logic=raw.get("capital_flow_logic", ""),
        risk_notes=raw.get("risk_notes", ""),
        source_document_ids=list(raw.get("source_document_ids", [])),
        created_at=datetime.fromisoformat(raw["created_at"]),
        updated_at=datetime.fromisoformat(raw["updated_at"]),
    )


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _validation_dict(validation: ThesisValidationResult) -> dict[str, Any]:
    return _jsonable(asdict(validation))


def main() -> int:
    args = parse_args()
    mock_dir = PROJECT_ROOT / "data" / "mock"
    safe_ticker = args.ticker.replace(".", "_")
    thesis = _load_thesis(mock_dir / f"thesis_{args.ticker.split('.', maxsplit=1)[0]}.json")
    if thesis.ticker != args.ticker:
        raise ValueError("Mock thesis ticker does not match --ticker")
    features = _read_json(mock_dir / f"daily_features_{args.ticker.split('.', maxsplit=1)[0]}_{args.as_of}.json")
    validation = validate_thesis_state(thesis, features)
    output: dict[str, Any] = {"validation": _validation_dict(validation)}

    if not validation.needs_llm_update:
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    contexts_path = mock_dir / f"retrieved_contexts_{args.ticker.split('.', maxsplit=1)[0]}_{args.as_of}.json"
    retrieved_contexts = list(_read_json(contexts_path))[:3]
    messages = build_thesis_update_prompt(thesis, validation, retrieved_contexts)
    cache = ThesisUpdateCache(PROJECT_ROOT / ".cache" / "thesis_llm")
    cache_key = build_cache_key(validation, retrieved_contexts)
    cached = cache.get(cache_key)

    if cached is not None:
        llm_update = cached
        cache_hit = True
    else:
        api_key = os.getenv("MOONSHOT_API_KEY")
        if not api_key:
            output["moonshot_api_key_configured"] = bool(api_key)
            output["llm_update_skipped"] = "MOONSHOT_API_KEY is not exported in this process"
            print(json.dumps(output, ensure_ascii=False, indent=2))
            return 0
        client = KimiClient(KimiConfig(api_key=api_key, model=args.model))
        llm_update, cache_hit = cache.get_or_compute(
            cache_key,
            lambda: request_thesis_update(client, args.model, messages),
        )

    output["cache_hit"] = cache_hit
    output["llm_update"] = llm_update
    if validation.needs_research_note:
        note_dir = PROJECT_ROOT / "outputs" / "thesis_notes"
        note_dir.mkdir(parents=True, exist_ok=True)
        note_path = note_dir / f"{safe_ticker}_{args.as_of}_{validation.new_status.value}.md"
        note_path.write_text(render_research_note(thesis, validation, llm_update), encoding="utf-8")
        output["research_note_path"] = str(note_path.relative_to(PROJECT_ROOT))

    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
