from __future__ import annotations

import json
import re

from domain.thesis import StockThesis, ThesisValidationResult


_SECRET_PATTERN = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")


def _safe_context(value: str) -> str:
    sanitized_lines = []
    for line in str(value).splitlines():
        if "MOONSHOT_API_KEY" in line:
            sanitized_lines.append("[secret configuration omitted]")
        else:
            sanitized_lines.append(_SECRET_PATTERN.sub("[secret omitted]", line))
    return "\n".join(sanitized_lines)[:1000]


def build_thesis_update_prompt(
    thesis: StockThesis,
    validation: ThesisValidationResult,
    retrieved_contexts: list[str],
) -> list[dict]:
    """Build a compact, evidence-bound prompt without reading environment state."""
    contexts = [_safe_context(context) for context in retrieved_contexts[:3]]
    input_payload = {
        "thesis": {
            "ticker": thesis.ticker,
            "name": thesis.name,
            "thesis_id": thesis.thesis_id,
            "theme": thesis.theme,
            "thesis_type": thesis.thesis_type.value,
            "key_factors": thesis.key_factors,
            "validation_signals": thesis.validation_signals,
            "invalidation_signals": thesis.invalidation_signals,
            "narrative_summary": thesis.narrative_summary,
            "fundamental_logic": thesis.fundamental_logic,
            "capital_flow_logic": thesis.capital_flow_logic,
            "risk_notes": thesis.risk_notes,
        },
        "validation": {
            "previous_status": validation.previous_status.value,
            "new_status": validation.new_status.value,
            "reason_codes": validation.reason_codes,
            "numeric_evidence": validation.numeric_evidence,
            "needs_research_note": validation.needs_research_note,
        },
        "retrieved_contexts": contexts,
    }
    output_schema = {
        "ticker": thesis.ticker,
        "thesis_id": thesis.thesis_id,
        "state_change": f"{validation.previous_status.value} -> {validation.new_status.value}",
        "reason_codes": [],
        "factor_status": {
            "still_valid": [],
            "weakening": [],
            "invalidated": [],
            "newly_emerged": [],
        },
        "short_summary": "",
        "risk_notes": "",
        "research_note_needed": validation.needs_research_note,
    }
    system = (
        "你是股票研究状态更新器。只能依据提供的 thesis、numeric_evidence 和 retrieved_contexts，"
        "只输出合法 JSON object。不得提供投资建议，不得预测未来价格，不得补充外部事实。"
        "任务仅限研究 thesis 状态更新；证据不足时明确写入 risk_notes。"
    )
    user = (
        "INPUT_JSON:\n"
        + json.dumps(input_payload, ensure_ascii=False, separators=(",", ":"))
        + "\nOUTPUT_SCHEMA:\n"
        + json.dumps(output_schema, ensure_ascii=False, separators=(",", ":"))
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]
