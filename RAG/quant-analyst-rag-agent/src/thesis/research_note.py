from __future__ import annotations

from typing import Any

from domain.thesis import StockThesis, ThesisValidationResult


def _items(values: list[Any]) -> str:
    return "\n".join(f"- {value}" for value in values) if values else "- None"


def render_research_note(
    thesis: StockThesis,
    validation: ThesisValidationResult,
    llm_update: dict,
) -> str:
    factor_status = llm_update.get("factor_status", {})
    evidence = "\n".join(
        f"- `{key}`: {value}" for key, value in sorted(validation.numeric_evidence.items())
    )
    state_change = f"{validation.previous_status.value} -> {validation.new_status.value}"
    sources = _items(thesis.source_document_ids)
    return f"""# Thesis Update: {thesis.ticker} {thesis.name}

## State Change
{state_change}

## Numeric Evidence
{evidence or "- None"}

## Factor Status

### Still Valid
{_items(list(factor_status.get("still_valid", [])))}

### Weakening
{_items(list(factor_status.get("weakening", [])))}

### Invalidated
{_items(list(factor_status.get("invalidated", [])))}

### Newly Emerged
{_items(list(factor_status.get("newly_emerged", [])))}

## Short Summary
{llm_update.get("short_summary", "")}

## Risk Notes
{llm_update.get("risk_notes", "")}

## Source Thesis
- Thesis ID: `{thesis.thesis_id}`
- Theme: {thesis.theme}
- Thesis Type: `{thesis.thesis_type.value}`
- Start Date: {thesis.start_date.isoformat()}
- Source Documents:
{sources}
"""
