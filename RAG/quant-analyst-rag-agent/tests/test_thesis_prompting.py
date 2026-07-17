from __future__ import annotations

import json

import pytest

from domain.thesis import ThesisStatus
from thesis.prompting import build_thesis_update_prompt
from thesis.rules import validate_thesis_state
from test_thesis_rules import _thesis


def test_prompt_uses_at_most_three_contexts_and_preserves_evidence() -> None:
    thesis = _thesis(ThesisStatus.THEME_WARMUP)
    features = {"distance_to_120d_high": -0.02, "amount_ratio_20d": 1.8, "rs_market_20d": 0.01}
    validation = validate_thesis_state(thesis, features)
    messages = build_thesis_update_prompt(thesis, validation, ["one", "two", "three", "four"])
    prompt = messages[1]["content"]

    assert len(messages) == 2
    assert '"retrieved_contexts":["one","two","three"]' in prompt
    assert "four" not in prompt
    for key, value in features.items():
        assert json.dumps(key, ensure_ascii=False) in prompt
        assert json.dumps(value, ensure_ascii=False) in prompt


def test_prompt_does_not_include_environment_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "sk-test-secret-that-must-not-leak"
    monkeypatch.setenv("MOONSHOT_API_KEY", secret)
    thesis = _thesis(ThesisStatus.THEME_WARMUP)
    validation = validate_thesis_state(
        thesis,
        {"distance_to_120d_high": -0.02, "amount_ratio_20d": 2, "rs_market_20d": 0.02},
    )

    messages = build_thesis_update_prompt(thesis, validation, ["safe context"])

    assert secret not in json.dumps(messages, ensure_ascii=False)


def test_prompt_truncates_and_sanitizes_contexts() -> None:
    thesis = _thesis(ThesisStatus.THEME_WARMUP)
    validation = validate_thesis_state(
        thesis,
        {"distance_to_120d_high": -0.02, "amount_ratio_20d": 2, "rs_market_20d": 0.02},
    )
    context = "MOONSHOT_API_KEY=sk-should-not-appear\n" + ("x" * 2_000)

    messages = build_thesis_update_prompt(thesis, validation, [context])
    prompt = messages[1]["content"]

    assert "sk-should-not-appear" not in prompt
    assert "x" * 1001 not in prompt
