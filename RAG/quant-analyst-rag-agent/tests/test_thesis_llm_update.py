from __future__ import annotations

import pytest

from thesis.llm_update import ThesisUpdateError, request_thesis_update


class FakeClient:
    def __init__(self, response) -> None:
        self.response = response
        self.kwargs = None

    def complete_json(self, messages, **kwargs):
        self.kwargs = kwargs
        return self.response


def test_request_thesis_update_uses_low_token_deterministic_settings() -> None:
    client = FakeClient(
        """{
          "ticker":"300308.SZ",
          "thesis_id":"thesis-1",
          "state_change":"WATCHLIST -> THEME_WARMUP",
          "reason_codes":[],
          "factor_status":{"still_valid":[],"weakening":[],"invalidated":[],"newly_emerged":[]},
          "short_summary":"test",
          "risk_notes":"",
          "research_note_needed":false
        }"""
    )

    result = request_thesis_update(client, "test-model", [{"role": "user", "content": "test"}])

    assert result["ticker"] == "300308.SZ"
    assert client.kwargs == {"model": "test-model", "temperature": 0, "max_tokens": 800}


def test_request_thesis_update_rejects_invalid_json() -> None:
    client = FakeClient("not-json")

    with pytest.raises(ThesisUpdateError, match="not valid JSON"):
        request_thesis_update(client, "test-model", [])
