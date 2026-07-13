from __future__ import annotations

import json

import pytest

from quant_agent.llm.kimi_client import KimiAPIError, KimiClient, KimiConfig


class FakeResponse:
    status_code = 200

    def json(self) -> dict:
        content = {
            "theme_name": "玻璃基板",
            "theme_type": "先进封装",
            "catalyst_type": "公司澄清",
            "evidence_strength": "strong",
            "company_relevance": "contradicted",
            "narrative_freshness": "new",
            "theme_score": 3,
            "fundamental_score": 0,
            "fundamental_support": "contradicted",
            "narrative_conflict_flag": True,
            "reasoning_summary": "市场叙事强但公司否认直接参与。",
            "risk_note": "叙事与基本面冲突。",
        }
        return {
            "id": "request-1",
            "model": "kimi-k2.6",
            "choices": [{"message": {"content": json.dumps(content, ensure_ascii=False)}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 80, "total_tokens": 180},
        }


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def post(self, url: str, **kwargs) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return FakeResponse()


def test_api_key_is_required_and_hidden_from_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MOONSHOT_API_KEY", raising=False)
    with pytest.raises(KimiAPIError, match="not configured"):
        KimiConfig.from_env(load_dotenv_file=False)

    config = KimiConfig(api_key="sk-secret")
    assert "sk-secret" not in repr(config)


def test_narrative_extraction_uses_json_mode_and_validates_result() -> None:
    session = FakeSession()
    client = KimiClient(KimiConfig(api_key="sk-secret"), session=session)

    result = client.extract_narrative(
        ticker="603773.SH",
        stock_name="沃格光电",
        published_at="2026-04-10",
        source_title="股票交易异常波动公告",
        source_url="https://example.com/announcement",
        document_text="公司目前未参与相关产品开发与生产。",
    )

    assert result.data["company_relevance"] == "contradicted"
    assert result.data["narrative_conflict_flag"] is True
    call = session.calls[0]
    assert call["url"] == "https://api.moonshot.cn/v1/chat/completions"
    assert call["json"]["response_format"] == {"type": "json_object"}
    assert call["json"]["thinking"] == {"type": "disabled"}
    assert call["headers"]["Authorization"] == "Bearer sk-secret"
