from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import requests


ALLOWED_EVIDENCE_STRENGTH = {"strong", "medium", "weak"}
ALLOWED_COMPANY_RELEVANCE = {"direct", "indirect", "market_misread", "contradicted", "unclear"}
ALLOWED_NARRATIVE_FRESHNESS = {"new", "reheated", "follow_on", "unknown"}
ALLOWED_FUNDAMENTAL_SUPPORT = {"strong", "medium", "weak", "contradicted", "unclear"}


class KimiAPIError(RuntimeError):
    """Safe Kimi API failure that never includes the API key."""


@dataclass(frozen=True)
class KimiConfig:
    api_key: str = field(repr=False)
    base_url: str = "https://api.moonshot.cn/v1"
    model: str = "kimi-k2.6"
    timeout_seconds: float = 60.0
    max_attempts: int = 3

    @classmethod
    def from_env(cls, *, load_dotenv_file: bool = False) -> "KimiConfig":
        """Build config from exported variables without opening dotenv files.

        ``load_dotenv_file`` remains only for call-site compatibility and is
        intentionally ignored. Secrets must already exist in the process
        environment.
        """
        del load_dotenv_file
        api_key = (os.getenv("MOONSHOT_API_KEY") or "").strip()
        if not api_key:
            raise KimiAPIError("MOONSHOT_API_KEY is not configured")
        return cls(
            api_key=api_key,
            base_url=os.getenv("MOONSHOT_BASE_URL", "https://api.moonshot.cn/v1").rstrip("/"),
            model=os.getenv("MOONSHOT_MODEL", "kimi-k2.6"),
        )


@dataclass(frozen=True)
class KimiResult:
    data: dict[str, Any]
    model: str
    usage: Mapping[str, int]
    request_id: str | None


class KimiClient:
    def __init__(
        self,
        config: KimiConfig,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.sleep = sleep

    def _chat_payload(
        self,
        messages: Sequence[Mapping[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 2_048,
    ) -> dict[str, Any]:
        return {
            "model": model or self.config.model,
            "messages": list(messages),
            "response_format": {"type": "json_object"},
            "thinking": {"type": "disabled"},
            "temperature": temperature,
            "max_completion_tokens": max_tokens,
        }

    def complete_json(
        self,
        messages: Sequence[Mapping[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 2_048,
    ) -> KimiResult:
        url = f"{self.config.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }
        payload = self._chat_payload(
            messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        last_error: Exception | None = None

        for attempt in range(self.config.max_attempts):
            try:
                response = self.session.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.config.timeout_seconds,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    raise KimiAPIError(f"Kimi API temporary failure: HTTP {response.status_code}")
                if response.status_code >= 400:
                    raise KimiAPIError(f"Kimi API rejected the request: HTTP {response.status_code}")
                body = response.json()
                content = body["choices"][0]["message"]["content"]
                parsed = json.loads(content)
                if not isinstance(parsed, dict):
                    raise KimiAPIError("Kimi JSON response is not an object")
                return KimiResult(
                    data=parsed,
                    model=str(body.get("model", model or self.config.model)),
                    usage=body.get("usage", {}),
                    request_id=body.get("id"),
                )
            except (requests.RequestException, KeyError, TypeError, ValueError, KimiAPIError) as exc:
                last_error = exc
                is_retryable = isinstance(exc, requests.RequestException) or "temporary failure" in str(exc)
                if not is_retryable or attempt + 1 >= self.config.max_attempts:
                    break
                self.sleep(2**attempt)

        raise KimiAPIError(f"Kimi request failed after {self.config.max_attempts} attempts") from last_error

    def extract_narrative(
        self,
        *,
        ticker: str,
        stock_name: str,
        published_at: str,
        source_title: str,
        source_url: str,
        document_text: str,
    ) -> KimiResult:
        """Extract structured narrative evidence from supplied text only.

        The method does not invoke Kimi product-side professional databases. Source
        acquisition remains a separate, auditable ingestion step.
        """
        system_prompt = """你是金融证据抽取器。只能使用用户提供的文档，不得补充外部事实或预测股价。
输出合法 JSON Object，必须包含：theme_name, theme_type, catalyst_type,
evidence_strength, company_relevance, narrative_freshness, theme_score,
fundamental_score, fundamental_support, narrative_conflict_flag, reasoning_summary, risk_note。
evidence_strength 只能是 strong/medium/weak；company_relevance 只能是
direct/indirect/market_misread/contradicted/unclear；narrative_freshness 只能是
new/reheated/follow_on/unknown；两个 score 必须是 0 到 3 的整数。
fundamental_support 只能是 strong/medium/weak/contradicted/unclear。
若文档证据不足，使用 unclear/unknown/低分，不得猜测。"""
        user_prompt = (
            f"ticker: {ticker}\n"
            f"stock_name: {stock_name}\n"
            f"published_at: {published_at}\n"
            f"source_title: {source_title}\n"
            f"source_url: {source_url}\n\n"
            f"document_text:\n{document_text}"
        )
        result = self.complete_json(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        self._validate_narrative(result.data)
        return result

    @staticmethod
    def _validate_narrative(data: Mapping[str, Any]) -> None:
        required = {
            "theme_name",
            "theme_type",
            "catalyst_type",
            "evidence_strength",
            "company_relevance",
            "narrative_freshness",
            "theme_score",
            "fundamental_score",
            "fundamental_support",
            "narrative_conflict_flag",
            "reasoning_summary",
            "risk_note",
        }
        missing = required - set(data)
        if missing:
            raise KimiAPIError(f"Kimi narrative result is missing fields: {sorted(missing)}")
        if data["evidence_strength"] not in ALLOWED_EVIDENCE_STRENGTH:
            raise KimiAPIError("Kimi narrative result has invalid evidence_strength")
        if data["company_relevance"] not in ALLOWED_COMPANY_RELEVANCE:
            raise KimiAPIError("Kimi narrative result has invalid company_relevance")
        if data["narrative_freshness"] not in ALLOWED_NARRATIVE_FRESHNESS:
            raise KimiAPIError("Kimi narrative result has invalid narrative_freshness")
        if data["fundamental_support"] not in ALLOWED_FUNDAMENTAL_SUPPORT:
            raise KimiAPIError("Kimi narrative result has invalid fundamental_support")
        for field_name in ("theme_score", "fundamental_score"):
            value = data[field_name]
            if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 3:
                raise KimiAPIError(f"Kimi narrative result has invalid {field_name}")
        if not isinstance(data["narrative_conflict_flag"], bool):
            raise KimiAPIError("Kimi narrative result has invalid narrative_conflict_flag")
