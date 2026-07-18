from __future__ import annotations

from datetime import datetime, timezone

import pytest

from domain.knowledge import KnowledgeDocumentStatus
from domain.query import RAGQueryRequest


NOW = datetime(2026, 7, 17, 8, 0, tzinfo=timezone.utc)


def test_query_contract_requires_point_in_time_and_defaults_to_finalized() -> None:
    request = RAGQueryRequest("韩国加息与半导体", NOW)

    assert request.statuses == (KnowledgeDocumentStatus.FINALIZED,)
    assert request.top_k == 8


def test_query_contract_rejects_naive_time_and_retracted_documents() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        RAGQueryRequest("test", datetime(2026, 7, 17))
    with pytest.raises(ValueError, match="RETRACTED"):
        RAGQueryRequest("test", NOW, statuses=(KnowledgeDocumentStatus.RETRACTED,))
