from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from domain.knowledge import KnowledgeDocumentStatus, KnowledgeDocumentType


@dataclass(frozen=True, slots=True)
class RetrievalEvalCase:
    case_id: str
    query: str
    as_of: datetime
    top_k: int
    tickers: tuple[str, ...] = ()
    themes: tuple[str, ...] = ()
    document_types: tuple[KnowledgeDocumentType, ...] = ()
    statuses: tuple[KnowledgeDocumentStatus, ...] = (KnowledgeDocumentStatus.FINALIZED,)
    relevant_document_ids: tuple[str, ...] = ()
    forbidden_document_ids: tuple[str, ...] = ()
    expect_no_results: bool = False

    def __post_init__(self) -> None:
        if not self.case_id.strip() or not self.query.strip():
            raise ValueError("case_id and query must not be empty")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if not 1 <= self.top_k <= 50:
            raise ValueError("top_k must be between 1 and 50")
        if self.expect_no_results and self.relevant_document_ids:
            raise ValueError("no-result cases cannot declare relevant documents")
        if not self.expect_no_results and not self.relevant_document_ids:
            raise ValueError("positive cases must declare relevant documents")
        if set(self.relevant_document_ids) & set(self.forbidden_document_ids):
            raise ValueError("a document cannot be both relevant and forbidden")


@dataclass(frozen=True, slots=True)
class RetrievalEvalCaseResult:
    case_id: str
    passed: bool
    returned_document_ids: tuple[str, ...]
    relevant_ranks: tuple[int, ...]
    recall_at_k: float
    reciprocal_rank: float
    temporal_violation_count: int
    filter_violation_count: int
    forbidden_hit_count: int
    failure_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RetrievalEvalReport:
    dataset_path: str
    case_count: int
    passed_count: int
    pass_rate: float
    mean_recall_at_k: float
    mean_reciprocal_rank: float
    temporal_violation_count: int
    filter_violation_count: int
    forbidden_hit_count: int
    cases: tuple[RetrievalEvalCaseResult, ...]
