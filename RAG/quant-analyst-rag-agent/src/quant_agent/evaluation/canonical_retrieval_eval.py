from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from domain.knowledge import KnowledgeDocumentStatus, KnowledgeDocumentType
from domain.query import RAGQueryRequest
from domain.retrieval_evaluation import (
    RetrievalEvalCase,
    RetrievalEvalCaseResult,
    RetrievalEvalReport,
)
from quant_agent.query.service import RAGQueryService
from quant_agent.retrieval.canonical_vector import CanonicalVectorIndex
from quant_agent.retrieval.lexical import CanonicalLexicalIndex


def load_retrieval_eval_cases(path: Path | str) -> tuple[RetrievalEvalCase, ...]:
    source = Path(path)
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("retrieval evaluation dataset must be a JSON list")
    cases: list[RetrievalEvalCase] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("retrieval evaluation cases must be JSON objects")
        cases.append(RetrievalEvalCase(
            case_id=str(item["case_id"]),
            query=str(item["query"]),
            as_of=datetime.fromisoformat(str(item["as_of"])),
            top_k=int(item.get("top_k", 5)),
            tickers=tuple(str(value) for value in item.get("tickers", [])),
            themes=tuple(str(value) for value in item.get("themes", [])),
            document_types=tuple(
                KnowledgeDocumentType(str(value)) for value in item.get("document_types", [])
            ),
            statuses=tuple(
                KnowledgeDocumentStatus(str(value))
                for value in item.get("statuses", [KnowledgeDocumentStatus.FINALIZED.value])
            ),
            relevant_document_ids=tuple(str(value) for value in item.get("relevant_document_ids", [])),
            forbidden_document_ids=tuple(str(value) for value in item.get("forbidden_document_ids", [])),
            expect_no_results=bool(item.get("expect_no_results", False)),
        ))
    if len({case.case_id for case in cases}) != len(cases):
        raise ValueError("retrieval evaluation case_id values must be unique")
    return tuple(cases)


def run_canonical_retrieval_eval(
    db_path: Path | str,
    dataset_path: Path | str,
) -> RetrievalEvalReport:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        document_rows = conn.execute(
            """SELECT document_id,version,tickers_json,themes_json,document_type,status
            FROM knowledge_documents"""
        ).fetchall()
    document_metadata = {
        (str(row["document_id"]), int(row["version"])): {
            "tickers": set(json.loads(row["tickers_json"])),
            "themes": set(json.loads(row["themes_json"])),
            "document_type": KnowledgeDocumentType(str(row["document_type"])),
            "status": KnowledgeDocumentStatus(str(row["status"])),
        }
        for row in document_rows
    }
    service = RAGQueryService(
        CanonicalLexicalIndex(db_path, initialize=False),
        CanonicalVectorIndex(db_path, initialize=False),
    )
    cases = load_retrieval_eval_cases(dataset_path)
    results: list[RetrievalEvalCaseResult] = []
    for case in cases:
        response = service.search(RAGQueryRequest(
            query_text=case.query,
            as_of=case.as_of,
            tickers=case.tickers,
            themes=case.themes,
            document_types=case.document_types,
            statuses=case.statuses,
            top_k=case.top_k,
        ))
        returned = tuple(evidence.document_id for evidence in response.evidence)
        seen_relevant: set[str] = set()
        relevant_ranks_list: list[int] = []
        for index, document_id in enumerate(returned):
            if document_id in case.relevant_document_ids and document_id not in seen_relevant:
                relevant_ranks_list.append(index + 1)
                seen_relevant.add(document_id)
        relevant_ranks = tuple(relevant_ranks_list)
        unique_relevant = set(case.relevant_document_ids)
        recall = (
            1.0
            if case.expect_no_results and not returned
            else len(unique_relevant & set(returned)) / max(len(unique_relevant), 1)
        )
        reciprocal_rank = 1.0 / min(relevant_ranks) if relevant_ranks else 0.0
        temporal_violations = sum(
            evidence.available_at > case.as_of for evidence in response.evidence
        )
        filter_violations = 0
        for evidence in response.evidence:
            metadata = document_metadata.get((evidence.document_id, evidence.document_version))
            if metadata is None:
                filter_violations += 1
                continue
            if case.tickers and not set(case.tickers) & metadata["tickers"]:
                filter_violations += 1
            if case.themes and not set(case.themes) & metadata["themes"]:
                filter_violations += 1
            if case.document_types and metadata["document_type"] not in case.document_types:
                filter_violations += 1
            if metadata["status"] not in case.statuses:
                filter_violations += 1
        forbidden_hits = sum(
            document_id in case.forbidden_document_ids for document_id in returned
        )
        reasons: list[str] = []
        if case.expect_no_results and returned:
            reasons.append("EXPECTED_NO_RESULTS")
        elif not case.expect_no_results and not relevant_ranks:
            reasons.append("NO_RELEVANT_DOCUMENT_IN_TOP_K")
        if temporal_violations:
            reasons.append("TEMPORAL_VIOLATION")
        if filter_violations:
            reasons.append("FILTER_VIOLATION")
        if forbidden_hits:
            reasons.append("FORBIDDEN_DOCUMENT_RETRIEVED")
        results.append(RetrievalEvalCaseResult(
            case_id=case.case_id,
            passed=not reasons,
            returned_document_ids=returned,
            relevant_ranks=relevant_ranks,
            recall_at_k=round(recall, 6),
            reciprocal_rank=round(reciprocal_rank, 6),
            temporal_violation_count=temporal_violations,
            filter_violation_count=filter_violations,
            forbidden_hit_count=forbidden_hits,
            failure_reasons=tuple(reasons),
        ))
    count = len(results)
    return RetrievalEvalReport(
        dataset_path=str(Path(dataset_path)),
        case_count=count,
        passed_count=sum(result.passed for result in results),
        pass_rate=round(sum(result.passed for result in results) / max(count, 1), 6),
        mean_recall_at_k=round(sum(result.recall_at_k for result in results) / max(count, 1), 6),
        mean_reciprocal_rank=round(
            sum(result.reciprocal_rank for result in results) / max(count, 1), 6
        ),
        temporal_violation_count=sum(result.temporal_violation_count for result in results),
        filter_violation_count=sum(result.filter_violation_count for result in results),
        forbidden_hit_count=sum(result.forbidden_hit_count for result in results),
        cases=tuple(results),
    )
