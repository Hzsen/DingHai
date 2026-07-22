from __future__ import annotations

from pathlib import Path

from quant_agent.evaluation.canonical_retrieval_eval import load_retrieval_eval_cases


def test_labeled_retrieval_dataset_has_positive_and_point_in_time_negative_cases() -> None:
    root = Path(__file__).resolve().parents[1]
    cases = load_retrieval_eval_cases(root / "data/evaluation/retrieval_cases.json")

    assert len(cases) >= 8
    assert any(case.expect_no_results for case in cases)
    assert any(case.tickers for case in cases)
    assert all(case.relevant_document_ids or case.expect_no_results for case in cases)

