from __future__ import annotations

import argparse
import json

from quant_agent.evaluation.answer_grounding_eval import run_grounding_eval
from quant_agent.evaluation.retrieval_eval import run_retrieval_eval
from quant_agent.evaluation.routing_eval import run_routing_eval


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Quant Analyst RAG Agent evaluations.")
    parser.add_argument("--routing", action="store_true")
    parser.add_argument("--retrieval", action="store_true")
    parser.add_argument("--grounding", action="store_true")
    args = parser.parse_args()

    run_all = not (args.routing or args.retrieval or args.grounding)
    results = {}
    if args.routing or run_all:
        results["routing"] = run_routing_eval()
    if args.retrieval or run_all:
        results["retrieval"] = run_retrieval_eval()
    if args.grounding or run_all:
        results["grounding"] = run_grounding_eval()

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
