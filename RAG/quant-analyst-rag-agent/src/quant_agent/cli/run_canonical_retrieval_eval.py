from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from quant_agent.config import Paths
from quant_agent.evaluation.canonical_retrieval_eval import run_canonical_retrieval_eval


def _resolve(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run labeled canonical retrieval evaluation.")
    parser.add_argument("--db", default="data/processed/phase1_research.db")
    parser.add_argument("--dataset", default="data/evaluation/retrieval_cases.json")
    parser.add_argument("--minimum-pass-rate", type=float, default=1.0)
    parser.add_argument("--output", help="Optional JSON report path")
    args = parser.parse_args()
    paths = Paths()
    report = run_canonical_retrieval_eval(
        _resolve(paths.project_root, args.db),
        _resolve(paths.project_root, args.dataset),
    )
    serialized = json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        output = _resolve(paths.project_root, args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)
    return 0 if report.pass_rate >= args.minimum_pass_rate else 1


if __name__ == "__main__":
    raise SystemExit(main())
