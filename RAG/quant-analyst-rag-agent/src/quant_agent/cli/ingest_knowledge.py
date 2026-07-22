from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from quant_agent.config import Paths
from quant_agent.knowledge.adapters import (
    KnowledgeMigrationService,
    ScreeningReportAdapter,
    StaticMarkdownAdapter,
    ThesisNoteAdapter,
    WeeklyResearchAdapter,
)
from quant_agent.knowledge.store import KnowledgeStore


SOURCES = ("static", "weekly", "thesis", "screening")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate real research artifacts into the canonical KnowledgeStore."
    )
    parser.add_argument("--db", default="data/processed/phase1_research.db")
    parser.add_argument("--source", action="append", choices=SOURCES)
    parser.add_argument("--docs-dir", default="data/docs")
    parser.add_argument("--thesis-dir", default="outputs/thesis_notes")
    parser.add_argument("--screening-report", default="outputs/reversal/reversal_screen_2026-07-14.md")
    parser.add_argument("--screening-as-of", default="2026-07-14")
    return parser


def _resolve(project_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (project_root / path).resolve()


def main() -> int:
    args = build_parser().parse_args()
    paths = Paths()
    project_root = paths.project_root
    db_path = _resolve(project_root, args.db)
    selected = tuple(args.source or SOURCES)
    adapters = {
        "static": StaticMarkdownAdapter(
            _resolve(project_root, args.docs_dir), project_root=project_root
        ),
        "weekly": WeeklyResearchAdapter(db_path),
        "thesis": ThesisNoteAdapter(
            _resolve(project_root, args.thesis_dir), project_root=project_root
        ),
        "screening": ScreeningReportAdapter(
            db_path,
            _resolve(project_root, args.screening_report),
            as_of=args.screening_as_of,
            project_root=project_root,
        ),
    }
    service = KnowledgeMigrationService(KnowledgeStore(db_path))
    results = [service.migrate(adapters[source]) for source in selected]
    payload = {
        "database": str(db_path),
        "sources": [asdict(result) for result in results],
        "totals": {
            "discovered_documents": sum(result.discovered_documents for result in results),
            "migrated_documents": sum(result.migrated_documents for result in results),
            "skipped_unchanged": sum(result.skipped_unchanged for result in results),
            "migrated_chunks": sum(result.migrated_chunks for result in results),
            "index_jobs_created": sum(result.index_jobs_created for result in results),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
