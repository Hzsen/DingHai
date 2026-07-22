from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, time, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Sequence

from domain.knowledge import KnowledgeDocumentStatus, KnowledgeDocumentType
from domain.query import QueryMode, RAGQueryRequest
from quant_agent.config import Paths
from quant_agent.knowledge.store import KnowledgeStore
from quant_agent.query.service import RAGQueryService
from quant_agent.retrieval.canonical_vector import VECTOR_INDEX_VERSION, CanonicalVectorIndex
from quant_agent.retrieval.index_worker import KnowledgeIndexWorker
from quant_agent.retrieval.lexical import INDEX_VERSION, CanonicalLexicalIndex
from quant_agent.retrieval.markdown_migration import migrate_markdown_documents


def _parse_as_of(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    stripped = value.strip()
    if len(stripped) == 10:
        parsed_date = datetime.fromisoformat(stripped).date()
        local_zone = datetime.now().astimezone().tzinfo or timezone.utc
        return datetime.combine(parsed_date, time.max, tzinfo=local_zone)
    parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("--as-of must include a timezone when a time is supplied")
    return parsed


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _print_json(value: Any) -> None:
    print(json.dumps(_jsonable(value), ensure_ascii=False, indent=2, sort_keys=True))


def _resolve_db(paths: Paths, value: str | None) -> Path:
    if value is None:
        return paths.knowledge_db_path
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (paths.project_root / path).resolve()


def _runtime(
    paths: Paths,
    db_value: str | None,
) -> tuple[KnowledgeStore, CanonicalLexicalIndex, CanonicalVectorIndex]:
    db_path = _resolve_db(paths, db_value)
    store = KnowledgeStore(db_path)
    return store, CanonicalLexicalIndex(db_path), CanonicalVectorIndex(db_path)


def _search(args: argparse.Namespace, paths: Paths) -> int:
    store, lexical_index, vector_index = _runtime(paths, args.db)
    if not args.no_bootstrap:
        migrate_markdown_documents(store, paths.docs_dir, project_root=paths.project_root)
        lexical_index.reconcile(store)
        sync = KnowledgeIndexWorker(store, lexical_index, vector_index).sync(max_jobs=100_000)
        if sync.failed:
            raise RuntimeError(f"index sync failed for {sync.failed} jobs")
        vector_index.reconcile(store)
    request = RAGQueryRequest(
        query_text=args.query,
        as_of=_parse_as_of(args.as_of),
        mode=QueryMode.SEARCH_ONLY,
        tickers=tuple(args.ticker),
        themes=tuple(args.theme),
        document_types=tuple(KnowledgeDocumentType(value) for value in args.document_type),
        statuses=(
            (KnowledgeDocumentStatus.FINALIZED, KnowledgeDocumentStatus.DRAFT)
            if args.include_drafts
            else (KnowledgeDocumentStatus.FINALIZED,)
        ),
        top_k=args.top_k,
    )
    response = RAGQueryService(lexical_index, vector_index).search(request)
    if args.json:
        _print_json(response)
        return 0
    print(f"query_id: {response.query_id}")
    print(f"as_of: {response.data_as_of.isoformat()}")
    print(f"index: {response.index_mode}")
    print(f"hits: {len(response.evidence)}")
    for position, item in enumerate(response.evidence, start=1):
        snippet = " ".join(item.text.split())
        if len(snippet) > 360:
            snippet = f"{snippet[:357]}..."
        print(f"\n[{position}] {item.title} / {item.section}")
        print(f"score={item.fusion_score:.4f}  chunk={item.chunk_id}@{item.document_version}")
        print(snippet)
        if item.source_uri:
            print(f"source={item.source_uri}")
    for warning in response.warnings:
        print(f"warning: {warning}")
    return 0


def _index(args: argparse.Namespace, paths: Paths) -> int:
    store, lexical_index, vector_index = _runtime(paths, args.db)
    if args.index_command == "migrate-markdown":
        docs_dir = Path(args.docs_dir).resolve() if args.docs_dir else paths.docs_dir
        _print_json(migrate_markdown_documents(store, docs_dir, project_root=paths.project_root))
        return 0
    if args.index_command == "sync":
        lexical_reconciliation = lexical_index.reconcile(store)
        result = KnowledgeIndexWorker(store, lexical_index, vector_index).sync(max_jobs=args.max_jobs)
        reconciliation = None if args.no_reconcile else vector_index.reconcile(store)
        _print_json({
            "lexical_reconciliation": lexical_reconciliation,
            "sync": result,
            "vector_reconciliation": reconciliation,
        })
        return 1 if result.failed else 0
    if args.index_command == "status":
        canonical_manifest = store.current_index_manifest()
        lexical_manifest = lexical_index.manifest()
        vector_manifest = vector_index.manifest()
        _print_json({
            "database": store.db_path,
            "lexical_index_version": INDEX_VERSION,
            "vector_index_version": VECTOR_INDEX_VERSION,
            "canonical_indexable_chunks": len(canonical_manifest),
            "indexed_lexical_chunks": len(lexical_manifest),
            "indexed_vectors": len(vector_manifest),
            "lexical_in_parity": canonical_manifest == lexical_manifest,
            "vector_in_parity": canonical_manifest == vector_manifest,
            "indexes_in_parity": canonical_manifest == lexical_manifest == vector_manifest,
            "outbox": store.index_job_counts(),
        })
        return 0
    raise ValueError(f"unknown index command: {args.index_command}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quant-agent", description="Canonical local RAG command line")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search = subparsers.add_parser("search", help="Search canonical KnowledgeDocument/KnowledgeChunk")
    search.add_argument("query")
    search.add_argument("--as-of", help="ISO datetime with timezone, or YYYY-MM-DD")
    search.add_argument("--top-k", type=int, default=8)
    search.add_argument("--ticker", action="append", default=[])
    search.add_argument("--theme", action="append", default=[])
    search.add_argument(
        "--document-type", action="append", default=[], choices=[item.value for item in KnowledgeDocumentType]
    )
    search.add_argument("--db")
    search.add_argument("--no-bootstrap", action="store_true")
    search.add_argument("--include-drafts", action="store_true")
    search.add_argument("--json", action="store_true")

    index = subparsers.add_parser("index", help="Migrate and synchronize canonical hybrid indexes")
    index_subparsers = index.add_subparsers(dest="index_command", required=True)
    migrate = index_subparsers.add_parser("migrate-markdown", help="Idempotently migrate data/docs Markdown")
    migrate.add_argument("--docs-dir")
    migrate.add_argument("--db")
    sync = index_subparsers.add_parser("sync", help="Consume pending jobs into lexical and vector indexes")
    sync.add_argument("--max-jobs", type=int, default=100_000)
    sync.add_argument("--no-reconcile", action="store_true")
    sync.add_argument("--db")
    status = index_subparsers.add_parser("status", help="Show hybrid index parity and outbox status")
    status.add_argument("--db")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    paths = Paths()
    if args.command == "search":
        return _search(args, paths)
    if args.command == "index":
        return _index(args, paths)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
