from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Sequence

from quant_agent.config import Paths
from quant_agent.knowledge.adapters import KnowledgeMigrationService, ThemeRotationKnowledgeAdapter
from quant_agent.knowledge.store import KnowledgeStore
from quant_agent.theme_rotation.catalog import load_theme_rotation_catalog


def add_arguments(parser: argparse.ArgumentParser) -> None:
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--prices", help="CSV/Parquet with date,symbol,close,volume")
    source.add_argument("--live", action="store_true", help="Fetch configured US symbols through AkShare")
    parser.add_argument("--as-of", help="ISO datetime/date; file mode defaults to the last benchmark date")
    parser.add_argument("--catalog", default="configs/theme_rotation_tech_v1.json")
    parser.add_argument("--db", default="data/processed/phase1_research.db")
    parser.add_argument("--output-dir", default="outputs/theme-rotation")
    parser.add_argument("--selected-theme", default="semiconductor")
    parser.add_argument("--live-lookback-days", type=int, default=550)
    parser.add_argument("--no-knowledge", action="store_true", help="Do not publish the snapshot into KnowledgeStore")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the deterministic technology-theme rotation monitor.")
    add_arguments(parser)
    return parser


def _resolve(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _parse_as_of(value: str | None) -> datetime | None:
    if value is None:
        return None
    if len(value) == 10:
        return datetime.combine(datetime.fromisoformat(value).date(), time(23, 59), tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def execute(args: argparse.Namespace, paths: Paths | None = None) -> int:
    from quant_agent.theme_rotation.data import fetch_live_theme_prices, load_theme_price_file
    from quant_agent.theme_rotation.engine import evaluate_theme_rotation
    from quant_agent.theme_rotation.report import publish_theme_rotation_outputs
    from quant_agent.theme_rotation.repository import publish_theme_rotation

    paths = paths or Paths()
    root = paths.project_root
    catalog_path = _resolve(root, args.catalog)
    config, definitions = load_theme_rotation_catalog(catalog_path)
    parsed_as_of = _parse_as_of(args.as_of)
    symbols = {config.benchmark_symbol} | {symbol for item in definitions for symbol in item.members}
    source_errors: list[dict[str, str]] = []
    if args.live:
        live_as_of = (parsed_as_of or datetime.now(timezone.utc)).date()
        prices, source_errors = fetch_live_theme_prices(
            symbols, live_as_of, lookback_days=args.live_lookback_days
        )
        source_label = "akshare.stock_us_daily"
    else:
        price_path = _resolve(root, args.prices)
        prices = load_theme_price_file(price_path)
        source_label = f"file:{price_path.relative_to(root) if price_path.is_relative_to(root) else price_path}"
    evaluation = evaluate_theme_rotation(
        prices, definitions, config, as_of=parsed_as_of, selected_theme_id=args.selected_theme
    )
    db_path = _resolve(root, args.db)
    output_dir = _resolve(root, args.output_dir)
    run_id = publish_theme_rotation(
        db_path, evaluation, prices, source=source_label, source_errors=source_errors
    )
    output_paths = publish_theme_rotation_outputs(output_dir, evaluation)
    migration = None
    if not args.no_knowledge:
        migration = KnowledgeMigrationService(KnowledgeStore(db_path)).migrate(
            ThemeRotationKnowledgeAdapter(db_path)
        )
    print(json.dumps({
        "run_id": run_id,
        "snapshot_id": evaluation.snapshot.snapshot_id,
        "as_of": evaluation.snapshot.as_of.isoformat(),
        "benchmark": evaluation.snapshot.benchmark_symbol,
        "theme_count": len(evaluation.snapshot.themes),
        "attribution_count": len(evaluation.snapshot.attributions),
        "alert_count": len(evaluation.snapshot.alerts),
        "data_coverage": evaluation.snapshot.data_coverage,
        "quality_flags": list(evaluation.snapshot.quality_flags),
        "source_errors": source_errors,
        "outputs": {key: str(value) for key, value in output_paths.items()},
        "database": str(db_path),
        "knowledge_migration": asdict(migration) if migration else None,
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return execute(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
