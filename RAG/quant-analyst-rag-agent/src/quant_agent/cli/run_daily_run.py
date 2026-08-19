"""``quant-agent daily-run`` command: unified daily research dashboard."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, time, timezone
from pathlib import Path

from domain.daily_run import DailyRunMode, ModuleStatus
from quant_agent.config import Paths
from quant_agent.daily_run.serialization import jsonable
from quant_agent.daily_run.service import run_daily


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


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--mode",
        choices=[item.value for item in DailyRunMode],
        default=DailyRunMode.LIVE.value,
        help="fixture = deterministic synthetic snapshot; live = aggregate real published outputs",
    )
    parser.add_argument("--as-of", help="ISO datetime with timezone, or YYYY-MM-DD")
    parser.add_argument("--db", help="SQLite database path (snapshots are persisted here)")
    parser.add_argument("--output-dir", help="dashboard output directory (default: outputs/daily)")


def execute(args: argparse.Namespace, paths: Paths | None = None) -> int:
    paths = paths or Paths()
    mode = DailyRunMode(args.mode)
    as_of = _parse_as_of(args.as_of)
    db_path = Path(args.db).expanduser() if args.db else paths.knowledge_db_path
    output_dir = Path(args.output_dir).expanduser() if args.output_dir else paths.project_root / "outputs" / "daily"
    result = run_daily(mode=mode, as_of=as_of, db_path=db_path, output_dir=output_dir, paths=paths)
    snapshot = result.snapshot
    print(
        json.dumps(
            jsonable(
                {
                    "run_id": snapshot.run_id,
                    "mode": snapshot.mode.value,
                    "synthetic": snapshot.synthetic,
                    "overall_status": snapshot.overall_status.value,
                    "module_statuses": {module.module_id: module.status.value for module in snapshot.modules},
                    "attention_items": len(snapshot.attention),
                    "outputs": result.outputs,
                }
            ),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if snapshot.overall_status is ModuleStatus.FAILED and mode is DailyRunMode.LIVE else 0
