from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from quant_agent.config import Paths
from quant_agent.data_sources.base import DataRequest
from quant_agent.data_sources.fred import FredCsvSource
from quant_agent.data_sources.pilot_market import PilotParquetMarketSource
from quant_agent.pipeline.warehouse import PhaseWarehouse
from quant_agent.research.cn_wave.backtest import publish_backtest, run_wave_backtest
from quant_agent.research.weekly_documents import build_weekly_documents, publish_weekly_documents, weekly_incremental_start
from quant_agent.screening.wave import build_wave_features, load_gold_prices, publish_screen, score_wave_features, screen_as_of


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Phase 1-3 pilot vertical slice.")
    parser.add_argument("--db", default="data/processed/phase1_research.db")
    parser.add_argument("--start-date", default="2022-01-01")
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--macro-start-date", default="2025-01-01")
    parser.add_argument("--skip-macro", action="store_true")
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--minimum-score", type=float, default=55)
    parser.add_argument("--transaction-cost-bps", type=float, default=10)
    parser.add_argument("--holding-days", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = Paths()
    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = paths.project_root / db_path
    warehouse = PhaseWarehouse(db_path)
    labels = pd.read_csv(paths.leader_cases_path, dtype={"ticker": str})
    symbols = tuple(sorted(set(labels["ticker"]).union({"000300.SH"})))
    as_of = date.fromisoformat(args.as_of) if args.as_of else pd.read_parquet(paths.phase0_market_prices_path)["date"].max().date()
    market_watermark = warehouse.watermark("cn_daily")
    market_start = max(
        date.fromisoformat(args.start_date),
        date.fromisoformat(market_watermark) - timedelta(days=7) if market_watermark else date.fromisoformat(args.start_date),
    )
    market_request = DataRequest("cn_daily", symbols, market_start, as_of, incremental=True)
    market_source = PilotParquetMarketSource(paths.phase0_market_prices_path, paths.phase0_benchmark_path)
    warehouse.ingest_batch(market_source.fetch(market_request))

    if not args.skip_macro:
        macro_request = DataRequest(
            "us_liquidity",
            ("WALCL", "WTREGEN", "RRPONTSYD"),
            date.fromisoformat(args.macro_start_date),
            as_of,
            incremental=True,
        )
        warehouse.ingest_batch(FredCsvSource().fetch(macro_request))

    prices = load_gold_prices(db_path)
    features = build_wave_features(prices)
    scored = score_wave_features(features)
    snapshot = screen_as_of(scored, as_of, top_n=args.top_n)
    phase2_dir = paths.project_root / "outputs" / "phase2"
    screen_csv, screen_md = publish_screen(db_path, scored, snapshot, phase2_dir)
    daily, summary = run_wave_backtest(
        scored,
        top_n=args.top_n,
        minimum_score=args.minimum_score,
        transaction_cost_bps=args.transaction_cost_bps,
        holding_days=args.holding_days,
    )
    from quant_agent.research.cn_wave.backtest import run_sensitivity
    sensitivity = run_sensitivity(scored)
    backtest_report = publish_backtest(db_path, daily, summary, paths.project_root / "outputs" / "phase3", sensitivity)
    weekly_start = weekly_incremental_start(db_path, as_of)
    weekly_documents, weekly_chunks = build_weekly_documents(scored, as_of, start_week=weekly_start)
    weekly_stats = publish_weekly_documents(
        db_path,
        weekly_documents,
        weekly_chunks,
        paths.project_root / "outputs" / "weekly",
    )
    output = {
        "db_path": str(db_path),
        "bronze_rows": warehouse.table_count("bronze_records"),
        "silver_cn_rows": warehouse.table_count("silver_cn_daily"),
        "silver_macro_rows": warehouse.table_count("silver_macro_observations"),
        "gold_cn_rows": warehouse.table_count("gold_cn_prices"),
        "gold_macro_rows": warehouse.table_count("gold_macro_observations"),
        "as_of": as_of.isoformat(),
        "eligible_count": int(snapshot["eligible"].sum()),
        "selected_count": int(snapshot["selected"].sum()),
        "market_request_start": market_start.isoformat(),
        "screen_csv": str(screen_csv),
        "screen_report": str(screen_md),
        "backtest_report": str(backtest_report),
        "backtest_summary": summary,
        "weekly_documents": weekly_stats,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
