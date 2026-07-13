from __future__ import annotations

import argparse
import json

import pandas as pd

from quant_agent.config import Paths
from quant_agent.ingestion.phase0_market_data import fetch_csi300, fetch_phase0_prices
from quant_agent.research.cn_wave.features import build_daily_features
from quant_agent.research.cn_wave.scoring import score_daily_features


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Phase 0 A-share leader baseline.")
    parser.add_argument("--refresh-data", action="store_true", help="Fetch and replace cached real market data.")
    parser.add_argument("--start-date", default="2022-01-01", help="Warm-up start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", default=None, help="End date; defaults to the latest label end date.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = Paths()
    paths.ensure_phase0_dirs()
    labels = pd.read_csv(paths.leader_cases_path, dtype={"ticker": str})
    narratives = pd.read_csv(paths.theme_events_path, dtype={"ticker": str})
    end_date = args.end_date or str(labels["end_date"].max())

    cache_missing = not paths.phase0_market_prices_path.exists() or not paths.phase0_benchmark_path.exists()
    if args.refresh_data or cache_missing:
        prices = fetch_phase0_prices(labels, args.start_date, end_date)
        benchmark = fetch_csi300(args.start_date, end_date)
        prices.to_parquet(paths.phase0_market_prices_path, index=False)
        benchmark.to_parquet(paths.phase0_benchmark_path, index=False)
    else:
        prices = pd.read_parquet(paths.phase0_market_prices_path)
        benchmark = pd.read_parquet(paths.phase0_benchmark_path)

    features = build_daily_features(prices, benchmark, narratives, labels)
    features.to_parquet(paths.phase0_features_path, index=False)
    scored = score_daily_features(features)
    phase0 = scored.loc[scored["is_labeled_positive"]].copy()

    result_columns = [
        "date",
        "ticker",
        "stock_name",
        "label_leader_type",
        "label_theme",
        "leader_score",
        "stage_label",
        "score_coverage",
        "evaluated_rule_count",
        "top_features",
        "risk_flags",
        "missing_components",
        "event_id",
        "available_at",
        "feature_version",
        "score_version",
    ]
    phase0[result_columns].to_csv(paths.phase0_baseline_path, index=False, encoding="utf-8-sig")

    summary = {
        "market_rows": len(prices),
        "feature_rows": len(features),
        "labeled_result_rows": len(phase0),
        "tickers": sorted(prices["ticker"].unique().tolist()),
        "feature_date_min": str(features["date"].min().date()),
        "feature_date_max": str(features["date"].max().date()),
        "features_path": str(paths.phase0_features_path),
        "baseline_path": str(paths.phase0_baseline_path),
        "warning": "Full-market amount ranks and industry/theme relative strength remain missing in Phase 0.",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
