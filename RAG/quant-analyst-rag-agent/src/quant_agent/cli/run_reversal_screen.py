from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd

from quant_agent.config import Paths
from quant_agent.data_sources.sina_market import fetch_sina_histories, normalize_sina_spot, prefilter_repair_universe
from quant_agent.screening.reversal import (
    REVERSAL_FEATURE_VERSION,
    REVERSAL_SCORE_VERSION,
    build_reversal_features,
    classify_market_repair,
    publish_reversal_screen,
    score_reversal_features,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Screen A-share leaders repairing after a market selloff.")
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--max-symbols", type=int, default=500)
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--from-cache", action="store_true")
    parser.add_argument("--reuse-spot-cache", action="store_true")
    parser.add_argument("--db", default="data/processed/phase1_research.db")
    return parser.parse_args()


def _fetch_benchmark(as_of: date) -> pd.DataFrame:
    import akshare as ak

    history = ak.stock_zh_index_daily(symbol="sh000300").copy()
    history["date"] = pd.to_datetime(history["date"])
    history = history.loc[history["date"].dt.date <= as_of, ["date", "open", "high", "low", "close", "volume"]]
    if history["date"].max().date() < as_of:
        spot = ak.stock_zh_index_spot_sina()
        row = spot.loc[spot["代码"] == "sh000300"].iloc[0]
        history = pd.concat(
            [history, pd.DataFrame([{
                "date": pd.Timestamp(as_of), "open": float(row["今开"]), "high": float(row["最高"]),
                "low": float(row["最低"]), "close": float(row["最新价"]), "volume": float(row["成交量"]),
            }])], ignore_index=True,
        )
    return history.sort_values("date").drop_duplicates("date", keep="last")


def _render_report(scored: pd.DataFrame, regime: dict[str, object], metadata: dict[str, object], path: Path, top_n: int) -> None:
    selected = scored.loc[scored["focus_selected"]].head(top_n)
    watchlist = scored.loc[
        scored["eligible"] & ~scored["focus_selected"]
        & scored["stage"].isin(["LEADER_REPAIR_CONFIRMED", "REPAIR_CANDIDATE"])
    ].head(15)
    lines = [
        f"# A-share Selloff Repair Screen — {metadata['as_of']}", "",
        "> Research screen only. The historical universe was prefiltered from the full Sina snapshot; this is not yet a full-universe backtest.", "",
        "## Market Regime", "",
        f"- Regime: `{regime['regime']}`",
        f"- Prior 3-day return: {float(regime['prior_return_3d']):.2%}",
        f"- Prior 5-day drawdown: {float(regime['prior_drawdown_5d']):.2%}",
        f"- Repair-day return: {float(regime['recovery_return_1d']):.2%}",
        f"- Intraday close location: {float(regime['intraday_close_location']):.1%}", "",
        "## Data Quality", "",
        f"- Full snapshot rows: {metadata['snapshot_rows']}",
        f"- Historical prefilter rows: {metadata['prefilter_rows']}",
        f"- Successful histories: {metadata['history_successes']}",
        f"- History coverage: {float(metadata['history_coverage']):.1%}",
        f"- Feature coverage: {float(metadata['feature_coverage']):.1%}",
        f"- Sources: {metadata['sources']}", "",
        "## Focus Candidates", "",
        "> Focus requires prior leader score >= 80, market amount rank <= 300, positive 20-day RS, within 15% of the 120-day high, and no risk flag.", "",
        "| Rank | Ticker | Name | Stage | Score | Prior leader | Selloff RS | Today | vs market | Volume ratio | Amount rank | Reasons | Risks |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for rank, row in enumerate(selected.itertuples(index=False), start=1):
        lines.append(
            f"| {rank} | {row.ticker} | {row.name} | {row.stage} | {row.reversal_score:.0f} | "
            f"{row.prior_leader_score_20d:.0f} | {row.selloff_resilience_3d:.2%} | {row.return_1d:.2%} | "
            f"{row.recovery_vs_market_1d:.2%} | {row.volume_ratio_20d:.2f} | {row.amount_rank_market:.0f} | "
            f"{row.top_reasons} | {row.risk_flags or '-'} |"
        )
    lines.extend(["", "## Broader Repair Watchlist", "", "| Rank | Ticker | Name | Stage | Score | Amount rank | Risks |", "|---:|---|---|---|---:|---:|---|"])
    for rank, row in enumerate(watchlist.itertuples(index=False), start=1):
        lines.append(f"| {rank} | {row.ticker} | {row.name} | {row.stage} | {row.reversal_score:.0f} | {row.amount_rank_market:.0f} | {row.risk_flags or '-'} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    as_of = date.fromisoformat(args.as_of)
    root = Paths().project_root
    raw_dir = root / "data" / "raw" / "sina"
    interim_dir = root / "data" / "interim" / "reversal"
    output_dir = root / "outputs" / "reversal"
    for directory in (raw_dir, interim_dir, output_dir):
        directory.mkdir(parents=True, exist_ok=True)
    spot_path = raw_dir / f"stock_spot_{as_of.isoformat()}.parquet"
    history_path = interim_dir / f"repair_histories_{as_of.isoformat()}.parquet"
    benchmark_path = interim_dir / f"csi300_{as_of.isoformat()}.parquet"
    errors_path = output_dir / f"history_errors_{as_of.isoformat()}.json"

    if args.from_cache:
        if not all(path.exists() for path in (spot_path, history_path, benchmark_path)):
            raise FileNotFoundError("--from-cache requires spot, history and benchmark cache files")
        spot = pd.read_parquet(spot_path)
        histories = pd.read_parquet(history_path)
        benchmark = pd.read_parquet(benchmark_path)
        errors = json.loads(errors_path.read_text(encoding="utf-8")) if errors_path.exists() else []
        prefilter_count = int(histories["ticker"].nunique() + len(errors))
    else:
        import akshare as ak

        spot = pd.read_parquet(spot_path) if args.reuse_spot_cache and spot_path.exists() else normalize_sina_spot(ak.stock_zh_a_spot(), as_of)
        prefilter = prefilter_repair_universe(spot, max_symbols=args.max_symbols)
        cached = pd.read_parquet(history_path) if history_path.exists() else pd.DataFrame()
        cached_symbols = set(cached["ticker"].unique()) if not cached.empty else set()
        missing = prefilter.loc[~prefilter["ticker"].isin(cached_symbols)]
        fetched, errors = fetch_sina_histories(missing, as_of, workers=args.workers) if not missing.empty else (pd.DataFrame(), [])
        histories = pd.concat([cached, fetched], ignore_index=True, sort=False) if not cached.empty else fetched
        histories = histories.loc[histories["ticker"].isin(prefilter["ticker"])].copy()
        benchmark = pd.read_parquet(benchmark_path) if args.reuse_spot_cache and benchmark_path.exists() else _fetch_benchmark(as_of)
        prefilter_count = len(prefilter)
        spot.to_parquet(spot_path, index=False)
        histories.to_parquet(history_path, index=False)
        benchmark.to_parquet(benchmark_path, index=False)
        errors_path.write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")

    success_count = int(histories["ticker"].nunique()) if not histories.empty else 0
    coverage = success_count / prefilter_count if prefilter_count else 0.0
    if coverage < 0.70:
        raise RuntimeError(f"history coverage {coverage:.1%} below required 70%; no report published")
    regime = classify_market_repair(benchmark)
    features = build_reversal_features(histories, benchmark, as_of)
    scored = score_reversal_features(features)
    scored["market_regime"] = regime.regime
    scored["universe_scope"] = f"sina_spot_prefilter_{prefilter_count}"
    csv_path = output_dir / f"reversal_screen_{as_of.isoformat()}.csv"
    report_path = output_dir / f"reversal_screen_{as_of.isoformat()}.md"
    scored.to_csv(csv_path, index=False, encoding="utf-8-sig")
    metadata = {
        "as_of": as_of.isoformat(), "snapshot_rows": len(spot), "prefilter_rows": prefilter_count,
        "history_successes": success_count, "history_errors": len(errors), "history_coverage": coverage,
        "feature_rows": len(features), "feature_coverage": len(features) / prefilter_count if prefilter_count else 0.0,
        "leader_repair_count": int((scored["stage"] == "LEADER_REPAIR_CONFIRMED").sum()),
        "repair_candidate_count": int((scored["stage"] == "REPAIR_CANDIDATE").sum()),
        "focus_candidate_count": int(scored["focus_selected"].sum()),
        "feature_version": REVERSAL_FEATURE_VERSION, "score_version": REVERSAL_SCORE_VERSION,
        "sources": "Sina all-A snapshot/amount rank + Tencent qfq OHLCV + CSI300 Sina index",
        "csv_path": str(csv_path), "report_path": str(report_path),
    }
    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = root / db_path
    metadata["db_path"] = str(db_path)
    publish_reversal_screen(db_path, scored, regime, metadata)
    _render_report(scored, regime.to_dict(), metadata, report_path, args.top_n)
    print(json.dumps({"market_regime": regime.to_dict(), **metadata}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
