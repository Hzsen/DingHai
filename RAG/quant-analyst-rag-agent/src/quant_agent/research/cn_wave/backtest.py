from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from quant_agent.screening.wave import BENCHMARK, SCORE_VERSION


BACKTEST_VERSION = "wave-backtest-v1.1.0"


def _forward_open_returns(scored: pd.DataFrame, holding_days: int) -> pd.DataFrame:
    output = []
    for _, group in scored.groupby("ticker", sort=False):
        group = group.sort_values("date").copy()
        group["execution_date"] = group["date"].shift(-1)
        group["exit_date"] = group["date"].shift(-(holding_days + 1))
        group["entry_open"] = group["open"].shift(-1)
        group["exit_open"] = group["open"].shift(-(holding_days + 1))
        group["forward_open_return"] = group["exit_open"] / group["entry_open"] - 1
        group["next_day_untradable"] = group["locked_limit_up"].shift(-1).fillna(True) | group["volume"].shift(-1).fillna(0).le(0)
        output.append(group)
    return pd.concat(output, ignore_index=True)


def run_wave_backtest(
    scored: pd.DataFrame,
    *,
    top_n: int = 3,
    minimum_score: float = 55,
    transaction_cost_bps: float = 10,
    holding_days: int = 5,
    oos_start_date: str = "2025-01-01",
    signal_frequency: str = "weekly",
) -> tuple[pd.DataFrame, dict[str, float | int | str]]:
    """Run a daily t-close signal / t+1-open execution pilot backtest."""
    if holding_days < 1:
        raise ValueError("holding_days must be at least 1")
    forward = _forward_open_returns(scored, holding_days)
    benchmark = forward.loc[forward["ticker"] == BENCHMARK, ["date", "forward_open_return"]].rename(
        columns={"forward_open_return": "benchmark_return"}
    )
    candidates = forward.loc[
        (forward["ticker"] != BENCHMARK)
        & forward["eligible"]
        & (forward["wave_score"] >= minimum_score)
        & ~forward["next_day_untradable"]
        & forward["forward_open_return"].notna()
    ].copy()
    previous: set[str] = set()
    rows: list[dict[str, object]] = []
    benchmark_signals = forward.loc[forward["ticker"] == BENCHMARK].sort_values("date")
    if signal_frequency == "weekly":
        iso = benchmark_signals["date"].dt.isocalendar()
        benchmark_signals = benchmark_signals.assign(iso_year=iso.year, iso_week=iso.week)
        rebalance_dates = set(benchmark_signals.groupby(["iso_year", "iso_week"], sort=True).tail(1)["date"])
    elif signal_frequency == "every_holding_period":
        rebalance_dates = set(benchmark_signals.iloc[::holding_days]["date"])
    else:
        raise ValueError("unsupported signal_frequency")
    for signal_date, group in benchmark_signals.groupby("date"):
        if signal_date not in rebalance_dates:
            continue
        benchmark_row = group.iloc[0]
        day = candidates.loc[candidates["date"] == signal_date].nlargest(top_n, ["wave_score", "rs_market_20d"])
        holdings = set(day["ticker"].tolist())
        if holdings:
            denominator = max(len(holdings), len(previous), 1)
            turnover = 1.0 if not previous else 1.0 - len(holdings & previous) / denominator
            gross_return = float(day["forward_open_return"].mean())
            execution_date = day["execution_date"].iloc[0]
            exit_date = day["exit_date"].iloc[0]
        else:
            turnover = 1.0 if previous else 0.0
            gross_return = 0.0
            execution_date = benchmark_row["execution_date"]
            exit_date = benchmark_row["exit_date"]
        cost = turnover * 2 * transaction_cost_bps / 10_000
        rows.append(
            {
                "signal_date": signal_date,
                "execution_date": execution_date,
                "exit_date": exit_date,
                "holdings": "|".join(sorted(holdings)),
                "holding_count": len(holdings),
                "gross_return": gross_return,
                "turnover": turnover,
                "transaction_cost": cost,
                "net_return": gross_return - cost,
                "benchmark_return": benchmark_row["forward_open_return"],
            }
        )
        previous = holdings
    daily = pd.DataFrame(rows).dropna(subset=["execution_date", "exit_date", "benchmark_return"]).reset_index(drop=True)
    daily["equity"] = (1 + daily["net_return"]).cumprod()
    daily["benchmark_equity"] = (1 + daily["benchmark_return"]).cumprod()
    observations = len(daily)
    periods_per_year = 252 / holding_days
    years = observations / periods_per_year if observations else 0
    annual_return = float(daily["equity"].iloc[-1] ** (1 / years) - 1) if years > 0 else 0.0
    benchmark_annual = float(daily["benchmark_equity"].iloc[-1] ** (1 / years) - 1) if years > 0 else 0.0
    volatility = float(daily["net_return"].std(ddof=1) * math.sqrt(periods_per_year)) if observations > 1 else 0.0
    sharpe = float(daily["net_return"].mean() / daily["net_return"].std(ddof=1) * math.sqrt(periods_per_year)) if observations > 1 and daily["net_return"].std(ddof=1) > 0 else 0.0
    drawdown = daily["equity"] / daily["equity"].cummax() - 1
    active = daily.loc[daily["holding_count"] > 0]
    summary: dict[str, float | int | str] = {
        "strategy_name": "cn_wave_topn_weekly" if signal_frequency == "weekly" else "cn_wave_topn_periodic",
        "backtest_version": BACKTEST_VERSION,
        "score_version": SCORE_VERSION,
        "start_date": daily["signal_date"].min().date().isoformat(),
        "end_date": daily["signal_date"].max().date().isoformat(),
        "observations": observations,
        "active_days": len(active),
        "annual_return": annual_return,
        "benchmark_annual_return": benchmark_annual,
        "annual_excess_return": annual_return - benchmark_annual,
        "volatility": volatility,
        "sharpe": sharpe,
        "max_drawdown": float(drawdown.min()),
        "hit_rate": float((active["net_return"] > 0).mean()) if len(active) else 0.0,
        "average_turnover": float(daily["turnover"].mean()),
        "transaction_cost_bps": transaction_cost_bps,
        "top_n": top_n,
        "minimum_score": minimum_score,
        "holding_days": holding_days,
        "signal_frequency": signal_frequency,
    }
    oos = daily.loc[daily["signal_date"] >= pd.Timestamp(oos_start_date)].copy()
    if len(oos):
        oos_equity = (1 + oos["net_return"]).cumprod()
        oos_years = len(oos) / periods_per_year
        summary["oos_start_date"] = oos_start_date
        summary["oos_observations"] = len(oos)
        summary["oos_annual_return"] = float(oos_equity.iloc[-1] ** (1 / oos_years) - 1)
        summary["oos_max_drawdown"] = float((oos_equity / oos_equity.cummax() - 1).min())
    return daily, summary


def run_sensitivity(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for holding_days in (1, 5, 20):
        for minimum_score in (45.0, 55.0, 65.0):
            _, summary = run_wave_backtest(
                scored,
                top_n=3,
                minimum_score=minimum_score,
                transaction_cost_bps=10,
                holding_days=holding_days,
                signal_frequency="every_holding_period",
            )
            rows.append(summary)
    return pd.DataFrame(rows)


def publish_backtest(
    db_path: Path | str,
    daily: pd.DataFrame,
    summary: dict[str, float | int | str],
    output_dir: Path | str,
    sensitivity: pd.DataFrame | None = None,
) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gold_backtest_sensitivity (
            backtest_version TEXT NOT NULL,holding_days INTEGER NOT NULL,minimum_score REAL NOT NULL,
            metrics_json TEXT NOT NULL,PRIMARY KEY(backtest_version,holding_days,minimum_score))"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gold_backtest_daily (
            backtest_version TEXT NOT NULL,signal_date TEXT NOT NULL,execution_date TEXT NOT NULL,
            exit_date TEXT NOT NULL,holdings TEXT NOT NULL,holding_count INTEGER NOT NULL,
            gross_return REAL NOT NULL,turnover REAL NOT NULL,transaction_cost REAL NOT NULL,
            net_return REAL NOT NULL,benchmark_return REAL NOT NULL,equity REAL NOT NULL,
            benchmark_equity REAL NOT NULL,PRIMARY KEY(backtest_version,signal_date))"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gold_backtest_summary (
            backtest_version TEXT PRIMARY KEY,strategy_name TEXT NOT NULL,start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,metrics_json TEXT NOT NULL)"""
        )
        conn.execute("DELETE FROM gold_backtest_daily WHERE backtest_version=?", (BACKTEST_VERSION,))
        for row in daily.itertuples(index=False):
            conn.execute(
                "INSERT INTO gold_backtest_daily VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (BACKTEST_VERSION,row.signal_date.date().isoformat(),row.execution_date.date().isoformat(),row.exit_date.date().isoformat(),row.holdings,row.holding_count,row.gross_return,row.turnover,row.transaction_cost,row.net_return,row.benchmark_return,row.equity,row.benchmark_equity),
            )
        conn.execute(
            "INSERT OR REPLACE INTO gold_backtest_summary VALUES (?,?,?,?,?)",
            (BACKTEST_VERSION,summary["strategy_name"],summary["start_date"],summary["end_date"],json.dumps(summary,ensure_ascii=False,sort_keys=True)),
        )
        if sensitivity is not None:
            conn.execute("DELETE FROM gold_backtest_sensitivity WHERE backtest_version=?", (BACKTEST_VERSION,))
            for item in sensitivity.to_dict(orient="records"):
                conn.execute(
                    "INSERT INTO gold_backtest_sensitivity VALUES (?,?,?,?)",
                    (BACKTEST_VERSION,int(item["holding_days"]),float(item["minimum_score"]),json.dumps(item,ensure_ascii=False,sort_keys=True)),
                )
    report = output_dir / f"backtest_{BACKTEST_VERSION}.md"
    report.write_text(
        "\n".join(
            [
                "# A-share WaveScore Pilot Backtest",
                "",
                "> Research validation only. Pilot universe results are not an investment recommendation.",
                "",
                "## Protocol",
                "",
                "- Signal: trade-date close",
                "- Entry: next trading day open",
                "- Exit/rebalance: entry plus configured holding period, at open",
                f"- Top N: {summary['top_n']}",
                f"- Minimum WaveScore: {summary['minimum_score']}",
                f"- Holding period: {summary['holding_days']} trading days",
                f"- Signal frequency: {summary['signal_frequency']}",
                f"- One-way transaction cost: {summary['transaction_cost_bps']} bps",
                "",
                "## Results",
                "",
                f"- Period: {summary['start_date']} to {summary['end_date']}",
                f"- Active days: {summary['active_days']}",
                f"- Annual return: {summary['annual_return']:.2%}",
                f"- Benchmark annual return: {summary['benchmark_annual_return']:.2%}",
                f"- Sharpe: {summary['sharpe']:.2f}",
                f"- Max drawdown: {summary['max_drawdown']:.2%}",
                f"- Hit rate: {summary['hit_rate']:.2%}",
                f"- OOS start: {summary.get('oos_start_date', 'N/A')}",
                f"- OOS annual return: {summary.get('oos_annual_return', 0):.2%}",
                f"- OOS max drawdown: {summary.get('oos_max_drawdown', 0):.2%}",
                "",
                "## Limitations",
                "",
                "- Eight-stock ex-post case-study universe creates severe selection bias.",
                "- The OOS date split does not remove universe selection bias; returns are engineering diagnostics only.",
                "- Current-adjusted cached prices are not strict historical corporate-action vintages.",
                "- Limit-lock handling is conservative and based on daily bars.",
            ]
        ) + "\n",
        encoding="utf-8",
    )
    if sensitivity is not None:
        sensitivity.to_csv(output_dir / "backtest_sensitivity.csv", index=False, encoding="utf-8-sig")
    return report
