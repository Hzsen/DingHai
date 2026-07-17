from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from quant_agent.research.cn_wave.features import board_limit_pct


FEATURE_VERSION = "wave-features-v1.0.0"
SCORE_VERSION = "wave-score-v1.0.0"
BENCHMARK = "000300.SH"


def load_gold_prices(db_path: Path | str) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        frame = pd.read_sql_query("SELECT * FROM gold_cn_prices ORDER BY trade_date,ticker", conn)
    frame["date"] = pd.to_datetime(frame.pop("trade_date"))
    return frame


def _one_security(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values("date").copy()
    close = group["close"]
    group["history_days"] = np.arange(1, len(group) + 1)
    for days in (1, 5, 20, 60):
        group[f"return_{days}d"] = close.pct_change(days, fill_method=None)
    for days in (20, 60, 120):
        group[f"ma_{days}d"] = close.rolling(days, min_periods=days).mean()
    group["ma20_slope_5d"] = group["ma_20d"] / group["ma_20d"].shift(5) - 1
    group["momentum_acceleration"] = group["return_5d"] - group["return_20d"] / 4
    for days in (60, 120, 250):
        group[f"rolling_high_{days}d"] = close.rolling(days, min_periods=days).max()
        group[f"distance_to_{days}d_high"] = close / group[f"rolling_high_{days}d"] - 1
    at_high = close >= group["rolling_high_120d"] * 0.999
    group["new_high_count_20d"] = at_high.astype(float).rolling(20, min_periods=1).sum()
    group["amount_ratio_20d"] = group["amount"] / group["amount"].shift(1).rolling(20, min_periods=20).mean()
    group["turnover_20d_sum"] = group["turnover_rate"].rolling(20, min_periods=20).sum()
    previous_close = close.shift(1)
    true_range = pd.concat(
        [group["high"] - group["low"], (group["high"] - previous_close).abs(), (group["low"] - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    group["atr_14d_pct"] = true_range.rolling(14, min_periods=14).mean() / close
    group["drawdown_from_120d_high"] = close / group["rolling_high_120d"] - 1
    group["upper_shadow_ratio"] = (group["high"] - group[["open", "close"]].max(axis=1)).clip(lower=0) / close
    group["high_volume_stall_flag"] = (
        (group["amount_ratio_20d"] >= 2) & (group["return_1d"] <= 0.03) & (group["upper_shadow_ratio"] >= 0.03)
    )
    group["limit_ratio"] = board_limit_pct(str(group.iloc[0]["ticker"]))
    group["locked_limit_up"] = (
        (group["return_1d"] >= group["limit_ratio"] - 0.005)
        & np.isclose(group["high"], group["low"], rtol=0, atol=1e-8)
    )
    return group


def build_wave_features(prices: pd.DataFrame) -> pd.DataFrame:
    required = {"ticker", "date", "open", "high", "low", "close", "volume", "amount", "turnover_rate"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"prices missing columns: {sorted(missing)}")
    prices = prices.copy()
    prices["date"] = pd.to_datetime(prices["date"])
    features = pd.concat([_one_security(group) for _, group in prices.groupby("ticker", sort=False)], ignore_index=True)
    benchmark = features.loc[features["ticker"] == BENCHMARK, ["date", "return_20d"]].rename(
        columns={"return_20d": "benchmark_return_20d"}
    )
    features = features.merge(benchmark, on="date", how="left", validate="many_to_one")
    features["rs_market_20d"] = features["return_20d"] - features["benchmark_return_20d"]
    stock_mask = features["ticker"] != BENCHMARK
    features["amount_rank_pilot"] = np.nan
    features["rs_rank_pilot_pct"] = np.nan
    features.loc[stock_mask, "amount_rank_pilot"] = features.loc[stock_mask].groupby("date")["amount"].rank(
        method="min", ascending=False
    )
    features.loc[stock_mask, "rs_rank_pilot_pct"] = features.loc[stock_mask].groupby("date")["rs_market_20d"].rank(
        method="average", ascending=False, pct=True
    )
    features["feature_version"] = FEATURE_VERSION
    return features.sort_values(["date", "ticker"]).reset_index(drop=True)


def _score(row: pd.Series) -> dict[str, object]:
    exclusions: list[str] = []
    if row["ticker"] == BENCHMARK:
        exclusions.append("benchmark")
    if int(row["history_days"]) < 120:
        exclusions.append("insufficient_history")
    if float(row["volume"]) <= 0:
        exclusions.append("no_volume")
    if bool(row["locked_limit_up"]):
        exclusions.append("locked_limit_up")
    if pd.isna(row["ma_60d"]) or pd.isna(row["rs_market_20d"]):
        exclusions.append("missing_core_features")

    reasons: list[str] = []
    risks: list[str] = []
    trend = 0
    if row["close"] > row["ma_20d"]:
        trend += 8; reasons.append("close_above_ma20")
    if row["ma_20d"] > row["ma_60d"]:
        trend += 8; reasons.append("ma20_above_ma60")
    if row["ma20_slope_5d"] > 0:
        trend += 9; reasons.append("ma20_rising")

    breakout = 0
    if row["distance_to_120d_high"] >= -0.03:
        breakout += 10; reasons.append("near_120d_high")
    if row["new_high_count_20d"] >= 2:
        breakout += 10; reasons.append("repeated_new_highs")

    momentum = 0
    if row["return_20d"] > 0:
        momentum += 5; reasons.append("positive_20d_momentum")
    if row["momentum_acceleration"] > 0:
        momentum += 5; reasons.append("momentum_accelerating")

    relative = 0
    if row["rs_market_20d"] > 0:
        relative += 10; reasons.append("outperforming_csi300")
    if row["rs_rank_pilot_pct"] <= 0.20:
        relative += 10; reasons.append("top20pct_pilot_rs")

    volume = 0
    if row["amount_ratio_20d"] >= 1.5:
        volume += 8; reasons.append("amount_expansion")
    if row["turnover_20d_sum"] >= 0.5:
        volume += 7; reasons.append("sufficient_turnover")

    risk_quality = 0
    if row["drawdown_from_120d_high"] >= -0.15:
        risk_quality += 5
    if row["atr_14d_pct"] <= 0.08:
        risk_quality += 5
    penalty = 0
    if bool(row["high_volume_stall_flag"]):
        penalty += 10; risks.append("high_volume_stall")
    if row["close"] / row["ma_20d"] - 1 > 0.30:
        penalty += 5; risks.append("extended_above_ma20")
    total = max(0, min(100, trend + breakout + momentum + relative + volume + risk_quality - penalty))
    return {
        "eligible": not exclusions,
        "exclusion_reasons": "|".join(exclusions),
        "trend_score": trend,
        "breakout_score": breakout,
        "momentum_score": momentum,
        "relative_strength_score": relative,
        "volume_score": volume,
        "risk_quality_score": risk_quality,
        "risk_penalty": penalty,
        "wave_score": total,
        "top_reasons": "|".join(reasons),
        "risk_flags": "|".join(risks),
        "score_version": SCORE_VERSION,
    }


def score_wave_features(features: pd.DataFrame) -> pd.DataFrame:
    scored = features.copy()
    score_columns = scored.apply(_score, axis=1, result_type="expand")
    for column in score_columns:
        scored[column] = score_columns[column]
    return scored


def screen_as_of(scored: pd.DataFrame, as_of: str | pd.Timestamp, top_n: int = 5) -> pd.DataFrame:
    as_of = pd.Timestamp(as_of)
    snapshot = scored.loc[scored["date"] <= as_of].sort_values("date").groupby("ticker", as_index=False).tail(1)
    snapshot = snapshot.loc[snapshot["date"] == as_of].copy()
    snapshot["rank"] = snapshot.loc[snapshot["eligible"], "wave_score"].rank(method="first", ascending=False)
    snapshot["selected"] = snapshot["eligible"] & snapshot["rank"].le(top_n)
    return snapshot.sort_values(["selected", "wave_score"], ascending=[False, False]).reset_index(drop=True)


def publish_screen(
    db_path: Path | str,
    scored: pd.DataFrame,
    snapshot: pd.DataFrame,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS security_master (
            ticker TEXT PRIMARY KEY,name TEXT NOT NULL,exchange TEXT NOT NULL,board TEXT NOT NULL,
            valid_from TEXT NOT NULL,valid_to TEXT,status_source TEXT NOT NULL)"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gold_cn_tradability (
            trade_date TEXT NOT NULL,ticker TEXT NOT NULL,is_listed INTEGER NOT NULL,
            is_suspended INTEGER NOT NULL,is_st INTEGER,status_known INTEGER NOT NULL,
            eligible INTEGER NOT NULL,exclusion_reasons TEXT NOT NULL,limit_ratio REAL NOT NULL,
            quality_note TEXT NOT NULL,source_run_id TEXT NOT NULL,
            PRIMARY KEY(trade_date,ticker))"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gold_cn_features (
            trade_date TEXT NOT NULL,ticker TEXT NOT NULL,feature_version TEXT NOT NULL,
            feature_json TEXT NOT NULL,source_run_id TEXT NOT NULL,
            PRIMARY KEY(trade_date,ticker,feature_version))"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gold_cn_screen_results (
            as_of TEXT NOT NULL,ticker TEXT NOT NULL,name TEXT NOT NULL,eligible INTEGER NOT NULL,
            selected INTEGER NOT NULL,rank REAL,wave_score REAL NOT NULL,component_json TEXT NOT NULL,
            exclusion_reasons TEXT NOT NULL,top_reasons TEXT NOT NULL,risk_flags TEXT NOT NULL,
            feature_version TEXT NOT NULL,score_version TEXT NOT NULL,source_run_id TEXT NOT NULL,
            universe_scope TEXT NOT NULL,PRIMARY KEY(as_of,ticker,score_version))"""
        )
        for ticker, group in scored.groupby("ticker"):
            first = group.sort_values("date").iloc[0]
            exchange = ticker.split(".")[-1]
            board = "benchmark" if ticker == BENCHMARK else ("chinext" if ticker.startswith(("300", "301")) else "star" if ticker.startswith("688") else "main")
            conn.execute(
                "INSERT OR REPLACE INTO security_master VALUES (?,?,?,?,?,?,?)",
                (ticker, first["name"], exchange, board, first["date"].date().isoformat(), None, "pilot_observed_range"),
            )
        feature_fields = [
            "close", "ma_20d", "ma_60d", "ma20_slope_5d", "return_5d", "return_20d",
            "momentum_acceleration", "distance_to_120d_high", "distance_to_250d_high",
            "new_high_count_20d", "amount_ratio_20d", "turnover_20d_sum", "atr_14d_pct",
            "drawdown_from_120d_high", "rs_market_20d", "amount_rank_pilot", "rs_rank_pilot_pct",
            "wave_score", "eligible", "exclusion_reasons", "top_reasons", "risk_flags",
        ]
        conn.execute("DELETE FROM gold_cn_features WHERE feature_version=?", (FEATURE_VERSION,))
        conn.execute("DELETE FROM gold_cn_tradability")
        for row in scored.itertuples(index=False):
            payload = {}
            for field in feature_fields:
                value = getattr(row, field)
                payload[field] = None if pd.isna(value) else (bool(value) if isinstance(value, (bool, np.bool_)) else float(value) if isinstance(value, (float, np.floating)) else int(value) if isinstance(value, (int, np.integer)) else value)
            conn.execute(
                "INSERT INTO gold_cn_features VALUES (?,?,?,?,?)",
                (row.date.date().isoformat(), row.ticker, FEATURE_VERSION, json.dumps(payload, ensure_ascii=False, sort_keys=True), row.source_run_id),
            )
            conn.execute(
                "INSERT INTO gold_cn_tradability VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (row.date.date().isoformat(),row.ticker,1,int(float(row.volume)<=0),None,0,int(row.eligible),row.exclusion_reasons,float(row.limit_ratio),"historical ST status unavailable in pilot",row.source_run_id),
            )
        as_of = snapshot["date"].iloc[0].date().isoformat()
        conn.execute("DELETE FROM gold_cn_screen_results WHERE as_of=? AND score_version=?", (as_of, SCORE_VERSION))
        for row in snapshot.itertuples(index=False):
            components = {name: getattr(row, name) for name in ("trend_score", "breakout_score", "momentum_score", "relative_strength_score", "volume_score", "risk_quality_score", "risk_penalty")}
            conn.execute(
                "INSERT INTO gold_cn_screen_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (as_of,row.ticker,row.name,int(row.eligible),int(row.selected),None if pd.isna(row.rank) else float(row.rank),float(row.wave_score),json.dumps(components,ensure_ascii=False),row.exclusion_reasons,row.top_reasons,row.risk_flags,row.feature_version,row.score_version,row.source_run_id,"phase2_pilot_8_stocks"),
            )
    columns = ["date","rank","ticker","name","selected","eligible","wave_score","trend_score","breakout_score","momentum_score","relative_strength_score","volume_score","risk_quality_score","risk_penalty","top_reasons","risk_flags","exclusion_reasons","feature_version","score_version","source_run_id"]
    csv_path = output_dir / f"cn_wave_screen_{as_of}.csv"
    snapshot[columns].to_csv(csv_path, index=False, encoding="utf-8-sig")
    selected = snapshot.loc[snapshot["selected"]]
    lines = [f"# A-share Wave Screen — {as_of}", "", "> Pilot universe: 8 research stocks; cross-sectional ranks are not full-market ranks.", "", "| Rank | Ticker | Name | WaveScore | Reasons | Risks |", "|---:|---|---|---:|---|---|"]
    for row in selected.itertuples(index=False):
        lines.append(f"| {int(row.rank)} | {row.ticker} | {row.name} | {row.wave_score:.0f} | {row.top_reasons} | {row.risk_flags or '-'} |")
    md_path = output_dir / f"cn_wave_screen_{as_of}.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path, md_path
