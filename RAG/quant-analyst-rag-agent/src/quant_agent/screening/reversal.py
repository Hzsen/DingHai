from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd


REVERSAL_FEATURE_VERSION = "reversal-features-v1.0.0"
REVERSAL_SCORE_VERSION = "reversal-score-v1.0.0"


@dataclass(frozen=True)
class MarketRepairRegime:
    as_of: str
    regime: str
    prior_return_3d: float
    prior_drawdown_5d: float
    recovery_return_1d: float
    intraday_close_location: float

    def to_dict(self) -> dict[str, str | float]:
        return asdict(self)


def classify_market_repair(benchmark: pd.DataFrame) -> MarketRepairRegime:
    benchmark = benchmark.sort_values("date").copy()
    if len(benchmark) < 7:
        raise ValueError("benchmark needs at least 7 observations")
    previous = benchmark.iloc[:-1]
    today = benchmark.iloc[-1]
    prior_return_3d = float(previous.iloc[-1]["close"] / previous.iloc[-4]["close"] - 1)
    prior_drawdown_5d = float(previous.iloc[-1]["close"] / previous.tail(5)["close"].max() - 1)
    recovery_return = float(today["close"] / previous.iloc[-1]["close"] - 1)
    spread = float(today["high"] - today["low"])
    close_location = float((today["close"] - today["low"]) / spread) if spread > 0 else 0.5
    had_selloff = prior_return_3d <= -0.02 or prior_drawdown_5d <= -0.03
    repaired = recovery_return >= 0.008 and close_location >= 0.65
    regime = "SELLOFF_REPAIR" if had_selloff and repaired else "NO_CONFIRMED_REPAIR"
    return MarketRepairRegime(
        as_of=pd.Timestamp(today["date"]).date().isoformat(), regime=regime,
        prior_return_3d=prior_return_3d, prior_drawdown_5d=prior_drawdown_5d,
        recovery_return_1d=recovery_return, intraday_close_location=close_location,
    )


def _leader_signature(group: pd.DataFrame, benchmark_returns: pd.Series) -> pd.Series:
    close = group["close"]
    ma20 = close.rolling(20, min_periods=20).mean()
    ma60 = close.rolling(60, min_periods=60).mean()
    high120 = close.rolling(120, min_periods=120).max()
    return20 = close.pct_change(20, fill_method=None)
    rs20 = return20 - benchmark_returns.reindex(group["date"]).to_numpy()
    activity_ratio = group["volume"] / group["volume"].shift(1).rolling(20, min_periods=20).mean()
    at_high = close >= high120 * 0.999
    new_highs = at_high.astype(float).rolling(20, min_periods=1).sum()
    score = (
        (close > ma20).astype(int) * 10 + (ma20 > ma60).astype(int) * 10
        + (ma20 / ma20.shift(5) - 1 > 0).astype(int) * 5
        + (close / high120 - 1 >= -0.03).astype(int) * 10 + (new_highs >= 2).astype(int) * 10
        + (return20 > 0).astype(int) * 10 + (rs20 > 0).astype(int) * 15
        + (activity_ratio >= 1.2).astype(int) * 10
        + (group["volume"].rolling(20, min_periods=20).sum() > 0).astype(int) * 10
        + (close / ma20 - 1 <= 0.25).astype(int) * 10
    )
    return score.clip(0, 100)


def build_reversal_features(
    histories: pd.DataFrame,
    benchmark: pd.DataFrame,
    as_of: str | pd.Timestamp,
) -> pd.DataFrame:
    required = {"date", "ticker", "name", "open", "high", "low", "close", "volume", "amount", "turnover_rate"}
    missing = required - set(histories.columns)
    if missing:
        raise ValueError(f"histories missing columns: {sorted(missing)}")
    as_of = pd.Timestamp(as_of)
    benchmark = benchmark.sort_values("date").copy()
    benchmark["date"] = pd.to_datetime(benchmark["date"])
    benchmark["return_20d"] = benchmark["close"].pct_change(20, fill_method=None)
    benchmark_returns = benchmark.set_index("date")["return_20d"]
    market_previous = benchmark.loc[benchmark["date"] < as_of]
    market_today = benchmark.loc[benchmark["date"] == as_of].iloc[-1]
    market_pre_return_3d = float(market_previous.iloc[-1]["close"] / market_previous.iloc[-4]["close"] - 1)
    market_pre_drawdown_5d = float(market_previous.iloc[-1]["close"] / market_previous.tail(5)["close"].max() - 1)
    market_return_1d = float(market_today["close"] / market_previous.iloc[-1]["close"] - 1)

    rows: list[dict[str, object]] = []
    for ticker, group in histories.groupby("ticker", sort=False):
        group = group.sort_values("date").drop_duplicates("date", keep="last").copy()
        group["date"] = pd.to_datetime(group["date"])
        current_rows = group.loc[group["date"] == as_of]
        if current_rows.empty:
            continue
        close = group["close"]
        group["ma5"] = close.rolling(5, min_periods=5).mean()
        group["ma10"] = close.rolling(10, min_periods=10).mean()
        group["ma20"] = close.rolling(20, min_periods=20).mean()
        group["ma60"] = close.rolling(60, min_periods=60).mean()
        group["ma20_slope_5d"] = group["ma20"] / group["ma20"].shift(5) - 1
        group["return_5d"] = close.pct_change(5, fill_method=None)
        group["return_20d"] = close.pct_change(20, fill_method=None)
        group["high120"] = close.rolling(120, min_periods=120).max()
        group["distance_to_120d_high"] = close / group["high120"] - 1
        group["new_high_count_20d"] = (close >= group["high120"] * 0.999).astype(float).rolling(20, min_periods=1).sum()
        group["volume_ratio_20d"] = group["volume"] / group["volume"].shift(1).rolling(20, min_periods=20).mean()
        group["leader_signature"] = _leader_signature(group, benchmark_returns)
        group["prior_leader_score_20d"] = group["leader_signature"].shift(1).rolling(20, min_periods=1).max()
        previous = group.loc[group["date"] < as_of]
        current = group.loc[group["date"] == as_of].iloc[-1]
        if len(previous) < 120:
            continue
        pre_return_3d = float(previous.iloc[-1]["close"] / previous.iloc[-4]["close"] - 1)
        pre_drawdown_5d = float(previous.iloc[-1]["close"] / previous.tail(5)["close"].max() - 1)
        spread = float(current["high"] - current["low"])
        location = float((current["close"] - current["low"]) / spread) if spread > 0 else 0.5
        market_return20 = float(benchmark_returns.get(as_of, np.nan))
        raw_volume_ratio = float(current["volume_ratio_20d"])
        volume_data_reliable = bool(np.isfinite(raw_volume_ratio) and 0.05 <= raw_volume_ratio <= 20.0)
        row = {
            "date": as_of, "ticker": ticker, "name": current["name"], "close": float(current["close"]),
            "return_1d": float(current["close"] / previous.iloc[-1]["close"] - 1),
            "return_5d": float(current["return_5d"]), "return_20d": float(current["return_20d"]),
            "pre_selloff_return_3d": pre_return_3d, "market_pre_selloff_return_3d": market_pre_return_3d,
            "selloff_resilience_3d": pre_return_3d - market_pre_return_3d,
            "pre_drawdown_5d": pre_drawdown_5d, "market_pre_drawdown_5d": market_pre_drawdown_5d,
            "recovery_vs_market_1d": float(current["close"] / previous.iloc[-1]["close"] - 1) - market_return_1d,
            "intraday_close_location": location, "close_vs_open": float(current["close"] / current["open"] - 1),
            "above_ma5": bool(current["close"] >= current["ma5"]), "above_ma10": bool(current["close"] >= current["ma10"]),
            "above_ma20": bool(current["close"] >= current["ma20"]), "ma20_above_ma60": bool(current["ma20"] >= current["ma60"]),
            "ma20_slope_5d": float(current["ma20_slope_5d"]),
            "distance_to_ma20": float(current["close"] / current["ma20"] - 1),
            "distance_to_120d_high": float(current["distance_to_120d_high"]),
            "new_high_count_20d": float(current["new_high_count_20d"]),
            "rs_market_20d": float(current["return_20d"] - market_return20),
            "volume_ratio_20d": raw_volume_ratio if volume_data_reliable else np.nan,
            "volume_data_reliable": volume_data_reliable,
            "amount_rank_market": float(current["amount_rank_market"]),
            "prior_leader_score_20d": float(current["prior_leader_score_20d"]),
            "current_leader_score": float(current["leader_signature"]),
            "history_days": len(group),
            "locked_limit_up": bool(np.isclose(current["high"], current["low"]) and current["close"] > previous.iloc[-1]["close"] * 1.095),
            "feature_version": REVERSAL_FEATURE_VERSION,
        }
        rows.append(row)
    return pd.DataFrame(rows)


def _score_reversal(row: pd.Series) -> dict[str, object]:
    reasons: list[str] = []
    risks: list[str] = []
    exclusions: list[str] = []
    if row["history_days"] < 120:
        exclusions.append("insufficient_history")
    if row["locked_limit_up"]:
        exclusions.append("locked_limit_up")
    if not row["volume_data_reliable"]:
        risks.append("volume_unit_unreliable")

    resilience = 0
    if row["selloff_resilience_3d"] >= 0:
        resilience += 10; reasons.append("outperformed_during_selloff")
    if row["pre_drawdown_5d"] >= row["market_pre_drawdown_5d"]:
        resilience += 7; reasons.append("shallower_drawdown_than_market")
    if row["prior_leader_score_20d"] >= 70:
        resilience += 8; reasons.append("prior_main_uptrend_signature")

    repair = 0
    if row["return_1d"] > 0:
        repair += 5; reasons.append("positive_repair_day")
    if row["intraday_close_location"] >= 0.80:
        repair += 7; reasons.append("closed_near_intraday_high")
    elif row["intraday_close_location"] >= 0.65:
        repair += 4; reasons.append("strong_intraday_recovery")
    if row["close_vs_open"] >= 0.01:
        repair += 5; reasons.append("bullish_real_body")
    elif row["close_vs_open"] > 0:
        repair += 3
    if row["recovery_vs_market_1d"] >= 0:
        repair += 5; reasons.append("repair_outperformed_market")
    elif row["recovery_vs_market_1d"] >= -0.01:
        repair += 2
    if row["above_ma5"]:
        repair += 3; reasons.append("reclaimed_ma5")

    leader = 0
    if row["above_ma20"]:
        leader += 5; reasons.append("held_or_reclaimed_ma20")
    if row["ma20_above_ma60"]:
        leader += 5; reasons.append("medium_trend_intact")
    if row["ma20_slope_5d"] > 0:
        leader += 4; reasons.append("ma20_still_rising")
    if row["rs_market_20d"] > 0:
        leader += 6; reasons.append("positive_20d_relative_strength")
    if row["distance_to_120d_high"] >= -0.10:
        leader += 5; reasons.append("near_120d_high_after_selloff")
    if row["new_high_count_20d"] >= 2:
        leader += 5; reasons.append("recent_repeated_highs")

    capital = 0
    if row["volume_ratio_20d"] >= 1.5:
        capital += 8; reasons.append("repair_with_volume_expansion")
    elif row["volume_ratio_20d"] >= 1.1:
        capital += 5
    elif row["volume_ratio_20d"] >= 0.8:
        capital += 2
    if row["amount_rank_market"] <= 100:
        capital += 6; reasons.append("top100_market_amount")
    elif row["amount_rank_market"] <= 300:
        capital += 4
    elif row["amount_rank_market"] <= 800:
        capital += 2

    penalty = 0
    if row["return_5d"] > 0.30:
        penalty += 8; risks.append("five_day_overheat")
    if row["distance_to_ma20"] > 0.25:
        penalty += 7; risks.append("extended_above_ma20")
    if row["intraday_close_location"] < 0.50:
        penalty += 8; risks.append("weak_close")
    if row["volume_ratio_20d"] >= 1.8 and row["intraday_close_location"] < 0.45 and row["return_1d"] <= 0.02:
        penalty += 10; risks.append("high_volume_stall")
    total = max(0, min(100, resilience + repair + leader + capital - penalty))
    repair_confirmed = row["return_1d"] > 0 and row["intraday_close_location"] >= 0.65
    if total >= 70 and repair_confirmed and row["prior_leader_score_20d"] >= 70:
        stage = "LEADER_REPAIR_CONFIRMED"
    elif total >= 60 and repair_confirmed:
        stage = "REPAIR_CANDIDATE"
    elif total >= 50:
        stage = "EARLY_STABILIZATION"
    else:
        stage = "NORMAL"
    focus_selected = bool(
        not exclusions and stage == "LEADER_REPAIR_CONFIRMED"
        and row["prior_leader_score_20d"] >= 80 and row["amount_rank_market"] <= 300
        and row["selloff_resilience_3d"] >= 0 and row["recovery_vs_market_1d"] >= 0
        and row["rs_market_20d"] > 0 and row["distance_to_120d_high"] >= -0.15
        and not risks
    )
    return {
        "eligible": not exclusions, "reversal_score": total, "stage": stage,
        "focus_selected": focus_selected,
        "resilience_score": resilience, "repair_score": repair, "leader_quality_score": leader,
        "capital_score": capital, "risk_penalty": penalty, "top_reasons": "|".join(reasons),
        "risk_flags": "|".join(risks), "exclusion_reasons": "|".join(exclusions),
        "score_version": REVERSAL_SCORE_VERSION,
    }


def score_reversal_features(features: pd.DataFrame) -> pd.DataFrame:
    scored = features.copy()
    values = scored.apply(_score_reversal, axis=1, result_type="expand")
    for column in values:
        scored[column] = values[column]
    return scored.sort_values(["eligible", "reversal_score", "amount_rank_market"], ascending=[False, False, True])


def publish_reversal_screen(
    db_path: Path | str,
    scored: pd.DataFrame,
    regime: MarketRepairRegime,
    source_metadata: dict[str, object],
) -> None:
    """Idempotently publish one as-of cross-section into the research Gold DB."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS gold_cn_reversal_screen_results (
            as_of TEXT NOT NULL,ticker TEXT NOT NULL,name TEXT NOT NULL,
            reversal_score REAL NOT NULL,stage TEXT NOT NULL,focus_selected INTEGER NOT NULL,
            feature_version TEXT NOT NULL,score_version TEXT NOT NULL,market_regime TEXT NOT NULL,
            feature_json TEXT NOT NULL,top_reasons TEXT NOT NULL,risk_flags TEXT NOT NULL,
            exclusion_reasons TEXT NOT NULL,source_metadata_json TEXT NOT NULL,
            PRIMARY KEY(as_of,ticker,score_version))"""
        )
        conn.execute("BEGIN IMMEDIATE")
        as_of = regime.as_of
        conn.execute(
            "DELETE FROM gold_cn_reversal_screen_results WHERE as_of=? AND score_version=?",
            (as_of, REVERSAL_SCORE_VERSION),
        )
        excluded = {
            "date", "ticker", "name", "reversal_score", "stage", "focus_selected",
            "feature_version", "score_version", "market_regime", "top_reasons",
            "risk_flags", "exclusion_reasons",
        }
        source_json = json.dumps(source_metadata, ensure_ascii=False, sort_keys=True)
        for record in scored.to_dict(orient="records"):
            feature_payload: dict[str, object] = {}
            for key, value in record.items():
                if key in excluded:
                    continue
                if pd.isna(value):
                    feature_payload[key] = None
                elif isinstance(value, (np.bool_, bool)):
                    feature_payload[key] = bool(value)
                elif isinstance(value, (np.integer, int)):
                    feature_payload[key] = int(value)
                elif isinstance(value, (np.floating, float)):
                    feature_payload[key] = float(value)
                else:
                    feature_payload[key] = value
            conn.execute(
                "INSERT INTO gold_cn_reversal_screen_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    as_of, record["ticker"], record["name"], float(record["reversal_score"]),
                    record["stage"], int(bool(record["focus_selected"])), record["feature_version"],
                    record["score_version"], regime.regime,
                    json.dumps(feature_payload, ensure_ascii=False, sort_keys=True),
                    record["top_reasons"], record["risk_flags"], record["exclusion_reasons"], source_json,
                ),
            )
