from __future__ import annotations

import pandas as pd

from quant_agent.data_sources.sina_market import prefilter_repair_universe
from quant_agent.screening.reversal import build_reversal_features, classify_market_repair, publish_reversal_screen, score_reversal_features


def _benchmark() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-01", periods=140)
    close = pd.Series(range(140), dtype=float) * 0.2 + 100
    close.iloc[-6:] = [126, 125, 124, 123, 121, 124]
    frame = pd.DataFrame({
        "date": dates, "open": close - 0.5, "high": close + 1, "low": close - 1,
        "close": close, "volume": 1_000_000,
    })
    frame.loc[frame.index[-1], ["open", "high", "low"]] = [121.0, 124.2, 120.0]
    return frame


def _leader_history(benchmark: pd.DataFrame) -> pd.DataFrame:
    dates = benchmark["date"]
    close = pd.Series(range(len(dates)), dtype=float) * 0.5 + 40
    close.iloc[-6:] = [107, 108, 107, 106, 105, 110]
    amount = pd.Series(100_000_000.0, index=range(len(dates)))
    amount.iloc[-1] = 180_000_000
    return pd.DataFrame({
        "date": dates, "ticker": "300001.SZ", "name": "测试龙头", "open": close - 1,
        "high": close + 1, "low": close - 2, "close": close, "volume": 10_000_000,
        "amount": amount, "turnover_rate": 0.03, "amount_rank_market": 50.0,
    })


def test_market_repair_regime_requires_prior_selloff_and_strong_close() -> None:
    regime = classify_market_repair(_benchmark())
    assert regime.regime == "SELLOFF_REPAIR"
    assert regime.recovery_return_1d > 0
    assert regime.intraday_close_location >= 0.65


def test_prior_leader_that_repairs_is_selected() -> None:
    benchmark = _benchmark()
    features = build_reversal_features(_leader_history(benchmark), benchmark, benchmark.iloc[-1]["date"])
    scored = score_reversal_features(features)
    row = scored.iloc[0]
    assert row["prior_leader_score_20d"] >= 70
    assert row["selloff_resilience_3d"] > 0
    assert row["stage"] == "LEADER_REPAIR_CONFIRMED"
    assert row["reversal_score"] >= 70
    assert row["focus_selected"]


def test_feature_date_is_exact_as_of() -> None:
    benchmark = _benchmark()
    as_of = benchmark.iloc[-1]["date"]
    histories = _leader_history(benchmark)
    future = histories.iloc[-1:].copy()
    future["date"] = as_of + pd.Timedelta(days=1)
    future["close"] = 999
    result = build_reversal_features(pd.concat([histories, future]), benchmark, as_of)
    assert result.iloc[0]["date"] == as_of
    assert result.iloc[0]["close"] != 999


def test_prefilter_preserves_market_amount_capacity_core() -> None:
    rows = []
    for index in range(20):
        rows.append({
            "ticker": f"{index:06d}.SZ", "amount": 1_000_000_000 - index,
            "amount_rank_market": index + 1, "return_1d": 0.01 + index / 100,
            "intraday_close_location": 0.9, "close": 10.0, "volume": 1_000_000,
            "is_st": False, "is_new_listing_name": False,
        })
    frame = pd.DataFrame(rows)
    selected = prefilter_repair_universe(frame, max_symbols=10)
    assert set(frame.nsmallest(10, "amount_rank_market")["ticker"]).issubset(set(selected["ticker"]))


def test_implausible_cross_source_volume_ratio_is_not_scored() -> None:
    benchmark = _benchmark()
    history = _leader_history(benchmark)
    history.loc[history.index[:-1], "volume"] = 1_000_000_000
    history.loc[history.index[-1], "volume"] = 1_000_000
    features = build_reversal_features(history, benchmark, benchmark.iloc[-1]["date"])
    scored = score_reversal_features(features)
    assert pd.isna(scored.iloc[0]["volume_ratio_20d"])
    assert "volume_unit_unreliable" in scored.iloc[0]["risk_flags"]
    assert not scored.iloc[0]["focus_selected"]


def test_reversal_gold_publish_is_idempotent(tmp_path) -> None:
    benchmark = _benchmark()
    regime = classify_market_repair(benchmark)
    scored = score_reversal_features(build_reversal_features(_leader_history(benchmark), benchmark, benchmark.iloc[-1]["date"]))
    db_path = tmp_path / "research.db"
    publish_reversal_screen(db_path, scored, regime, {"source": "fixture"})
    publish_reversal_screen(db_path, scored, regime, {"source": "fixture"})
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM gold_cn_reversal_screen_results").fetchone()[0]
        focus = conn.execute("SELECT focus_selected FROM gold_cn_reversal_screen_results").fetchone()[0]
    assert count == 1
    assert focus == 1
