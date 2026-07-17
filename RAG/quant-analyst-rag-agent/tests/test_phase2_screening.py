from __future__ import annotations

import numpy as np
import pandas as pd

from quant_agent.screening.wave import build_wave_features, score_wave_features, screen_as_of


def _prices() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=160)
    frames = []
    for ticker, name, multiplier in (("000300.SH", "沪深300", 1.0), ("300001.SZ", "强势股", 1.8), ("600001.SH", "普通股", 0.8)):
        close = np.linspace(10, 15 * multiplier, len(dates))
        frames.append(pd.DataFrame({"ticker":ticker,"name":name,"date":dates,"open":close*0.998,"high":close*1.01,"low":close*0.99,"close":close,"volume":1_000_000.0,"amount":close*1_000_000,"turnover_rate":0.03,"source_run_id":"run-1","adjustment":"qfq","available_at":"2025-01-01"}))
    return pd.concat(frames, ignore_index=True)


def test_future_prices_do_not_change_past_features() -> None:
    prices = _prices()
    cutoff = pd.Timestamp("2025-07-01")
    original = build_wave_features(prices)
    changed = prices.copy()
    changed.loc[changed["date"] > cutoff, "close"] *= 10
    rebuilt = build_wave_features(changed)
    columns = ["ticker","date","return_20d","ma_60d","distance_to_120d_high","rs_market_20d"]
    left = original.loc[original["date"] <= cutoff, columns].reset_index(drop=True)
    right = rebuilt.loc[rebuilt["date"] <= cutoff, columns].reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right)


def test_screen_outputs_explainable_components() -> None:
    scored = score_wave_features(build_wave_features(_prices()))
    as_of = scored["date"].max()
    snapshot = screen_as_of(scored, as_of, top_n=1)

    selected = snapshot.loc[snapshot["selected"]]
    assert len(selected) == 1
    assert selected.iloc[0]["ticker"] != "000300.SH"
    assert selected.iloc[0]["top_reasons"]
    assert snapshot.loc[snapshot["ticker"] == "000300.SH", "exclusion_reasons"].str.contains("benchmark").all()
