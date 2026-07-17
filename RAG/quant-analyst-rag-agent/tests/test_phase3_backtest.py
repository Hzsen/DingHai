from __future__ import annotations

import pandas as pd

from quant_agent.research.cn_wave.backtest import run_wave_backtest


def test_backtest_uses_next_open_for_execution() -> None:
    dates = pd.bdate_range("2026-01-05", periods=5)
    rows = []
    for ticker, opens, eligible, score in (
        ("000300.SH", [100, 101, 102, 103, 104], False, 0),
        ("300001.SZ", [10, 20, 30, 45, 45], True, 80),
    ):
        for index, day in enumerate(dates):
            rows.append({"ticker":ticker,"date":day,"open":opens[index],"volume":100,"eligible":eligible,"wave_score":score,"rs_market_20d":0.1,"locked_limit_up":False})
    scored = pd.DataFrame(rows)

    daily, summary = run_wave_backtest(scored, top_n=1, minimum_score=55, transaction_cost_bps=0, holding_days=1, signal_frequency="every_holding_period")

    first = daily.loc[daily["signal_date"] == dates[0]].iloc[0]
    assert first["execution_date"] == dates[1]
    assert first["exit_date"] == dates[2]
    assert first["gross_return"] == 0.5
    assert summary["active_days"] > 0


def test_weekly_frequency_uses_last_trading_day_of_week() -> None:
    dates = pd.bdate_range("2026-01-05", periods=20)
    rows = []
    for ticker, base, eligible, score in (("000300.SH", 100, False, 0), ("300001.SZ", 10, True, 80)):
        for index, day in enumerate(dates):
            rows.append({"ticker":ticker,"date":day,"open":base+index,"volume":100,"eligible":eligible,"wave_score":score,"rs_market_20d":0.1,"locked_limit_up":False})
    daily, summary = run_wave_backtest(pd.DataFrame(rows), top_n=1, minimum_score=55, transaction_cost_bps=0, holding_days=5, signal_frequency="weekly")

    assert all(day.weekday() == 4 for day in daily["signal_date"])
    assert summary["signal_frequency"] == "weekly"
