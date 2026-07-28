# A-share Selloff Repair Screen — 2026-07-24

> Publication mode: `INTRADAY_PREVIEW`. Intraday data are incomplete; this preview is not written to Gold.

> Research screen only. The historical universe was prefiltered from the full Sina snapshot; this is not yet a full-universe backtest.

## Market Regime

- Regime: `NO_CONFIRMED_REPAIR`
- Prior 3-day return: 2.82%
- Prior 5-day drawdown: -0.24%
- Repair-day return: -1.21%
- Intraday close location: 36.6%

## Data Quality

- Full snapshot rows: 5200
- Historical prefilter rows: 495
- Successful histories: 495
- History coverage: 100.0%
- Feature coverage: 98.8%
- Sources: Sina all-A snapshot/amount rank + Tencent qfq OHLCV + CSI300 Sina index

## Focus Candidates

> Focus requires prior leader score >= 80, market amount rank <= 300, positive 20-day RS, within 15% of the 120-day high, and no risk flag.

| Rank | Ticker | Name | Stage | Score | Prior leader | Selloff RS | Today | vs market | Volume ratio | Amount rank | Reasons | Risks |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| 1 | 603893.SH | 瑞芯微 | LEADER_REPAIR_CONFIRMED | 81 | 100 | 0.06% | 5.16% | 6.37% | 1.18 | 20 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|strong_intraday_recovery|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|top100_market_amount | - |
| 2 | 300458.SZ | 全志科技 | LEADER_REPAIR_CONFIRMED | 81 | 100 | 9.18% | 3.16% | 4.37% | 1.18 | 52 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|strong_intraday_recovery|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|top100_market_amount | - |
| 3 | 002156.SZ | 通富微电 | LEADER_REPAIR_CONFIRMED | 80 | 100 | 8.01% | 10.00% | 11.21% | 1.31 | 2 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|top100_market_amount | - |

## Broader Repair Watchlist

| Rank | Ticker | Name | Stage | Score | Amount rank | Risks |
|---:|---|---|---|---:|---:|---|
| 1 | 002774.SZ | 快意电梯 | LEADER_REPAIR_CONFIRMED | 88 | 1025 | - |
| 2 | 300214.SZ | 日科化学 | LEADER_REPAIR_CONFIRMED | 85 | 343 | - |
| 3 | 300929.SZ | 华骐环保 | LEADER_REPAIR_CONFIRMED | 82 | 1984 | - |
| 4 | 301127.SZ | 武汉天源 | LEADER_REPAIR_CONFIRMED | 80 | 508 | - |
| 5 | 688072.SH | 拓荆科技 | LEADER_REPAIR_CONFIRMED | 79 | 23 | volume_unit_unreliable |
| 6 | 300996.SZ | 普联软件 | LEADER_REPAIR_CONFIRMED | 78 | 1023 | - |
| 7 | 688237.SH | 超卓航科 | LEADER_REPAIR_CONFIRMED | 77 | 769 | volume_unit_unreliable |
| 8 | 603983.SH | 丸美生物 | LEADER_REPAIR_CONFIRMED | 77 | 1052 | - |
| 9 | 603580.SH | 艾艾精工 | LEADER_REPAIR_CONFIRMED | 76 | 534 | extended_above_ma20 |
| 10 | 605028.SH | 世茂能源 | LEADER_REPAIR_CONFIRMED | 75 | 1337 | - |
| 11 | 003033.SZ | 征和工业 | LEADER_REPAIR_CONFIRMED | 75 | 2551 | - |
| 12 | 002083.SZ | 孚日股份 | LEADER_REPAIR_CONFIRMED | 73 | 297 | - |
| 13 | 600617.SH | 国新能源 | LEADER_REPAIR_CONFIRMED | 73 | 1805 | - |
| 14 | 003013.SZ | 地铁设计 | LEADER_REPAIR_CONFIRMED | 73 | 1941 | - |
| 15 | 001258.SZ | 立新能源 | LEADER_REPAIR_CONFIRMED | 72 | 140 | five_day_overheat|extended_above_ma20 |
