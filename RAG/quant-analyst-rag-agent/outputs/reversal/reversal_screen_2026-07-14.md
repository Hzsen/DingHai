# A-share Selloff Repair Screen — 2026-07-14

> Research screen only. The historical universe was prefiltered from the full Sina snapshot; this is not yet a full-universe backtest.

## Market Regime

- Regime: `SELLOFF_REPAIR`
- Prior 3-day return: -1.26%
- Prior 5-day drawdown: -3.71%
- Repair-day return: 2.15%
- Intraday close location: 100.0%

## Data Quality

- Full snapshot rows: 5200
- Historical prefilter rows: 500
- Successful histories: 500
- History coverage: 100.0%
- Feature coverage: 98.6%
- Sources: Sina all-A snapshot/amount rank + Tencent qfq OHLCV + CSI300 Sina index

## Focus Candidates

> Focus requires prior leader score >= 80, market amount rank <= 300, positive 20-day RS, within 15% of the 120-day high, and no risk flag.

| Rank | Ticker | Name | Stage | Score | Prior leader | Selloff RS | Today | vs market | Volume ratio | Amount rank | Reasons | Risks |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| 1 | 002156.SZ | 通富微电 | LEADER_REPAIR_CONFIRMED | 94 | 90 | 13.40% | 5.84% | 3.69% | 1.52 | 10 | outperformed_during_selloff|shallower_drawdown_than_market|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|repair_with_volume_expansion|top100_market_amount | - |
| 2 | 603127.SH | 昭衍新药 | LEADER_REPAIR_CONFIRMED | 92 | 90 | 14.75% | 7.00% | 4.84% | 2.03 | 156 | outperformed_during_selloff|shallower_drawdown_than_market|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|repair_with_volume_expansion | - |
| 3 | 002422.SZ | 科伦药业 | LEADER_REPAIR_CONFIRMED | 92 | 100 | 7.98% | 8.26% | 6.11% | 1.74 | 162 | outperformed_during_selloff|shallower_drawdown_than_market|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|repair_with_volume_expansion | - |
| 4 | 603259.SH | 药明康德 | LEADER_REPAIR_CONFIRMED | 91 | 100 | 5.46% | 6.05% | 3.89% | 1.11 | 39 | outperformed_during_selloff|shallower_drawdown_than_market|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|top100_market_amount | - |
| 5 | 002851.SZ | 麦格米特 | LEADER_REPAIR_CONFIRMED | 91 | 100 | 10.65% | 8.49% | 6.33% | 1.41 | 44 | outperformed_during_selloff|shallower_drawdown_than_market|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|top100_market_amount | - |
| 6 | 002821.SZ | 凯莱英 | LEADER_REPAIR_CONFIRMED | 89 | 100 | 11.83% | 7.63% | 5.48% | 1.39 | 154 | outperformed_during_selloff|shallower_drawdown_than_market|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs | - |
| 7 | 600584.SH | 长电科技 | LEADER_REPAIR_CONFIRMED | 84 | 90 | 6.51% | 3.69% | 1.53% | 1.22 | 4 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|top100_market_amount | - |
| 8 | 002185.SZ | 华天科技 | LEADER_REPAIR_CONFIRMED | 84 | 100 | 13.13% | 6.42% | 4.27% | 1.66 | 6 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|strong_intraday_recovery|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|repair_with_volume_expansion|top100_market_amount | - |
| 9 | 002938.SZ | 鹏鼎控股 | LEADER_REPAIR_CONFIRMED | 82 | 100 | 6.75% | 9.28% | 7.13% | 1.22 | 56 | outperformed_during_selloff|shallower_drawdown_than_market|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|positive_20d_relative_strength|recent_repeated_highs|top100_market_amount | - |
| 10 | 300990.SZ | 同飞股份 | LEADER_REPAIR_CONFIRMED | 82 | 90 | 8.59% | 14.69% | 12.53% | 2.02 | 297 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|strong_intraday_recovery|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|repair_with_volume_expansion | - |
| 11 | 002384.SZ | 东山精密 | LEADER_REPAIR_CONFIRMED | 81 | 90 | 0.90% | 10.00% | 7.85% | 1.10 | 5 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|top100_market_amount | - |
| 12 | 300502.SZ | 新易盛 | LEADER_REPAIR_CONFIRMED | 80 | 100 | 1.59% | 10.99% | 8.84% | 1.30 | 3 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|positive_20d_relative_strength|near_120d_high_after_selloff|recent_repeated_highs|top100_market_amount | - |
| 13 | 000988.SZ | 华工科技 | LEADER_REPAIR_CONFIRMED | 78 | 100 | 5.83% | 3.63% | 1.48% | 1.01 | 32 | outperformed_during_selloff|shallower_drawdown_than_market|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|reclaimed_ma5|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|recent_repeated_highs|top100_market_amount | - |
| 14 | 300604.SZ | 长川科技 | LEADER_REPAIR_CONFIRMED | 73 | 100 | 3.79% | 3.19% | 1.04% | 1.05 | 31 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|closed_near_intraday_high|bullish_real_body|repair_outperformed_market|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|recent_repeated_highs|top100_market_amount | - |
| 15 | 002364.SZ | 中恒电气 | LEADER_REPAIR_CONFIRMED | 71 | 80 | 5.89% | 3.55% | 1.40% | 1.09 | 178 | outperformed_during_selloff|prior_main_uptrend_signature|positive_repair_day|strong_intraday_recovery|bullish_real_body|repair_outperformed_market|reclaimed_ma5|held_or_reclaimed_ma20|medium_trend_intact|ma20_still_rising|positive_20d_relative_strength|near_120d_high_after_selloff | - |

## Broader Repair Watchlist

| Rank | Ticker | Name | Stage | Score | Amount rank | Risks |
|---:|---|---|---|---:|---:|---|
| 1 | 605208.SH | 永茂泰 | LEADER_REPAIR_CONFIRMED | 88 | 911 | - |
| 2 | 300759.SZ | 康龙化成 | LEADER_REPAIR_CONFIRMED | 87 | 95 | extended_above_ma20 |
| 3 | 600428.SH | 中远海特 | LEADER_REPAIR_CONFIRMED | 85 | 279 | extended_above_ma20 |
| 4 | 603087.SH | 甘李药业 | LEADER_REPAIR_CONFIRMED | 85 | 346 | - |
| 5 | 603110.SH | 东方材料 | LEADER_REPAIR_CONFIRMED | 85 | 694 | - |
| 6 | 002458.SZ | 益生股份 | LEADER_REPAIR_CONFIRMED | 85 | 825 | - |
| 7 | 300558.SZ | 贝达药业 | LEADER_REPAIR_CONFIRMED | 84 | 325 | - |
| 8 | 603669.SH | 灵康药业 | LEADER_REPAIR_CONFIRMED | 83 | 725 | extended_above_ma20 |
| 9 | 000739.SZ | 普洛药业 | LEADER_REPAIR_CONFIRMED | 83 | 1124 | - |
| 10 | 002745.SZ | 木林森 | LEADER_REPAIR_CONFIRMED | 80 | 353 | - |
| 11 | 001359.SZ | 平安电工 | LEADER_REPAIR_CONFIRMED | 80 | 731 | - |
| 12 | 688106.SH | 金宏气体 | LEADER_REPAIR_CONFIRMED | 79 | 172 | volume_unit_unreliable |
| 13 | 601628.SH | 中国人寿 | LEADER_REPAIR_CONFIRMED | 79 | 242 | - |
| 14 | 600673.SH | 东阳光 | LEADER_REPAIR_CONFIRMED | 78 | 130 | - |
| 15 | 603233.SH | 大参林 | LEADER_REPAIR_CONFIRMED | 78 | 1241 | - |
