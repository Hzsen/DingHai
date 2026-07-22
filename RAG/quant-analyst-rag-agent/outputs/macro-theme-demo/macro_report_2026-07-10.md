# Liquidity Transmission — 2026-07-10

> Valid until 2026-07-11T20:00:00+00:00; model `macro-regime-v1.1.0`.

## System Liquidity

- Primary: `LONG_DURATION_PRESSURE`
- Risk: `NORMAL` (32/100)
- Liquidity: `CONTRACTING` (-42)
- 20D source impulse: `-25.0bn USD`
- Real-rate pressure: `EXTREME_PRESSURE` (95/100)
- Confidence: 80%; coverage: 100%

## Fast Market Theme (1–5D)

- Dominant: `REAL_RATE_TIGHTENING` — 真实利率约束增强
- Confidence: 59%
- Interpretation: 真实利率正在上升，长久期资产面临更强的折现率约束。
- Strongest signals: DGS30=+1.04z, DFII10=+0.78z, DGS10=+0.72z

| Theme | Family | Confidence | Confirmations | Persistence |
|---|---|---:|---:|---:|
| 真实利率约束增强 (`REAL_RATE_TIGHTENING`) | RATES_INFLATION | 59% | 1/3 | 1 |

- Supporting evidence: RATE_PRESSURE_SCORE_AT_LEAST_60=+95.00, DFII10_5D_Z_AT_LEAST_0_5=+0.78, DGS10_5D_Z_POSITIVE=+0.72
- Conflicting evidence: -
- Invalidation: DFII10_5D_Z_BELOW_0_25, RATE_PRESSURE_SCORE_BELOW_50

## Source Decomposition

| Source | 20D liquidity contribution | Direction | Observation |
|---|---:|---|---|
| Fed balance sheet | +10.0bn | INJECTION | 2026-07-10 |
| TGA | -40.0bn | DRAIN | 2026-07-10 |
| RRP | +5.0bn | INJECTION | 2026-07-10 |

## Liquidity Absorption by Target

| Target | Proxy | Absorption | Score | Liquidity impulse | Market confirmation | Macro structure | Confidence |
|---|---|---|---:|---:|---:|---:|---:|
| Dollar / cash | DTWEXBGS | ABSORBING | +44.0 | +9.2 | +4.5 | +30.3 | 100% |
| US large cap | SPY | REJECTING | -33.7 | -12.5 | -6.6 | -14.6 | 100% |
| Banks / credit | KRE | REJECTING | -39.8 | -11.7 | -16.7 | -11.4 | 44% |
| Treasury 7–10Y | IEF | REJECTING | -50.7 | -3.3 | -11.8 | -35.6 | 100% |
| US small cap | IWM | REJECTING | -54.8 | -13.3 | -20.6 | -20.8 | 100% |
| AI / semiconductors | QQQ | STRONG_REJECTION | -70.9 | -12.5 | -27.3 | -31.1 | 100% |
| Treasury 20Y+ | TLT | STRONG_REJECTION | -72.0 | -3.3 | -21.6 | -47.0 | 100% |
| Gold | GLD | STRONG_REJECTION | -72.4 | -7.5 | -31.9 | -33.0 | 100% |

## Evidence

- Drivers: 10Y_REAL_YIELD_HIGH_PERCENTILE, 10Y_REAL_YIELD_RISING_5D, 10Y_REAL_YIELD_RISING_20D, REAL_YIELD_CHANGE_STATISTICALLY_LARGE, 10Y_NOMINAL_RISING, LONG_END_TERM_PRESSURE, GOLD_WEAKNESS_CONFIRMS_REAL_RATE_PRESSURE, SMALL_CAP_TRANSMISSION_WEAK, BANK_TRANSMISSION_WEAK, FED_BALANCE_SHEET_EXPANDING, RRP_DRAWDOWN_RELEASES_LIQUIDITY
- Confirmations: CREDIT_SPREAD_CONTAINED, FED_BALANCE_SHEET_EXPANDING, RRP_DRAWDOWN_RELEASES_LIQUIDITY
- Conflicts: CREDIT_SPREAD_CONTAINED, TGA_REBUILD_DRAINS_LIQUIDITY, DOLLAR_STRENGTH_TIGHTENS
- Quality flags: ASYNCHRONOUS_RATE_DECOMPOSITION
- Stale series: -

> Target scores are relative liquidity-transmission proxies, not audited ETF creation/redemption flows, not investment advice, and not price forecasts.
