# Liquidity Transmission — 2026-07-17

> Valid until 2026-07-18T10:10:00+00:00; model `macro-regime-v1.1.0`.

## System Liquidity

- Primary: `RISK_ON_CONFIRMING`
- Risk: `NORMAL` (21/100)
- Liquidity: `STRONGLY_EXPANDING` (+100)
- 20D source impulse: `+176.2bn USD`
- Real-rate pressure: `NEUTRAL` (45/100)
- Confidence: 67%; coverage: 90%

## Source Decomposition

| Source | 20D liquidity contribution | Direction | Observation |
|---|---:|---|---|
| Fed balance sheet | +7.4bn | INJECTION | 2026-07-15 |
| TGA | +162.5bn | INJECTION | 2026-07-15 |
| RRP | +6.3bn | INJECTION | 2026-07-16 |

## Liquidity Absorption by Target

| Target | Proxy | Absorption | Score | Liquidity impulse | Market confirmation | Macro structure | Confidence |
|---|---|---|---:|---:|---:|---:|---:|
| Banks / credit | KRE | ABSORBING | +50.6 | +28.0 | +30.8 | -8.2 | 67% |
| US large cap | SPY | MIXED | +16.7 | +30.0 | -5.8 | -7.5 | 67% |
| US small cap | IWM | MIXED | +1.0 | +32.0 | -19.1 | -11.8 | 67% |
| Dollar / cash | DTWEXBGS | MIXED | -6.5 | -22.0 | -0.8 | +16.3 | 47% |
| AI / semiconductors | QQQ | MIXED | -16.9 | +30.0 | -31.7 | -15.2 | 67% |
| Treasury 7–10Y | IEF | REJECTING | -25.4 | +8.0 | -17.8 | -15.6 | 67% |
| Gold | GLD | REJECTING | -31.2 | +18.0 | -39.8 | -9.4 | 67% |
| Treasury 20Y+ | TLT | REJECTING | -41.4 | +8.0 | -28.6 | -20.9 | 67% |

## Half-Month Change (2026-07-03 → 2026-07-17)

- Net liquidity: -54.7bn → +176.2bn
- Risk score: 10.5 → 20.9
- Real-rate pressure: 60.0 → 45.0
- Material change events: 8

## Evidence

- Drivers: 10Y_REAL_YIELD_HIGH_PERCENTILE, 10Y_REAL_YIELD_RISING_20D, GOLD_WEAKNESS_CONFIRMS_REAL_RATE_PRESSURE, SMALL_CAP_TRANSMISSION_WEAK, FED_BALANCE_SHEET_EXPANDING, TGA_DRAWDOWN_INJECTS_LIQUIDITY, RRP_DRAWDOWN_RELEASES_LIQUIDITY, DOLLAR_WEAKER_SUPPORTS_LIQUIDITY
- Confirmations: CREDIT_SPREAD_CONTAINED, FED_BALANCE_SHEET_EXPANDING, TGA_DRAWDOWN_INJECTS_LIQUIDITY, RRP_DRAWDOWN_RELEASES_LIQUIDITY, DOLLAR_WEAKER_SUPPORTS_LIQUIDITY
- Conflicts: CREDIT_SPREAD_CONTAINED
- Quality flags: BROAD_DOLLAR_PROXY_NOT_ICE_DXY
- Stale series: -

> Target scores are relative liquidity-transmission proxies, not audited ETF creation/redemption flows, not investment advice, and not price forecasts.
