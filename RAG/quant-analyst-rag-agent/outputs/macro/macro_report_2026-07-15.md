# Liquidity Transmission — 2026-07-15

> Valid until 2026-07-16T23:59:00+00:00; model `macro-regime-v1.1.0`.

## System Liquidity

- Primary: `RISK_ON_CONFIRMING`
- Risk: `NORMAL` (20/100)
- Liquidity: `STRONGLY_EXPANDING` (+100)
- 20D source impulse: `+150.0bn USD`
- Real-rate pressure: `NEUTRAL` (45/100)
- Confidence: 67%; coverage: 90%

## Source Decomposition

| Source | 20D liquidity contribution | Direction | Observation |
|---|---:|---|---|
| Fed balance sheet | -0.04bn | DRAIN | 2026-07-08 |
| TGA | +144.6bn | INJECTION | 2026-07-08 |
| RRP | +5.4bn | INJECTION | 2026-07-14 |

## Liquidity Absorption by Target

| Target | Proxy | Absorption | Score | Liquidity impulse | Market confirmation | Macro structure | Confidence |
|---|---|---|---:|---:|---:|---:|---:|
| US large cap | SPY | MIXED | +19.9 | +30.0 | -2.7 | -7.4 | 73% |
| Banks / credit | KRE | MIXED | +13.6 | +28.0 | -6.5 | -7.9 | 73% |
| US small cap | IWM | MIXED | +1.0 | +32.0 | -19.4 | -11.6 | 73% |
| Gold | GLD | MIXED | -1.1 | +18.0 | -10.3 | -8.8 | 73% |
| AI / semiconductors | QQQ | MIXED | -6.0 | +30.0 | -20.9 | -15.1 | 73% |
| Dollar / cash | DTWEXBGS | MIXED | -7.0 | -22.0 | -1.1 | +16.1 | 60% |
| Treasury 7–10Y | IEF | REJECTING | -24.1 | +8.0 | -16.3 | -15.8 | 73% |
| Treasury 20Y+ | TLT | REJECTING | -42.7 | +8.0 | -29.7 | -21.0 | 73% |

## Half-Month Change (2026-07-01 → 2026-07-15)

- Net liquidity: -80.9bn → +150.0bn
- Risk score: 11.5 → 20.3
- Real-rate pressure: 60.0 → 45.0
- Material change events: 9

## Evidence

- Drivers: 10Y_REAL_YIELD_HIGH_PERCENTILE, 10Y_REAL_YIELD_RISING_20D, 10Y_NOMINAL_RISING, SMALL_CAP_TRANSMISSION_WEAK, TGA_DRAWDOWN_INJECTS_LIQUIDITY, RRP_DRAWDOWN_RELEASES_LIQUIDITY, DOLLAR_WEAKER_SUPPORTS_LIQUIDITY
- Confirmations: CREDIT_SPREAD_CONTAINED, TGA_DRAWDOWN_INJECTS_LIQUIDITY, RRP_DRAWDOWN_RELEASES_LIQUIDITY, DOLLAR_WEAKER_SUPPORTS_LIQUIDITY
- Conflicts: CREDIT_SPREAD_CONTAINED, FED_BALANCE_SHEET_CONTRACTING
- Quality flags: BROAD_DOLLAR_PROXY_NOT_ICE_DXY
- Stale series: -

> Target scores are relative liquidity-transmission proxies, not audited ETF creation/redemption flows, not investment advice, and not price forecasts.
