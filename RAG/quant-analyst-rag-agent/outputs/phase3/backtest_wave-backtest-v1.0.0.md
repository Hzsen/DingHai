# A-share WaveScore Pilot Backtest

> Research validation only. Pilot universe results are not an investment recommendation.

## Protocol

- Signal: trade-date close
- Entry: next trading day open
- Exit/rebalance: entry plus configured holding period, at open
- Top N: 3
- Minimum WaveScore: 55
- Holding period: 5 trading days
- Signal frequency: weekly
- One-way transaction cost: 10 bps

## Results

- Period: 2022-01-07 to 2026-06-18
- Active days: 186
- Annual return: 62.12%
- Benchmark annual return: 1.43%
- Sharpe: 1.14
- Max drawdown: -54.74%
- Hit rate: 54.84%
- OOS start: 2025-01-01
- OOS annual return: 150.73%
- OOS max drawdown: -38.15%

## Limitations

- Eight-stock ex-post case-study universe creates severe selection bias.
- The OOS date split does not remove universe selection bias; returns are engineering diagnostics only.
- Current-adjusted cached prices are not strict historical corporate-action vintages.
- Limit-lock handling is conservative and based on daily bars.
