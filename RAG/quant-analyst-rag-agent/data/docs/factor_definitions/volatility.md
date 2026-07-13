# Volatility 20D Factor

The volatility_20d factor measures annualized realized volatility over the most recent 20 trading days.

## Formula

volatility_20d = stdev(daily_returns, 20) * sqrt(252)

## Interpretation

Lower-volatility baskets often behave defensively, while high-volatility readings can identify unstable securities. In the sample regime table, the low-volatility strategy is more resilient than momentum during high-volatility regimes.

## Failure Modes

The factor can lag in fast recoveries because defensive securities may not rebound as quickly as cyclical or high-beta names.
