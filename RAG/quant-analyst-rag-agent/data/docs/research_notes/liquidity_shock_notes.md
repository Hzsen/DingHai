# Liquidity Shock Notes

Liquidity shocks occur when trading volume rises but execution quality deteriorates. In March 2020, the sample anomaly log records liquidity_shock conditions for JPM, where dollar volume surged while bid-ask conditions deteriorated.

## Why Dollar Volume Is Not Enough

Dollar volume is useful, but it is not the same as executable liquidity. Stress periods can produce very high volume because many participants are trying to exit at the same time.

## Research Implication

Liquidity-aware strategies should combine dollar volume with turnover, transaction cost assumptions, and anomaly checks. The liquidity_dollar_volume factor is best interpreted as a capacity screen rather than a standalone alpha signal.
