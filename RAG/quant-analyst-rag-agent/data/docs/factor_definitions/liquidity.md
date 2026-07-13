# Liquidity Dollar Volume Factor

The liquidity_dollar_volume factor estimates trading depth using recent dollar volume.

## Formula

liquidity_dollar_volume = mean(close * volume, 20)

## Interpretation

Higher dollar volume usually means better execution capacity, less slippage, and lower implementation risk. It is a practical factor for strategy screening because a statistically attractive signal can be unusable if turnover overwhelms available liquidity.

## Failure Modes

Liquidity can appear high during stressed trading because volume spikes when investors rush to exit positions. During liquidity shocks, bid-ask spreads may widen even as dollar volume rises, so volume should not be treated as complete proof of easy execution.
