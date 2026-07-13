# Momentum 60D Factor

The momentum_60d factor measures intermediate-term price strength over the prior 60 trading days. It is calculated as adjusted close divided by adjusted close 60 trading days ago minus one.

## Formula

momentum_60d = adjusted_close / adjusted_close_60d_ago - 1

## Interpretation

A high score indicates that a stock has recently outperformed. The factor is often useful in bull-trend and recovery regimes when investor flows reinforce existing winners.

## Failure Modes

Momentum can fail during abrupt volatility spikes, crowded-position unwinds, policy shocks, and fast factor rotations. The March 2020 COVID shock is an example in the sample data: prior winners sold off as investors raised cash and reduced gross exposure.
