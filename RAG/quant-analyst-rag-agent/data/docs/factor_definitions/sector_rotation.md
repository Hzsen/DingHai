# Sector Relative Strength Factor

The sector_relative_strength factor compares sector performance with a broad-market benchmark over a 60-day window.

## Formula

sector_relative_strength = sector_return_60d - benchmark_return_60d

## Interpretation

Positive values indicate sectors outperforming the broad market. The related sector_rotation_strategy rotates toward sectors with stronger relative strength while controlling turnover and transaction costs.

## Failure Modes

Sector leadership can reverse during macro shocks, commodity dislocations, and policy surprises. Concentrated benchmark leadership can also make relative strength unstable.
