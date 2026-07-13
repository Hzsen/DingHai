"""A-share main-uptrend baseline features and scoring."""

from quant_agent.research.cn_wave.features import build_daily_features
from quant_agent.research.cn_wave.scoring import score_daily_features

__all__ = ["build_daily_features", "score_daily_features"]
