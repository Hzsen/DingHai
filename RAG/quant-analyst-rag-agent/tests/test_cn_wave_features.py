from __future__ import annotations

import numpy as np
import pandas as pd

from quant_agent.research.cn_wave.features import build_daily_features


def _market_fixture() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=280)
    close = np.linspace(10.0, 20.0, len(dates))
    return pd.DataFrame(
        {
            "date": dates,
            "ticker": "300308.SZ",
            "stock_name": "中际旭创",
            "open": close * 0.995,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": 1_000_000,
            "amount": 100_000_000.0,
            "turnover_rate": 0.02,
        }
    )


def test_features_respect_narrative_available_at_and_missing_cross_section() -> None:
    market = _market_fixture()
    dates = market["date"]
    benchmark = pd.DataFrame({"date": dates, "close": np.linspace(100.0, 110.0, len(dates))})
    event_date = dates.iloc[250]
    narratives = pd.DataFrame(
        [
            {
                "event_id": "event-1",
                "published_at": event_date,
                "available_at": event_date,
                "ticker": "300308.SZ",
                "theme_name": "AI光模块",
                "catalyst_type": "订单",
                "source_type": "company_disclosure",
                "source_title": "测试公告",
                "source_url": "https://example.com/evidence",
                "company_relevance": "direct",
                "theme_score": 3,
                "fundamental_score": 2,
                "narrative_conflict_flag": False,
                "risk_note": "",
            }
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "ticker": "300308.SZ",
                "start_date": dates.iloc[260],
                "end_date": dates.iloc[270],
                "leader_type": "institutional_trend",
                "theme": "CPO_AI_optical_module",
            }
        ]
    )

    features = build_daily_features(market, benchmark, narratives, labels)
    before_event = features.loc[features["date"] < event_date]
    at_event = features.loc[features["date"] == event_date].iloc[0]

    assert before_event["theme_score"].isna().all()
    assert at_event["theme_score"] == 3
    assert pd.isna(at_event["amount_rank_market"])
    assert pd.notna(features.iloc[249]["rolling_high_250d"])
    assert pd.notna(features.iloc[59]["chip_concentration_60d"])
    assert features["is_labeled_positive"].sum() == 11


def test_cross_section_rank_requires_minimum_universe() -> None:
    market = _market_fixture().tail(260)
    benchmark = pd.DataFrame({"date": market["date"], "close": market["close"] * 5})
    event_date = market["date"].iloc[0]
    narratives = pd.DataFrame(
        [
            {
                "event_id": "event-1",
                "published_at": event_date,
                "available_at": event_date,
                "ticker": "300308.SZ",
                "theme_name": "AI光模块",
                "catalyst_type": "订单",
                "source_type": "company_disclosure",
                "source_title": "测试公告",
                "source_url": "https://example.com/evidence",
                "company_relevance": "direct",
                "theme_score": 3,
                "fundamental_score": 2,
                "narrative_conflict_flag": False,
                "risk_note": "",
            }
        ]
    )
    labels = pd.DataFrame(
        [{"ticker": "300308.SZ", "start_date": event_date, "end_date": event_date, "leader_type": "test", "theme": "test"}]
    )

    features = build_daily_features(
        market,
        benchmark,
        narratives,
        labels,
        minimum_market_universe_size=1,
    )

    assert features["amount_rank_market"].eq(1).all()
