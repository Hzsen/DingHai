from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd


FEATURE_VERSION = "cn-wave-features-v0.1.0"
REQUIRED_MARKET_COLUMNS = {
    "date",
    "ticker",
    "stock_name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "turnover_rate",
}


def _datetime_ns(values: pd.Series) -> pd.Series:
    """Normalize CSV and Parquet timestamps to one resolution for pandas 3 as-of joins."""
    return pd.to_datetime(values).astype("datetime64[ns]")


def board_limit_pct(ticker: str) -> float:
    """Return the normal daily price-limit ratio for the Phase 0 sample period."""
    code = ticker.split(".", maxsplit=1)[0]
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    if code.startswith(("8", "4")):
        return 0.30
    return 0.10


def _require_columns(frame: pd.DataFrame, required: Iterable[str], frame_name: str) -> None:
    missing = set(required) - set(frame.columns)
    if missing:
        raise ValueError(f"{frame_name} is missing columns: {sorted(missing)}")


def _chip_window_features(group: pd.DataFrame, window: int = 60, min_periods: int = 40) -> pd.DataFrame:
    """Approximate volume-at-price features from daily bars.

    This is deliberately named a proxy: without tick-level holder data we allocate each
    day's amount to its typical price, then aggregate the last 60 sessions into 20 bins.
    """
    concentration = np.full(len(group), np.nan)
    avg_cost_distance = np.full(len(group), np.nan)
    overhead_supply = np.full(len(group), np.nan)

    for end in range(len(group)):
        start = max(0, end - window + 1)
        sample = group.iloc[start : end + 1]
        if len(sample) < min_periods:
            continue

        weights = sample["amount"].to_numpy(dtype=float)
        typical_prices = ((sample["high"] + sample["low"] + sample["close"]) / 3).to_numpy(dtype=float)
        valid = np.isfinite(weights) & np.isfinite(typical_prices) & (weights > 0)
        if valid.sum() < min_periods or weights[valid].sum() <= 0:
            continue

        prices = typical_prices[valid]
        weights = weights[valid]
        current_close = float(group.iloc[end]["close"])
        low_price = float(prices.min())
        high_price = float(prices.max())

        if np.isclose(low_price, high_price):
            concentration[end] = 1.0
        else:
            bins = np.linspace(low_price, high_price, 21)
            histogram, _ = np.histogram(prices, bins=bins, weights=weights)
            concentration[end] = float(np.sort(histogram)[-3:].sum() / histogram.sum())

        average_cost = float(np.average(prices, weights=weights))
        avg_cost_distance[end] = current_close / average_cost - 1 if average_cost else np.nan
        overhead_supply[end] = float(weights[prices > current_close].sum() / weights.sum())

    return pd.DataFrame(
        {
            "chip_concentration_60d": concentration,
            "avg_cost_distance": avg_cost_distance,
            "overhead_supply_ratio": overhead_supply,
        },
        index=group.index,
    )


def _compute_one_stock(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values("date").copy()
    close = group["close"]

    for days in (1, 5, 20, 60):
        group[f"return_{days}d"] = close.pct_change(days, fill_method=None)

    group["amount_ratio_20d"] = group["amount"] / group["amount"].shift(1).rolling(20, min_periods=20).mean()
    group["turnover_20d_sum"] = group["turnover_rate"].rolling(20, min_periods=20).sum()
    group["base_turnover_sum_60d"] = group["turnover_rate"].rolling(60, min_periods=40).sum()

    for days in (60, 120, 250):
        group[f"rolling_high_{days}d"] = close.rolling(days, min_periods=days).max()

    group["distance_to_120d_high"] = close / group["rolling_high_120d"] - 1
    at_120d_high = close >= group["rolling_high_120d"] * 0.999
    group["new_high_count_20d"] = at_120d_high.astype(float).rolling(20, min_periods=1).sum()

    group["board_limit_pct"] = board_limit_pct(str(group.iloc[0]["ticker"]))
    group["limit_up_flag"] = group["return_1d"] >= group["board_limit_pct"] - 0.005
    group["limit_up_count_10d"] = group["limit_up_flag"].astype(int).rolling(10, min_periods=1).sum()
    group["large_up_flag"] = group["return_1d"] > 0.07
    group["large_up_count_10d"] = group["large_up_flag"].astype(int).rolling(10, min_periods=1).sum()

    group["upper_shadow_ratio"] = (
        group["high"] - group[["open", "close"]].max(axis=1)
    ).clip(lower=0) / group["close"]
    group["high_volume_stall_flag"] = (
        (group["amount_ratio_20d"] >= 2.0)
        & (group["return_1d"] <= 0.03)
        & (group["upper_shadow_ratio"] >= 0.03)
    )

    chip_features = _chip_window_features(group)
    for column in chip_features:
        group[column] = chip_features[column]
    return group


def _attach_benchmark(features: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    _require_columns(benchmark, {"date", "close"}, "benchmark")
    benchmark = benchmark[["date", "close"]].copy().sort_values("date")
    benchmark["date"] = _datetime_ns(benchmark["date"])
    benchmark["benchmark_return_20d"] = benchmark["close"].pct_change(20, fill_method=None)
    benchmark = benchmark.rename(columns={"close": "benchmark_close"})
    features = features.merge(benchmark, on="date", how="left", validate="many_to_one")
    features["rs_market_20d"] = features["return_20d"] - features["benchmark_return_20d"]
    return features


def _attach_narratives(features: pd.DataFrame, narratives: pd.DataFrame) -> pd.DataFrame:
    required = {
        "event_id",
        "available_at",
        "ticker",
        "theme_name",
        "company_relevance",
        "theme_score",
        "fundamental_score",
        "narrative_conflict_flag",
    }
    _require_columns(narratives, required, "narratives")
    narratives = narratives.copy()
    narratives["available_at"] = _datetime_ns(narratives["available_at"])
    narratives["published_at"] = _datetime_ns(narratives["published_at"])
    narratives["narrative_conflict_flag"] = narratives["narrative_conflict_flag"].map(
        lambda value: str(value).strip().lower() == "true"
    )

    optional_columns = {
        "theme_type",
        "evidence_strength",
        "narrative_freshness",
        "fundamental_support",
    }
    for column in optional_columns - set(narratives.columns):
        narratives[column] = pd.NA

    event_columns = [
        "event_id",
        "published_at",
        "available_at",
        "theme_name",
        "theme_type",
        "catalyst_type",
        "source_type",
        "source_title",
        "source_url",
        "evidence_strength",
        "company_relevance",
        "narrative_freshness",
        "theme_score",
        "fundamental_score",
        "fundamental_support",
        "narrative_conflict_flag",
        "risk_note",
    ]
    merged_groups: list[pd.DataFrame] = []
    for ticker, group in features.groupby("ticker", sort=False):
        events = narratives.loc[narratives["ticker"] == ticker, event_columns].sort_values("available_at")
        group = group.sort_values("date")
        if events.empty:
            for column in event_columns:
                group[column] = pd.NA
            merged_groups.append(group)
            continue
        merged_groups.append(
            pd.merge_asof(
                group,
                events,
                left_on="date",
                right_on="available_at",
                direction="backward",
                allow_exact_matches=True,
            )
        )
    return pd.concat(merged_groups, ignore_index=True)


def _attach_labels(features: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    _require_columns(labels, {"ticker", "start_date", "end_date", "leader_type", "theme"}, "labels")
    labels = labels.copy()
    labels["start_date"] = _datetime_ns(labels["start_date"])
    labels["end_date"] = _datetime_ns(labels["end_date"])
    features["is_labeled_positive"] = False
    features["label_leader_type"] = pd.Series(pd.NA, index=features.index, dtype="object")
    features["label_theme"] = pd.Series(pd.NA, index=features.index, dtype="object")

    for label in labels.itertuples(index=False):
        mask = (
            (features["ticker"] == label.ticker)
            & (features["date"] >= label.start_date)
            & (features["date"] <= label.end_date)
        )
        if features.loc[mask, "is_labeled_positive"].any():
            raise ValueError(f"Overlapping leader labels detected for {label.ticker}")
        features.loc[mask, "is_labeled_positive"] = True
        features.loc[mask, "label_leader_type"] = label.leader_type
        features.loc[mask, "label_theme"] = label.theme
    return features


def _add_cross_section_features(features: pd.DataFrame, minimum_universe_size: int) -> pd.DataFrame:
    daily_count = features.groupby("date")["ticker"].transform("nunique")
    has_full_universe = daily_count >= minimum_universe_size
    features["amount_rank_market"] = features.groupby("date")["amount"].rank(method="min", ascending=False)
    features.loc[~has_full_universe, "amount_rank_market"] = np.nan

    features["rs_rank_market_20d"] = features.groupby("date")["rs_market_20d"].rank(
        method="average", ascending=False, pct=True
    )
    features.loc[~has_full_universe, "rs_rank_market_20d"] = np.nan
    return features


def _add_coverage_metadata(features: pd.DataFrame) -> pd.DataFrame:
    coverage_groups = {
        "price": ["return_20d", "rolling_high_120d"],
        "amount": ["amount_ratio_20d"],
        "turnover": ["base_turnover_sum_60d"],
        "market_relative": ["rs_market_20d"],
        "chip_proxy": ["chip_concentration_60d", "overhead_supply_ratio"],
        "market_cross_section": ["amount_rank_market", "rs_rank_market_20d"],
        "narrative": ["theme_score", "fundamental_score"],
    }

    def summarize(row: pd.Series) -> tuple[float, str]:
        missing = [name for name, columns in coverage_groups.items() if row[columns].isna().any()]
        coverage = (len(coverage_groups) - len(missing)) / len(coverage_groups)
        return coverage, "|".join(missing)

    summaries = features.apply(summarize, axis=1, result_type="expand")
    features["feature_coverage"] = summaries[0].astype(float)
    features["missing_feature_groups"] = summaries[1]
    return features


def build_daily_features(
    market_prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    narratives: pd.DataFrame,
    labels: pd.DataFrame,
    *,
    minimum_market_universe_size: int = 1_000,
) -> pd.DataFrame:
    """Build deterministic daily features without silently inventing missing cross-sections."""
    _require_columns(market_prices, REQUIRED_MARKET_COLUMNS, "market_prices")
    prices = market_prices.copy()
    prices["date"] = _datetime_ns(prices["date"])
    prices = prices.sort_values(["ticker", "date"])
    if prices.duplicated(["ticker", "date"]).any():
        raise ValueError("market_prices contains duplicate ticker/date rows")

    numeric_columns = ["open", "high", "low", "close", "volume", "amount", "turnover_rate"]
    prices[numeric_columns] = prices[numeric_columns].apply(pd.to_numeric, errors="raise")
    invalid_ohlc = (prices["high"] < prices[["open", "close", "low"]].max(axis=1)) | (
        prices["low"] > prices[["open", "close", "high"]].min(axis=1)
    )
    if invalid_ohlc.any():
        raise ValueError(f"market_prices contains {int(invalid_ohlc.sum())} invalid OHLC rows")
    if (prices[["close", "volume", "amount"]] < 0).any().any():
        raise ValueError("market_prices contains negative price, volume, or amount")

    computed = [_compute_one_stock(group) for _, group in prices.groupby("ticker", sort=False)]
    features = pd.concat(computed, ignore_index=True)
    features = _attach_benchmark(features, benchmark)
    features["rs_industry_20d"] = np.nan
    features["rs_theme_20d"] = np.nan
    features = _add_cross_section_features(features, minimum_market_universe_size)
    features = _attach_narratives(features, narratives)
    features["theme_freshness"] = features["narrative_freshness"]
    features = _attach_labels(features, labels)
    features = _add_coverage_metadata(features)
    features["feature_version"] = FEATURE_VERSION
    return features.sort_values(["date", "ticker"]).reset_index(drop=True)
