from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from domain.macro import SeriesFeature


def _value_at_or_before(frame: pd.DataFrame, target: pd.Timestamp) -> float | None:
    rows = frame.loc[frame["observation_date"] <= target]
    return None if rows.empty else float(rows.iloc[-1]["value"])


def _delta(frame: pd.DataFrame, as_of: pd.Timestamp, days: int) -> float | None:
    current = _value_at_or_before(frame, as_of)
    previous = _value_at_or_before(frame, as_of - pd.Timedelta(days=days))
    return None if current is None or previous is None else current - previous


def compute_macro_features(observations: pd.DataFrame, as_of: datetime) -> dict[str, SeriesFeature]:
    """Build point-in-time features using only rows available by ``as_of``."""
    required = {"series_id", "observation_date", "available_at", "value", "unit", "source", "is_realtime"}
    missing = required - set(observations.columns)
    if missing:
        raise ValueError(f"macro observations missing columns: {sorted(missing)}")
    frame = observations.copy()
    frame["observation_date"] = pd.to_datetime(frame["observation_date"], utc=True)
    frame["available_at"] = pd.to_datetime(frame["available_at"], utc=True)
    as_of_ts = pd.Timestamp(as_of)
    as_of_ts = as_of_ts.tz_localize("UTC") if as_of_ts.tzinfo is None else as_of_ts.tz_convert("UTC")
    frame = frame.loc[frame["available_at"] <= as_of_ts].sort_values(["series_id", "observation_date", "available_at"])
    result: dict[str, SeriesFeature] = {}
    for series_id, group in frame.groupby("series_id", sort=True):
        group = group.drop_duplicates("observation_date", keep="last")
        latest = group.iloc[-1]
        delta5_history = group.set_index("observation_date")["value"].diff(5).dropna()
        trailing = delta5_history.tail(252)
        current_delta5 = _delta(group, as_of_ts, 5)
        z5: float | None = None
        if current_delta5 is not None and len(trailing) >= 20 and float(trailing.std(ddof=0)) > 0:
            z5 = float((current_delta5 - trailing.mean()) / trailing.std(ddof=0))
        values_5y = group.loc[group["observation_date"] >= as_of_ts - pd.Timedelta(days=365 * 5), "value"]
        percentile = None
        if len(values_5y) >= 20:
            percentile = float((values_5y <= float(latest["value"])).mean())
        stale_days = max(0, int((as_of_ts.normalize() - latest["observation_date"].normalize()).days))
        flags: list[str] = []
        if stale_days > (3 if bool(latest["is_realtime"]) else 8):
            flags.append("STALE_SERIES")
        result[str(series_id)] = SeriesFeature(
            series_id=str(series_id), as_of=as_of, value=float(latest["value"]), unit=str(latest["unit"]),
            source=str(latest["source"]), observation_date=latest["observation_date"].isoformat(),
            available_at=latest["available_at"].isoformat(), is_realtime=bool(latest["is_realtime"]),
            stale_days=stale_days, delta_1d=_delta(group, as_of_ts, 1), delta_5d=current_delta5,
            delta_20d=_delta(group, as_of_ts, 20), percentile_5y=percentile,
            z_change_5d_252=z5, quality_flags=tuple(flags),
        )
    return result


def derived_ratio_feature(
    series_id: str,
    numerator: SeriesFeature | None,
    denominator: SeriesFeature | None,
    as_of: datetime,
) -> SeriesFeature | None:
    if numerator is None or denominator is None or denominator.value == 0:
        return None
    def ratio_delta(name: str) -> float | None:
        n_delta = getattr(numerator, name)
        d_delta = getattr(denominator, name)
        if n_delta is None or d_delta is None:
            return None
        n_previous = numerator.value - n_delta
        d_previous = denominator.value - d_delta
        return None if d_previous == 0 else numerator.value / denominator.value - n_previous / d_previous
    return SeriesFeature(
        series_id=series_id, as_of=as_of, value=numerator.value / denominator.value, unit="ratio",
        source=f"derived:{numerator.series_id}/{denominator.series_id}",
        observation_date=max(numerator.observation_date, denominator.observation_date),
        available_at=max(numerator.available_at, denominator.available_at),
        is_realtime=numerator.is_realtime and denominator.is_realtime,
        stale_days=max(numerator.stale_days, denominator.stale_days), delta_1d=ratio_delta("delta_1d"),
        delta_5d=ratio_delta("delta_5d"), delta_20d=ratio_delta("delta_20d"),
        percentile_5y=None, z_change_5d_252=None,
        quality_flags=tuple(sorted(set(numerator.quality_flags + denominator.quality_flags))),
    )

