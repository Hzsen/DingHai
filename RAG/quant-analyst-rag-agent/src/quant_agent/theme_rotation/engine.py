from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Mapping, Sequence

import pandas as pd

from domain.theme_rotation import (
    AttributionSignal,
    RotationAlert,
    RotationAlertDirection,
    ThemeDefinition,
    ThemeInstrumentKind,
    ThemeMetricSnapshot,
    ThemeReason,
    ThemeRotationConfig,
    ThemeRotationSnapshot,
    ThemeRotationState,
    ThemeTrend,
)
from quant_agent.theme_rotation.catalog import _validate_required_themes


@dataclass(frozen=True, slots=True)
class ThemeRotationEvaluation:
    snapshot: ThemeRotationSnapshot
    dates: tuple[str, ...]
    theme_score_history: dict[str, tuple[float | None, ...]]
    attribution_score_history: dict[str, tuple[float | None, ...]]


def _pct_change(series: pd.Series, periods: int) -> pd.Series:
    return 100.0 * (series / series.shift(periods) - 1.0)


def _cap100(series: pd.Series) -> pd.Series:
    return series.clip(lower=0.0, upper=100.0)


def _score_acceleration(series: pd.Series, scale: float, maximum: float) -> pd.Series:
    return (series.fillna(0.0).clip(lower=0.0) / scale * maximum).clip(upper=maximum)


def _points(condition: pd.Series, value: float) -> pd.Series:
    return condition.fillna(False).astype(float) * value


def _series_values(series: pd.Series) -> tuple[float | None, ...]:
    return tuple(float(value) if pd.notna(value) and math.isfinite(float(value)) else None for value in series)


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _trend_from_code(code: int) -> ThemeTrend:
    return {
        2: ThemeTrend.STRONG,
        1: ThemeTrend.LEAN_STRONG,
        -1: ThemeTrend.LEAN_WEAK,
        -2: ThemeTrend.WEAK,
    }.get(code, ThemeTrend.NEUTRAL)


def _state_from_code(code: int) -> ThemeRotationState:
    return {
        3: ThemeRotationState.CROWDED_UPTREND,
        2: ThemeRotationState.CONFIRMED_ENTRY,
        1: ThemeRotationState.EARLY_ROTATION,
        -1: ThemeRotationState.CAPITAL_EXIT,
        -2: ThemeRotationState.DISTRIBUTION_EXIT,
        9: ThemeRotationState.NO_DATA,
    }.get(code, ThemeRotationState.NEUTRAL_WATCH)


def _state_codes(
    score: pd.Series,
    overextended: pd.Series,
    distribution: pd.Series,
    relative_5d: pd.Series,
    relative_20d: pd.Series,
    config: ThemeRotationConfig,
) -> pd.Series:
    output: list[int] = []
    for values in zip(score, overextended, distribution, relative_5d, relative_20d):
        value, over, dist, rel5, rel20 = values
        if pd.isna(value):
            output.append(9)
        elif bool(dist):
            output.append(-2)
        elif bool(over) and float(value) >= config.early_rotation_threshold:
            output.append(3)
        elif float(value) >= config.confirmed_entry_threshold:
            output.append(2)
        elif float(value) >= config.early_rotation_threshold:
            output.append(1)
        elif float(value) < config.weakening_threshold and pd.notna(rel5) and pd.notna(rel20) and rel5 < 0 and rel20 < 0:
            output.append(-1)
        else:
            output.append(0)
    return pd.Series(output, index=score.index, dtype="int64")


def _prepare_prices(
    prices: pd.DataFrame,
    benchmark_symbol: str,
    symbols: set[str],
    as_of: datetime | None,
    lookback_bars: int,
) -> tuple[pd.DatetimeIndex, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    required = {"date", "symbol", "close", "volume"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"theme rotation prices missing columns: {sorted(missing)}")
    frame = prices.loc[:, ["date", "symbol", "close", "volume"]].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["date", "symbol", "close"])
    frame = frame.loc[(frame["close"] > 0) & (frame["volume"].fillna(0) >= 0)]
    if as_of is not None:
        cutoff = pd.Timestamp(as_of.date())
        frame = frame.loc[frame["date"] <= cutoff]
    if frame.empty:
        raise ValueError("theme rotation prices contain no usable observations")
    frame = frame.sort_values(["date", "symbol"]).drop_duplicates(["date", "symbol"], keep="last")
    close_raw = frame.pivot(index="date", columns="symbol", values="close").sort_index()
    volume_raw = frame.pivot(index="date", columns="symbol", values="volume").sort_index()
    if benchmark_symbol not in close_raw.columns:
        raise ValueError(f"benchmark {benchmark_symbol} is absent from price input")
    calendar = close_raw.index[close_raw[benchmark_symbol].notna()][-lookback_bars:]
    if calendar.empty:
        raise ValueError("benchmark has no usable observations")
    ordered_symbols = sorted(symbols | {benchmark_symbol})
    close_raw = close_raw.reindex(index=calendar, columns=ordered_symbols)
    volume_raw = volume_raw.reindex(index=calendar, columns=ordered_symbols)
    close = close_raw.ffill()
    volume = volume_raw.ffill()
    return calendar, close_raw, close, volume


def _theme_series(
    definition: ThemeDefinition,
    close: pd.DataFrame,
    volume: pd.DataFrame,
    trend_length: int,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    members = list(definition.members)
    if definition.kind is ThemeInstrumentKind.ETF:
        index = close[definition.proxy_symbol]
        dollar_volume = index * volume[definition.proxy_symbol]
        moving_average = index.rolling(trend_length, min_periods=trend_length).mean()
        breadth = pd.Series(pd.NA, index=index.index, dtype="Float64")
        available = index.notna() & moving_average.notna()
        breadth.loc[available] = (index.loc[available] > moving_average.loc[available]).map({True: 70.0, False: 30.0})
        return index.astype(float), dollar_volume.astype(float).ffill(), breadth.astype(float)

    member_close = close[members]
    member_returns = member_close / member_close.shift(1) - 1.0
    equal_weight_return = member_returns.mean(axis=1, skipna=True).fillna(0.0)
    index = (1.0 + equal_weight_return).cumprod() * 100.0
    dollar_volume = (member_close * volume[members]).mean(axis=1, skipna=True).ffill()
    moving_average = member_close.rolling(trend_length, min_periods=trend_length).mean()
    available_count = member_close.notna().sum(axis=1)
    active_count = ((member_close > moving_average) & member_close.notna()).sum(axis=1)
    breadth = 100.0 * active_count / available_count.where(available_count > 0)
    return index.astype(float), dollar_volume.astype(float), breadth.astype(float)


def _calculate_metrics(
    index: pd.Series,
    dollar_volume: pd.Series,
    breadth: pd.Series,
    benchmark: pd.Series,
    config: ThemeRotationConfig,
) -> pd.DataFrame:
    ratio = index / benchmark
    relative_1d = _pct_change(ratio, 1)
    relative_5d = _pct_change(ratio, 5)
    relative_20d = _pct_change(ratio, 20)
    relative_60d = _pct_change(ratio, 60)
    absolute_5d = _pct_change(index, 5)
    ma20 = ratio.rolling(config.trend_length, min_periods=config.trend_length).mean()
    ma50 = ratio.rolling(config.slow_length, min_periods=config.slow_length).mean()
    volume_fixed = dollar_volume.ffill()
    volume_ratio = volume_fixed / volume_fixed.rolling(
        config.volume_length, min_periods=config.volume_length
    ).mean()
    distance_50 = 100.0 * (ratio / ma50 - 1.0)
    valid = ratio.notna() & benchmark.notna()

    trend_component = (
        _points(ratio > ma20, 10.0)
        + _points(ratio > ma50, 10.0)
        + _points(ma20 > ma20.shift(5), 10.0)
    )
    flow_component = (
        _points(relative_5d > 0, 10.0)
        + _points(relative_20d > 0, 8.0)
        + _points(relative_5d > relative_5d.shift(5), 8.0)
        + _points(relative_5d > relative_20d / 4.0, 8.0)
    )
    volume_component = pd.Series(0.0, index=ratio.index)
    volume_component.loc[(relative_1d > 0) & (volume_ratio >= 1.0)] = 10.0
    volume_component.loc[(relative_1d > 0) & (volume_ratio >= config.volume_confirmation)] = 20.0
    breadth_component = pd.Series(0.0, index=ratio.index)
    breadth_component.loc[breadth >= 50.0] = 8.0
    breadth_component.loc[breadth >= 70.0] = 15.0
    overextended = (
        ((relative_60d > config.crowded_relative_60d) & relative_60d.notna())
        | ((distance_50 > config.crowded_distance_50d) & distance_50.notna())
    )
    crowd_component = (~overextended).astype(float) * 8.0
    raw_score = trend_component + flow_component + volume_component + breadth_component + crowd_component
    base_score = _cap100(raw_score).where(valid)
    down_day = index < index.shift(1)
    down_volume_pressure = valid & down_day & (volume_ratio >= config.volume_confirmation)
    beta_lift = valid & (absolute_5d > 0) & (relative_5d < 0)
    distribution = valid & (ratio < ma20) & (relative_5d < 0) & (volume_ratio > config.volume_confirmation) & (relative_1d < 0)

    trend_code = pd.Series(0, index=ratio.index, dtype="int64")
    trend_code.loc[(ratio > ma20) & (ratio > ma50)] = 2
    trend_code.loc[(ratio > ma20) & ~(ratio > ma50)] = 1
    trend_code.loc[(ratio < ma20) & (ratio < ma50)] = -2
    trend_code.loc[(ratio < ma20) & ~(ratio < ma50)] = -1
    return pd.DataFrame({
        "index": index,
        "ratio": ratio,
        "relative_1d": relative_1d,
        "relative_5d": relative_5d,
        "relative_20d": relative_20d,
        "relative_60d": relative_60d,
        "absolute_5d": absolute_5d,
        "acceleration": relative_5d - relative_20d / 4.0,
        "volume_ratio": volume_ratio,
        "breadth": breadth,
        "distance_50": distance_50,
        "trend_component": trend_component,
        "flow_component": flow_component,
        "volume_component": volume_component,
        "breadth_component": breadth_component,
        "crowd_component": crowd_component,
        "base_score": base_score,
        "overextended": overextended,
        "down_volume_pressure": down_volume_pressure,
        "beta_lift": beta_lift,
        "distribution": distribution,
        "trend_code": trend_code,
    })


def _pair_acceleration(left: pd.Series, right: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    ratio = left / right
    relative_5d = _pct_change(ratio, 5)
    relative_20d = _pct_change(ratio, 20)
    return relative_5d, relative_20d, relative_5d - relative_20d / 4.0


def _theme_reason(
    theme_id: str,
    state_code: int,
    score: float | None,
    relative_5d: float | None,
    relative_20d: float | None,
    absolute_5d: float | None,
    beta_lift: bool,
    segment_diffusion: float,
    attributions: Mapping[str, float],
    config: ThemeRotationConfig,
) -> ThemeReason:
    if state_code == 9 or score is None:
        return ThemeReason.NO_DATA
    if state_code == -2:
        return ThemeReason.DISTRIBUTION_EXIT
    if beta_lift and score < config.early_rotation_threshold:
        return ThemeReason.BETA_LIFT_FALSE_STRENGTH
    if attributions["internal_de_risking"] >= 70 and (absolute_5d or 0) < 0 and (relative_5d or 0) < 0:
        return ThemeReason.INTERNAL_DE_RISKING
    if theme_id == "software" and attributions["software_semiconductor_rotation"] >= 60:
        return ThemeReason.SOFTWARE_SEMICONDUCTOR_ROTATION
    if theme_id == "ai_apps" and attributions["ai_application_catchup"] >= 60:
        return ThemeReason.AI_APPLICATION_CATCHUP
    if theme_id in {"optical_components", "ai_network", "memory_storage", "data_center_infra"} and segment_diffusion >= 60 and score >= config.early_rotation_threshold:
        return ThemeReason.AI_CHAIN_DIFFUSION
    if theme_id == "hyperscalers" and attributions["hyperscaler_handoff"] >= 60:
        return ThemeReason.HYPERSCALER_HANDOFF
    if attributions["month_quarter_rebalance"] >= 65 and state_code <= 1:
        return ThemeReason.MONTH_QUARTER_REBALANCE
    if theme_id in {"semiconductor", "ai_hardware"} and attributions["hardware_crowding_unwind"] >= 60 and state_code <= 0:
        return ThemeReason.CROWDING_UNWIND
    if state_code == 3:
        return ThemeReason.CROWDED_UPTREND
    if score >= config.confirmed_entry_threshold:
        return ThemeReason.ACTIVE_INFLOW
    if score >= config.early_rotation_threshold:
        return ThemeReason.EARLY_ROTATION
    if (absolute_5d or 0) < 0 and (relative_5d or 0) > 0:
        return ThemeReason.RELATIVE_RESILIENCE
    if (relative_5d or 0) < 0 and (relative_20d or 0) < 0:
        return ThemeReason.DISTRIBUTION_EXIT
    return ThemeReason.NEUTRAL


def _cross_alert(
    alert_id: str,
    label: str,
    series: pd.Series,
    threshold: float,
    direction: RotationAlertDirection,
) -> RotationAlert | None:
    if len(series) < 2 or pd.isna(series.iloc[-1]) or pd.isna(series.iloc[-2]):
        return None
    previous, current = float(series.iloc[-2]), float(series.iloc[-1])
    crossed = (
        current > threshold and previous <= threshold
        if direction is RotationAlertDirection.CROSS_ABOVE
        else current < threshold and previous >= threshold
    )
    if not crossed:
        return None
    return RotationAlert(alert_id, label, direction, threshold, previous, current)


def evaluate_theme_rotation(
    prices: pd.DataFrame,
    definitions: Sequence[ThemeDefinition],
    config: ThemeRotationConfig | None = None,
    *,
    as_of: datetime | None = None,
    selected_theme_id: str = "semiconductor",
) -> ThemeRotationEvaluation:
    config = config or ThemeRotationConfig()
    themes = tuple(definitions)
    _validate_required_themes(themes)
    by_id = {item.theme_id: item for item in themes}
    if selected_theme_id not in by_id:
        raise ValueError(f"unknown selected theme: {selected_theme_id}")
    symbols = {symbol for item in themes for symbol in item.members}
    calendar, close_raw, close, volume = _prepare_prices(
        prices, config.benchmark_symbol, symbols, as_of, config.lookback_bars
    )
    benchmark = close[config.benchmark_symbol]
    metric_frames: dict[str, pd.DataFrame] = {}
    live_members: dict[str, int] = {}
    coverage: dict[str, float] = {}
    for definition in themes:
        index, dollar_volume, breadth = _theme_series(definition, close, volume, config.trend_length)
        metric_frames[definition.theme_id] = _calculate_metrics(index, dollar_volume, breadth, benchmark, config)
        present = int(close_raw.loc[calendar[-1], list(definition.members)].notna().sum())
        live_members[definition.theme_id] = present
        coverage[definition.theme_id] = present / len(definition.members)

    m = metric_frames
    soft_semi_5, _, soft_semi_accel = _pair_acceleration(m["software"]["index"], m["semiconductor"]["index"])
    app_soft_5, _, app_soft_accel = _pair_acceleration(m["ai_apps"]["index"], m["software"]["index"])
    _, _, optical_semi_accel = _pair_acceleration(m["optical_components"]["index"], m["semiconductor"]["index"])
    _, _, network_semi_accel = _pair_acceleration(m["ai_network"]["index"], m["semiconductor"]["index"])
    _, _, memory_semi_accel = _pair_acceleration(m["memory_storage"]["index"], m["semiconductor"]["index"])
    _, _, infra_semi_accel = _pair_acceleration(m["data_center_infra"]["index"], m["semiconductor"]["index"])
    hyperscaler_hardware_5, _, hyperscaler_hardware_accel = _pair_acceleration(
        m["hyperscalers"]["index"], m["ai_hardware"]["index"]
    )

    pair_score = _cap100(
        _score_acceleration(-m["semiconductor"]["acceleration"], 3.0, 25.0)
        + _score_acceleration(-m["ai_hardware"]["acceleration"], 3.0, 20.0)
        + _score_acceleration(m["software"]["acceleration"], 3.0, 25.0)
        + _score_acceleration(soft_semi_accel, 2.0, 20.0)
        + _points(m["software"]["relative_5d"] > m["semiconductor"]["relative_5d"], 10.0)
        + _points(m["software"]["relative_5d"] > 0, 5.0)
    )
    app_catchup = _cap100(
        _score_acceleration(app_soft_accel, 2.0, 35.0)
        + _points(m["ai_apps"]["relative_5d"] > m["software"]["relative_5d"], 15.0)
        + _points(m["ai_apps"]["base_score"] >= 60, 20.0)
        + _points((m["software"]["relative_5d"] < 0) & (m["ai_apps"]["relative_5d"] > 0), 15.0)
    )

    def diffusion_score(theme_id: str, acceleration: pd.Series) -> pd.Series:
        frame = m[theme_id]
        breadth_points = pd.Series(0.0, index=calendar)
        breadth_points.loc[frame["breadth"] >= 50] = 8.0
        breadth_points.loc[frame["breadth"] >= 70] = 15.0
        return _cap100(
            _score_acceleration(acceleration, 2.0, 30.0)
            + _points(frame["relative_5d"] > 0, 15.0)
            + _points(frame["base_score"] >= 60, 20.0)
            + breadth_points
            + _points(m["semiconductor"]["overextended"] | m["ai_hardware"]["overextended"], 10.0)
        )

    memory_diffusion = diffusion_score("memory_storage", memory_semi_accel)
    network_diffusion = diffusion_score("ai_network", network_semi_accel)
    optical_diffusion = diffusion_score("optical_components", optical_semi_accel)
    power_diffusion = diffusion_score("data_center_infra", infra_semi_accel)
    overall_diffusion = pd.concat(
        [memory_diffusion, network_diffusion, optical_diffusion, power_diffusion], axis=1
    ).max(axis=1)
    crowding_unwind = _cap100(
        _points(m["semiconductor"]["overextended"] | m["ai_hardware"]["overextended"], 25.0)
        + _score_acceleration(-m["semiconductor"]["acceleration"], 3.0, 25.0)
        + _score_acceleration(-m["ai_hardware"]["acceleration"], 3.0, 25.0)
        + _points((m["semiconductor"]["relative_5d"] < 0) | (m["ai_hardware"]["relative_5d"] < 0), 15.0)
        + _points(m["semiconductor"]["down_volume_pressure"] | m["ai_hardware"]["down_volume_pressure"], 15.0)
    )
    buyer_shift = _cap100(
        _score_acceleration(hyperscaler_hardware_accel, 2.0, 35.0)
        + _score_acceleration(m["hyperscalers"]["acceleration"], 3.0, 20.0)
        + _score_acceleration(-m["ai_hardware"]["acceleration"], 3.0, 15.0)
        + _points(m["hyperscalers"]["relative_5d"] > m["ai_hardware"]["relative_5d"], 15.0)
        + _points(m["hyperscalers"]["base_score"] >= 60, 15.0)
    )
    weak_count = sum((m[item.theme_id]["relative_5d"] < 0).astype(int) for item in themes)
    absolute_down_count = sum((m[item.theme_id]["absolute_5d"] < 0).astype(int) for item in themes)
    internal_de_risk = pd.Series(0.0, index=calendar)
    internal_de_risk += _points(weak_count >= 6, 10.0)
    internal_de_risk.loc[weak_count >= 8] = 22.0
    internal_de_risk.loc[weak_count >= 10] = 35.0
    internal_de_risk += _points(absolute_down_count >= 8, 15.0)
    internal_de_risk += _points(absolute_down_count >= 10, 10.0)
    internal_de_risk += _points(
        (m["cybersecurity"]["relative_5d"] > m["semiconductor"]["relative_5d"])
        & (m["cybersecurity"]["relative_5d"] > m["ai_hardware"]["relative_5d"]),
        10.0,
    )
    internal_de_risk += _points(m["hyperscalers"]["relative_5d"] > m["ai_hardware"]["relative_5d"], 5.0)
    internal_de_risk = _cap100(internal_de_risk)

    winner_ids = ("semiconductor", "ai_hardware", "memory_storage", "ai_network", "optical_components", "data_center_infra")
    laggard_ids = ("software", "cloud", "cybersecurity", "robotics", "hyperscalers")
    winner_deceleration_count = sum(
        ((m[key]["relative_20d"] > 5) & (m[key]["acceleration"] < 0)).astype(int) for key in winner_ids
    )
    laggard_recovery_count = sum(
        ((m[key]["relative_20d"] < 2) & (m[key]["acceleration"] > 0)).astype(int) for key in laggard_ids
    )
    month_turn = pd.Series((calendar.day >= 24) | (calendar.day <= 3), index=calendar)
    quarter_turn = pd.Series(
        (calendar.month.isin([3, 6, 9, 12]) & (calendar.day >= 20))
        | (calendar.month.isin([1, 4, 7, 10]) & (calendar.day <= 3)),
        index=calendar,
    )
    rebalance = _points(month_turn, 20.0) + _points(quarter_turn, 25.0)
    rebalance += _points(winner_deceleration_count == 1, 10.0)
    rebalance += _points(winner_deceleration_count == 2, 18.0)
    rebalance += _points(winner_deceleration_count >= 3, 25.0)
    rebalance += _points(laggard_recovery_count == 1, 10.0)
    rebalance += _points(laggard_recovery_count == 2, 18.0)
    rebalance += _points(laggard_recovery_count >= 3, 25.0)
    rebalance += _points(pair_score >= 45, 10.0)
    rebalance = _cap100(rebalance)

    attribution_histories = {
        "software_semiconductor_rotation": pair_score,
        "ai_application_catchup": app_catchup,
        "memory_diffusion": memory_diffusion,
        "network_diffusion": network_diffusion,
        "optical_diffusion": optical_diffusion,
        "power_cooling_diffusion": power_diffusion,
        "month_quarter_rebalance": rebalance,
        "hardware_crowding_unwind": crowding_unwind,
        "hyperscaler_handoff": buyer_shift,
        "internal_de_risking": internal_de_risk,
    }
    boosts: dict[str, pd.Series] = {key: pd.Series(0.0, index=calendar) for key in by_id}
    boosts["software"] = _points(pair_score >= 60, 10.0) + _points((pair_score >= 45) & (pair_score < 60), 5.0)
    boosts["cybersecurity"] = _points(internal_de_risk >= 60, 5.0)
    boosts["ai_broad"] = _points(app_catchup >= 60, 8.0) + _points(
        (app_catchup < 60) & (overall_diffusion >= 60), 6.0
    ) + _points((app_catchup < 60) & (overall_diffusion >= 45) & (overall_diffusion < 60), 3.0)
    boosts["ai_apps"] = _points(app_catchup >= 60, 12.0) + _points((app_catchup >= 45) & (app_catchup < 60), 6.0)
    boosts["optical_components"] = _points(optical_diffusion >= 60, 10.0) + _points((optical_diffusion >= 45) & (optical_diffusion < 60), 5.0)
    boosts["ai_network"] = _points(network_diffusion >= 60, 10.0) + _points((network_diffusion >= 45) & (network_diffusion < 60), 5.0)
    boosts["memory_storage"] = _points(memory_diffusion >= 60, 12.0) + _points((memory_diffusion >= 45) & (memory_diffusion < 60), 6.0)
    boosts["data_center_infra"] = _points(power_diffusion >= 60, 10.0) + _points((power_diffusion >= 45) & (power_diffusion < 60), 5.0)
    boosts["hyperscalers"] = _points(buyer_shift >= 60, 10.0) + _points((buyer_shift >= 45) & (buyer_shift < 60), 5.0)
    boosts["ai_hardware"] = -_points(crowding_unwind >= 60, 8.0) - _points((crowding_unwind >= 45) & (crowding_unwind < 60), 4.0)

    final_scores: dict[str, pd.Series] = {}
    final_state_codes: dict[str, pd.Series] = {}
    for theme_id, frame in m.items():
        final_scores[theme_id] = _cap100(frame["base_score"] + boosts[theme_id]).where(frame["base_score"].notna())
        final_state_codes[theme_id] = _state_codes(
            final_scores[theme_id], frame["overextended"], frame["distribution"],
            frame["relative_5d"], frame["relative_20d"], config,
        )

    current_attr = {key: float(value.iloc[-1]) for key, value in attribution_histories.items()}
    segment_scores = {
        "optical_components": current_attr["optical_diffusion"],
        "ai_network": current_attr["network_diffusion"],
        "memory_storage": current_attr["memory_diffusion"],
        "data_center_infra": current_attr["power_cooling_diffusion"],
    }
    metric_snapshots: list[ThemeMetricSnapshot] = []
    for definition in themes:
        frame = m[definition.theme_id]
        row = frame.iloc[-1]
        score = _optional_float(final_scores[definition.theme_id].iloc[-1])
        state_code = int(final_state_codes[definition.theme_id].iloc[-1])
        reason = _theme_reason(
            definition.theme_id, state_code, score,
            _optional_float(row["relative_5d"]), _optional_float(row["relative_20d"]),
            _optional_float(row["absolute_5d"]), bool(row["beta_lift"]),
            segment_scores.get(definition.theme_id, 0.0), current_attr,
            config,
        )
        metric_snapshots.append(ThemeMetricSnapshot(
            theme_id=definition.theme_id, label=definition.label, proxy_symbol=definition.proxy_symbol,
            kind=definition.kind, members=definition.members,
            relative_1d=_optional_float(row["relative_1d"]),
            relative_5d=_optional_float(row["relative_5d"]),
            relative_20d=_optional_float(row["relative_20d"]),
            relative_60d=_optional_float(row["relative_60d"]),
            absolute_5d=_optional_float(row["absolute_5d"]),
            acceleration=_optional_float(row["acceleration"]),
            volume_ratio=_optional_float(row["volume_ratio"]),
            breadth=_optional_float(row["breadth"]),
            distance_from_relative_ma50=_optional_float(row["distance_50"]),
            trend=_trend_from_code(int(row["trend_code"])),
            base_score=_optional_float(row["base_score"]), score=score,
            state=_state_from_code(state_code), reason=reason,
            overextended=bool(row["overextended"]),
            down_volume_pressure=bool(row["down_volume_pressure"]), beta_lift=bool(row["beta_lift"]),
            distribution=bool(row["distribution"]), live_members=live_members[definition.theme_id],
            member_count=len(definition.members), data_coverage=coverage[definition.theme_id],
            score_components={
                "trend": float(row["trend_component"]), "flow": float(row["flow_component"]),
                "volume": float(row["volume_component"]), "breadth": float(row["breadth_component"]),
                "not_overheated": float(row["crowd_component"]),
                "attribution_boost": float(boosts[definition.theme_id].iloc[-1]),
            },
        ))

    latest = lambda series: _optional_float(series.iloc[-1]) or 0.0
    attributions = (
        AttributionSignal("software_semiconductor_rotation", "软件/半导体 强弱对调", latest(pair_score), "强者减速、软件回血", "半导体重新提速或软件5日相对转负"),
        AttributionSignal("ai_application_catchup", "AI应用追赶", latest(app_catchup), f"应用对软件提速 {latest(app_soft_accel):+.2f}%", "应用对软件的比价提速转负"),
        AttributionSignal("memory_diffusion", "储存扩散", latest(memory_diffusion), f"储存对半导体提速 {latest(memory_semi_accel):+.2f}%", "提速转负或广度跌破50%"),
        AttributionSignal("network_diffusion", "AI网络扩散", latest(network_diffusion), f"网络对半导体提速 {latest(network_semi_accel):+.2f}%", "提速转负或广度跌破50%"),
        AttributionSignal("optical_diffusion", "光通信扩散", latest(optical_diffusion), f"光通信对半导体提速 {latest(optical_semi_accel):+.2f}%", "提速转负或广度跌破50%"),
        AttributionSignal("power_cooling_diffusion", "电力散热扩散", latest(power_diffusion), f"基建对半导体提速 {latest(infra_semi_accel):+.2f}%", "提速转负或广度跌破50%"),
        AttributionSignal("month_quarter_rebalance", "月/季末再平衡", latest(rebalance), f"强者减速 {int(winner_deceleration_count.iloc[-1])} 个 / 弱者回血 {int(laggard_recovery_count.iloc[-1])} 个", "月季末窗口结束"),
        AttributionSignal("hardware_crowding_unwind", "硬件拥挤出清", latest(crowding_unwind), "半导体和硬件拥挤/减速", "减速收敛且量能恢复"),
        AttributionSignal("hyperscaler_handoff", "云巨头接棒", latest(buyer_shift), f"买方对卖方提速 {latest(hyperscaler_hardware_accel):+.2f}%", "买方对卖方比价提速转负"),
        AttributionSignal("internal_de_risking", "科技内部降风险", latest(internal_de_risk), f"转弱主题 {int(weak_count.iloc[-1])}/{len(themes)}", "转弱主题数回到6个以下"),
    )

    alerts: list[RotationAlert] = []
    selected = final_scores[selected_theme_id]
    alert_specs = [
        ("selected_early_rotation", "所选主题进入早期轮动", selected, config.early_rotation_threshold, RotationAlertDirection.CROSS_ABOVE),
        ("selected_confirmed_entry", "所选主题确认进入", selected, config.confirmed_entry_threshold, RotationAlertDirection.CROSS_ABOVE),
        ("selected_weakening", "所选主题转弱", selected, config.weakening_threshold, RotationAlertDirection.CROSS_BELOW),
        ("ai_chain_diffusion", "AI链条扩散", overall_diffusion, 60.0, RotationAlertDirection.CROSS_ABOVE),
    ]
    labels = {item.attribution_id: item.label for item in attributions}
    alert_specs.extend(
        (key, labels[key], series, 60.0, RotationAlertDirection.CROSS_ABOVE)
        for key, series in attribution_histories.items()
    )
    for spec in alert_specs:
        alert = _cross_alert(*spec)
        if alert is not None:
            alerts.append(alert)

    global_coverage = sum(coverage.values()) / len(coverage)
    quality_flags: list[str] = []
    if len(calendar) < max(61, config.slow_length + 6):
        quality_flags.append("INSUFFICIENT_HISTORY_FOR_ALL_WINDOWS")
    if any(value < 1 for value in coverage.values()):
        quality_flags.append("PARTIAL_THEME_MEMBER_COVERAGE")
    if len(calendar) < 2:
        quality_flags.append("INSUFFICIENT_ALERT_HISTORY")
    observation_as_of = datetime.combine(calendar[-1].date(), time(21, 0), tzinfo=timezone.utc)
    digest = hashlib.sha256(
        f"{observation_as_of.isoformat()}|{config.model_version}|{config.benchmark_symbol}".encode("utf-8")
    ).hexdigest()[:12]
    snapshot = ThemeRotationSnapshot(
        snapshot_id=f"theme-rotation/{calendar[-1].date().isoformat()}/{digest}",
        as_of=observation_as_of, valid_until=observation_as_of + timedelta(days=1),
        benchmark_symbol=config.benchmark_symbol, selected_theme_id=selected_theme_id,
        themes=tuple(metric_snapshots), attributions=attributions, alerts=tuple(alerts),
        data_coverage=global_coverage, quality_flags=tuple(quality_flags),
        model_version=config.model_version, reference_model_version=config.reference_model_version,
    )
    return ThemeRotationEvaluation(
        snapshot=snapshot,
        dates=tuple(item.date().isoformat() for item in calendar),
        theme_score_history={key: _series_values(value) for key, value in final_scores.items()},
        attribution_score_history={key: _series_values(value) for key, value in attribution_histories.items()},
    )
