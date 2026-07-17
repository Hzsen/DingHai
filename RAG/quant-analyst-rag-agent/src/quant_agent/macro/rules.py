from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

from domain.macro import (
    AssetStance,
    InflationQuadrant,
    LiquidityFlowState,
    LiquiditySourceFlow,
    LiquidityState,
    LiquidityTargetFlow,
    MacroRegime,
    MacroSnapshot,
    RatePressureState,
    RiskState,
    SeriesFeature,
    Stance,
    StanceHorizon,
)


MACRO_MODEL_VERSION = "macro-regime-v1.1.0"
CORE_SERIES = ("DFII10", "DGS10", "DGS30", "DGS2", "T10YIE", "DXY", "BAMLC0A0CM", "VIX", "VIX3M", "MOVE")


def _first(features: dict[str, SeriesFeature], *series_ids: str) -> SeriesFeature | None:
    """Return the first available canonical series or documented proxy."""
    return next((features[series_id] for series_id in series_ids if series_id in features), None)


def _clip(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _bp(feature: SeriesFeature | None, field: str) -> float | None:
    value = getattr(feature, field) if feature is not None else None
    return None if value is None else float(value) * 100.0


def _return(feature: SeriesFeature | None, field: str) -> float | None:
    if feature is None:
        return None
    delta = getattr(feature, field)
    if delta is None:
        return None
    previous = feature.value - float(delta)
    return None if previous == 0 else float(delta) / previous


def _millions_usd_delta(feature: SeriesFeature | None, field: str) -> float | None:
    """Normalize balance-sheet flow deltas before combining them."""
    if feature is None:
        return None
    value = getattr(feature, field)
    if value is None:
        return None
    if feature.unit == "millions_usd":
        return float(value)
    if feature.unit == "billions_usd":
        return float(value) * 1_000.0
    return None


def _rate_pressure(features: dict[str, SeriesFeature]) -> tuple[float, RatePressureState, list[str], list[str]]:
    real10 = features.get("DFII10")
    nominal10 = features.get("DGS10")
    nominal30 = features.get("DGS30")
    gld = features.get("GLD")
    score = 0.0
    drivers: list[str] = []
    conflicts: list[str] = []
    if real10:
        if real10.percentile_5y is not None and real10.percentile_5y >= 0.90:
            score += 20; drivers.append("10Y_REAL_YIELD_HIGH_PERCENTILE")
        delta5 = _bp(real10, "delta_5d")
        delta20 = _bp(real10, "delta_20d")
        if delta5 is not None and delta5 >= 5:
            score += 15; drivers.append("10Y_REAL_YIELD_RISING_5D")
        elif delta5 is not None and delta5 <= -5:
            score -= 10; conflicts.append("10Y_REAL_YIELD_FALLING_5D")
        if delta20 is not None and delta20 >= 10:
            score += 15; drivers.append("10Y_REAL_YIELD_RISING_20D")
        if real10.z_change_5d_252 is not None and real10.z_change_5d_252 >= 0.7:
            score += 15; drivers.append("REAL_YIELD_CHANGE_STATISTICALLY_LARGE")
    n10_5 = _bp(nominal10, "delta_5d")
    n30_5 = _bp(nominal30, "delta_5d")
    if n10_5 is not None and n10_5 >= 5:
        score += 10; drivers.append("10Y_NOMINAL_RISING")
    if n30_5 is not None and n10_5 is not None and n30_5 > n10_5 and n30_5 >= 5:
        score += 10; drivers.append("LONG_END_TERM_PRESSURE")
    if _return(gld, "delta_5d") is not None and _return(gld, "delta_5d") < -0.02:
        score += 10; drivers.append("GOLD_WEAKNESS_CONFIRMS_REAL_RATE_PRESSURE")
    score = _clip(score)
    state = (
        RatePressureState.LOW if score < 30 else RatePressureState.NEUTRAL if score < 50
        else RatePressureState.PRESSURE_BUILDING if score < 70
        else RatePressureState.SUSTAINED_PRESSURE if score < 85 else RatePressureState.EXTREME_PRESSURE
    )
    return score, state, drivers, conflicts


def _inflation_quadrant(features: dict[str, SeriesFeature]) -> tuple[InflationQuadrant, list[str]]:
    real = features.get("DFII10")
    bei = features.get("T10YIE")
    nominal = features.get("DGS10")
    flags: list[str] = []
    real5, bei5 = _bp(real, "delta_5d"), _bp(bei, "delta_5d")
    if real5 is None or bei5 is None:
        quadrant = InflationQuadrant.INSUFFICIENT_DATA
    elif real5 > 0 and bei5 <= 0:
        quadrant = InflationQuadrant.REAL_RATE_SHOCK
    elif real5 > 0 and bei5 > 0:
        quadrant = InflationQuadrant.MIXED_INFLATION_PRESSURE
    elif real5 <= 0 and bei5 > 0:
        quadrant = InflationQuadrant.REFLATION
    else:
        quadrant = InflationQuadrant.DISINFLATION_OR_GROWTH_SCARE
    n1, r1, b1 = _bp(nominal, "delta_1d"), _bp(real, "delta_1d"), _bp(bei, "delta_1d")
    if None not in (n1, r1, b1) and abs(float(n1) - float(r1) - float(b1)) > 3:
        flags.append("ASYNCHRONOUS_RATE_DECOMPOSITION")
    return quadrant, flags


def _risk(features: dict[str, SeriesFeature]) -> tuple[float, RiskState, list[str], list[str]]:
    credit = features.get("BAMLC0A0CM")
    vix = features.get("VIX")
    vix3m = features.get("VIX3M")
    move = features.get("MOVE")
    dxy = _first(features, "DXY", "DXY_PROXY", "UUP")
    iwm_spy = features.get("IWM_SPY")
    kre_spy = features.get("KRE_SPY")
    drivers: list[str] = []
    offsets: list[str] = []
    credit_component = 0.0
    if credit:
        credit_component = _clip((credit.value - 0.55) / 1.0 * 100)
        if credit.value >= 1.0: drivers.append("CREDIT_SPREAD_ELEVATED")
        elif credit.value <= 0.8: offsets.append("CREDIT_SPREAD_CONTAINED")
    vix_component = 0.0
    if vix and vix3m and vix3m.value:
        ratio = vix.value / vix3m.value
        vix_component = _clip((ratio - 0.85) / 0.30 * 100)
        if ratio > 1.0: drivers.append("VIX_CURVE_BACKWARDATION")
    move_component = 0.0
    if move:
        move_component = _clip((move.value - 70) / 60 * 100)
        if move.value >= 110: drivers.append("RATE_VOLATILITY_HIGH")
    dxy_component = 0.0
    if dxy:
        dxy_r20 = _return(dxy, "delta_20d")
        dxy_component = _clip((dxy_r20 or 0.0) * 1000 + 35)
        if dxy_r20 is not None and dxy_r20 > 0.02: drivers.append("DOLLAR_TIGHTENING")
    transmission = 0.0
    for feature, label in ((iwm_spy, "SMALL_CAP_TRANSMISSION_WEAK"), (kre_spy, "BANK_TRANSMISSION_WEAK")):
        r20 = _return(feature, "delta_20d")
        if r20 is not None and r20 < -0.03:
            transmission += 50; drivers.append(label)
    score = _clip(credit_component * 0.30 + vix_component * 0.20 + move_component * 0.15 + dxy_component * 0.10 + transmission * 0.20)
    state = (
        RiskState.CALM if score < 20 else RiskState.NORMAL if score < 40 else RiskState.ELEVATED if score < 60
        else RiskState.HIGH_RISK if score < 75 else RiskState.STRESS if score < 90 else RiskState.SYSTEMIC_STRESS
    )
    return score, state, drivers, offsets


def _liquidity(features: dict[str, SeriesFeature]) -> tuple[float, LiquidityState, list[str], list[str]]:
    walcl, tga, rrp = features.get("WALCL"), features.get("WTREGEN"), features.get("RRPONTSYD")
    dxy, credit = _first(features, "DXY", "DXY_PROXY", "UUP"), features.get("BAMLC0A0CM")
    balance_delta = 0.0
    known = 0
    for feature, sign in ((walcl, 1), (tga, -1), (rrp, -1)):
        normalized_delta = _millions_usd_delta(feature, "delta_20d")
        if normalized_delta is not None:
            balance_delta += sign * normalized_delta; known += 1
    score = _clip(balance_delta / 100_000 * 100, -100, 100) if known else 0.0
    drivers: list[str] = []
    offsets: list[str] = []
    walcl_delta = _millions_usd_delta(walcl, "delta_20d")
    tga_delta = _millions_usd_delta(tga, "delta_20d")
    rrp_delta = _millions_usd_delta(rrp, "delta_20d")
    if walcl_delta is not None:
        (drivers if walcl_delta > 0 else offsets).append(
            "FED_BALANCE_SHEET_EXPANDING" if walcl_delta > 0 else "FED_BALANCE_SHEET_CONTRACTING"
        )
    if tga_delta is not None:
        (drivers if tga_delta < 0 else offsets).append(
            "TGA_DRAWDOWN_INJECTS_LIQUIDITY" if tga_delta < 0 else "TGA_REBUILD_DRAINS_LIQUIDITY"
        )
    if rrp_delta is not None:
        (drivers if rrp_delta < 0 else offsets).append(
            "RRP_DRAWDOWN_RELEASES_LIQUIDITY" if rrp_delta < 0 else "RRP_REBUILD_ABSORBS_LIQUIDITY"
        )
    dxy_r20 = _return(dxy, "delta_20d")
    if dxy_r20 is not None:
        score -= _clip(dxy_r20 * 1000, -30, 30)
        (drivers if dxy_r20 < 0 else offsets).append("DOLLAR_WEAKER_SUPPORTS_LIQUIDITY" if dxy_r20 < 0 else "DOLLAR_STRENGTH_TIGHTENS")
    if credit:
        c20 = _bp(credit, "delta_20d")
        if c20 is not None and c20 > 10:
            score -= 20; offsets.append("CREDIT_SPREAD_WIDENING")
        elif c20 is not None and c20 <= 0:
            score += 10; drivers.append("CREDIT_STABLE_OR_TIGHTER")
    score = _clip(score, -100, 100)
    state = (
        LiquidityState.STRONGLY_EXPANDING if score >= 60 else LiquidityState.EXPANDING if score >= 20
        else LiquidityState.NEUTRAL if score > -20 else LiquidityState.CONTRACTING if score > -60
        else LiquidityState.STRONGLY_CONTRACTING
    )
    return score, state, drivers, offsets


def _liquidity_source_flows(features: dict[str, SeriesFeature]) -> tuple[LiquiditySourceFlow, ...]:
    specs = (
        ("FED_BALANCE_SHEET", features.get("WALCL"), 1.0),
        ("TREASURY_GENERAL_ACCOUNT", features.get("WTREGEN"), -1.0),
        ("OVERNIGHT_REVERSE_REPO", features.get("RRPONTSYD"), -1.0),
    )
    flows: list[LiquiditySourceFlow] = []
    for source_id, feature, sign in specs:
        delta_millions = _millions_usd_delta(feature, "delta_20d")
        if feature is None or delta_millions is None:
            continue
        flow_billions = sign * delta_millions / 1_000.0
        flows.append(LiquiditySourceFlow(
            source_id=source_id,
            flow_billions_usd_20d=flow_billions,
            direction="INJECTION" if flow_billions > 0 else "DRAIN" if flow_billions < 0 else "FLAT",
            observation_date=feature.observation_date,
            confidence=max(0.25, 1 - feature.stale_days / 10),
        ))
    return tuple(flows)


def _flow_state(score: float) -> LiquidityFlowState:
    if score >= 60:
        return LiquidityFlowState.STRONG_ABSORPTION
    if score >= 20:
        return LiquidityFlowState.ABSORBING
    if score > -20:
        return LiquidityFlowState.MIXED
    if score > -60:
        return LiquidityFlowState.REJECTING
    return LiquidityFlowState.STRONG_REJECTION


def _liquidity_target_flows(
    features: dict[str, SeriesFeature],
    liquidity_score: float,
    rate_score: float,
    risk_score: float,
) -> tuple[LiquidityTargetFlow, ...]:
    """Estimate relative liquidity absorption; this is not ETF creation/redemption accounting."""
    specs = {
        "US_LARGE_CAP": ("SPY", "QQQ_SPY", 0.30, -0.12, -0.10),
        "AI_SEMICONDUCTOR": ("QQQ", "SOXX_QQQ", 0.30, -0.30, -0.08),
        "US_SMALL_CAP": ("IWM", "IWM_SPY", 0.32, -0.10, -0.35),
        "US_BANKS_CREDIT": ("KRE", "KRE_SPY", 0.28, 0.05, -0.50),
        "TREASURY_7_10Y": ("IEF", "IEF_SPY", 0.08, -0.45, 0.22),
        "TREASURY_20Y_PLUS": ("TLT", "TLT_SPY", 0.08, -0.58, 0.25),
        "GOLD": ("GLD", "GLD_SPY", 0.18, -0.30, 0.12),
        "DOLLAR_CASH": ("DXY_PROXY", None, -0.22, 0.20, 0.35),
    }
    output: list[LiquidityTargetFlow] = []
    for target_id, (market_id, relative_id, liquidity_weight, rate_weight, risk_weight) in specs.items():
        market = _first(features, market_id, "DXY", "UUP") if target_id == "DOLLAR_CASH" else features.get(market_id)
        return5 = _return(market, "delta_5d")
        return20 = _return(market, "delta_20d")
        market_component = _clip((return5 or 0.0) * 500 + (return20 or 0.0) * 250, -45, 45)
        relative_component = 0.0
        relative = features.get(relative_id) if relative_id else None
        relative_return = _return(relative, "delta_20d")
        if relative_return is not None:
            relative_component = _clip(relative_return * 350, -18, 18)
            market_component = _clip(market_component + relative_component, -55, 55)
        impulse_component = liquidity_score * liquidity_weight
        structural_component = rate_score * rate_weight + risk_score * risk_weight
        if target_id == "GOLD":
            dollar_return = _return(_first(features, "DXY", "DXY_PROXY", "UUP"), "delta_20d")
            structural_component -= _clip((dollar_return or 0.0) * 500, -15, 15)
        score = _clip(impulse_component + market_component + structural_component, -100, 100)
        supporting: list[str] = []
        conflicts: list[str] = []
        if impulse_component >= 10:
            supporting.append("SYSTEM_LIQUIDITY_IMPULSE_POSITIVE")
        elif impulse_component <= -10:
            conflicts.append("SYSTEM_LIQUIDITY_IMPULSE_NEGATIVE")
        if market_component >= 8:
            supporting.append("MARKET_ABSORPTION_CONFIRMED")
        elif market_component <= -8:
            conflicts.append("MARKET_ABSORPTION_NOT_CONFIRMED")
        if relative_component >= 5:
            supporting.append("RELATIVE_TRANSMISSION_IMPROVING")
        elif relative_component <= -5:
            conflicts.append("RELATIVE_TRANSMISSION_WEAKENING")
        if structural_component >= 8:
            supporting.append("MACRO_STRUCTURE_SUPPORTIVE")
        elif structural_component <= -8:
            conflicts.append("MACRO_STRUCTURE_RESTRICTIVE")
        expected = [market, features.get("DFII10"), _first(features, "DXY", "DXY_PROXY", "UUP")]
        present = [item for item in expected if item is not None]
        freshness = sum(max(0.25, 1 - item.stale_days / 10) for item in present) / len(expected)
        confidence = _clip((len(present) / len(expected)) * freshness, 0, 1)
        output.append(LiquidityTargetFlow(
            target_id=target_id,
            proxy_symbol="DTWEXBGS" if target_id == "DOLLAR_CASH" else market_id,
            state=_flow_state(score),
            absorption_score=score,
            liquidity_impulse_component=impulse_component,
            market_confirmation_component=market_component,
            structural_component=structural_component,
            confidence=confidence,
            supporting_signals=tuple(supporting),
            conflicting_signals=tuple(conflicts),
            measurement_note="Relative liquidity-transmission proxy; not audited ETF net fund flow.",
        ))
    return tuple(sorted(output, key=lambda item: item.absorption_score, reverse=True))


def _stance(score: float) -> Stance:
    return Stance.STRONGLY_BULLISH if score >= 60 else Stance.BULLISH if score >= 25 else Stance.NEUTRAL if score > -25 else Stance.BEARISH if score > -60 else Stance.STRONGLY_BEARISH


def _horizon_field(horizon: StanceHorizon) -> str:
    return "delta_1d" if horizon is StanceHorizon.TACTICAL else "delta_5d" if horizon is StanceHorizon.SWING else "delta_20d"


def _asset_stances(
    features: dict[str, SeriesFeature], as_of: datetime, valid_until: datetime,
    rate_score: float, risk_score: float, liquidity_score: float,
) -> tuple[AssetStance, ...]:
    output: list[AssetStance] = []
    specs = {
        "SPX": ("SPY", 0.25, -0.15, -0.25),
        "NDX": ("QQQ", 0.20, -0.30, -0.20),
        "RUT": ("IWM", 0.20, -0.10, -0.30),
        "UST10_PRICE": ("IEF", 0.00, -0.45, 0.20),
        "UST30_PRICE": ("TLT", 0.00, -0.55, 0.20),
        "GLD": ("GLD", 0.05, -0.30, 0.10),
    }
    for horizon in StanceHorizon:
        field = _horizon_field(horizon)
        for asset_id, (market_id, liquidity_weight, rate_weight, risk_weight) in specs.items():
            market = features.get(market_id)
            market_return = _return(market, field)
            trend_score = _clip((market_return or 0.0) * 800, -45, 45)
            score = trend_score + liquidity_score * liquidity_weight + rate_score * rate_weight + risk_score * risk_weight
            supporting: list[str] = []
            opposing: list[str] = []
            if market_return is not None:
                (supporting if market_return > 0 else opposing).append(f"{market_id}_{field.upper()}_{'POSITIVE' if market_return > 0 else 'NEGATIVE'}")
            if liquidity_score >= 20: supporting.append("LIQUIDITY_EXPANDING")
            if liquidity_score <= -20: opposing.append("LIQUIDITY_CONTRACTING")
            if asset_id in {"SPX", "NDX", "RUT", "GLD"} and rate_score >= 70: opposing.append("REAL_RATE_PRESSURE_HIGH")
            if asset_id.startswith("UST") and rate_score >= 70: opposing.append("YIELDS_UP_IS_BOND_PRICE_NEGATIVE")
            if asset_id == "NDX":
                soxx_qqq = features.get("SOXX_QQQ")
                relative = _return(soxx_qqq, field)
                if relative is not None:
                    score += _clip(relative * 500, -15, 15)
                    (supporting if relative > 0 else opposing).append("SOXX_CONFIRMATION" if relative > 0 else "SOXX_DIVERGENCE")
            if asset_id == "RUT":
                for ratio_id in ("IWM_SPY", "KRE_SPY"):
                    relative = _return(features.get(ratio_id), field)
                    if relative is not None:
                        score += _clip(relative * 350, -12, 12)
                        (supporting if relative > 0 else opposing).append(f"{ratio_id}_{'IMPROVING' if relative > 0 else 'WEAKENING'}")
            if asset_id == "GLD":
                dxy_return = _return(_first(features, "DXY", "DXY_PROXY", "UUP"), field)
                if dxy_return is not None:
                    score -= _clip(dxy_return * 500, -15, 15)
                    (supporting if dxy_return < 0 else opposing).append("DOLLAR_WEAKER" if dxy_return < 0 else "DOLLAR_STRONGER")
            expected = [market, features.get("DFII10"), _first(features, "DXY", "DXY_PROXY", "UUP")]
            present = [item for item in expected if item is not None]
            freshness = sum(max(0.25, 1 - item.stale_days / 10) for item in present) / len(expected)
            confidence = _clip((len(present) / len(expected)) * freshness, 0, 1)
            score = _clip(score, -100, 100)
            output.append(AssetStance(
                asset_id=asset_id, horizon=horizon, stance=_stance(score), direction_score=score,
                confidence=confidence, supporting_factors=tuple(supporting), opposing_factors=tuple(opposing),
                risk_triggers=("CREDIT_SPREAD_WIDENING", "VIX_CURVE_BACKWARDATION"),
                invalidation_conditions=("DIRECTION_SCORE_CROSSES_NEUTRAL_BAND",),
                as_of=as_of, valid_until=valid_until,
            ))
    return tuple(output)


def evaluate_macro(features: dict[str, SeriesFeature], as_of: datetime) -> MacroSnapshot:
    valid_until = as_of + timedelta(days=1)
    rate_score, rate_state, rate_drivers, rate_conflicts = _rate_pressure(features)
    quadrant, decomposition_flags = _inflation_quadrant(features)
    risk_score, risk_state, risk_drivers, risk_offsets = _risk(features)
    liquidity_score, liquidity_state, liquidity_drivers, liquidity_offsets = _liquidity(features)
    source_flows = _liquidity_source_flows(features)
    target_flows = _liquidity_target_flows(features, liquidity_score, rate_score, risk_score)
    stale = tuple(sorted(key for key, feature in features.items() if "STALE_SERIES" in feature.quality_flags))
    input_quality_flags = {
        flag for feature in features.values() for flag in feature.quality_flags if flag != "STALE_SERIES"
    }
    quality_flags = tuple(sorted(set(decomposition_flags).union(input_quality_flags)))
    available_core = sum(series_id in features for series_id in CORE_SERIES)
    if "DXY" not in features and _first(features, "DXY_PROXY", "UUP") is not None:
        available_core += 1
    coverage = available_core / len(CORE_SERIES)
    freshness = 1 - min(0.75, sum(min(features[s].stale_days, 10) for s in CORE_SERIES if s in features) / max(1, available_core * 20))
    confidence = _clip(coverage * freshness, 0, 1)
    if quality_flags:
        confidence *= 0.8
    if risk_state is RiskState.SYSTEMIC_STRESS:
        regime = MacroRegime.SYSTEMIC_STRESS
    elif risk_score >= 75:
        regime = MacroRegime.BROAD_RISK_OFF
    elif risk_score >= 60 and features.get("BAMLC0A0CM") and features["BAMLC0A0CM"].value >= 1.0:
        regime = MacroRegime.CREDIT_TIGHTENING
    elif rate_score >= 70:
        regime = MacroRegime.LONG_DURATION_PRESSURE
    elif liquidity_score >= 20 and risk_score < 40:
        regime = MacroRegime.RISK_ON_CONFIRMING
    elif liquidity_score >= 40:
        regime = MacroRegime.LIQUIDITY_EASING
    else:
        regime = MacroRegime.NEUTRAL_TRANSITION
    stances = _asset_stances(features, as_of, valid_until, rate_score, risk_score, liquidity_score)
    observation_fingerprint = MACRO_MODEL_VERSION + "|" + "|".join(
        sorted(f"{key}:{value.observation_date}:{value.value}" for key, value in features.items())
    )
    snapshot_id = "macro/" + as_of.date().isoformat() + "/" + hashlib.sha256(observation_fingerprint.encode()).hexdigest()[:12]
    return MacroSnapshot(
        snapshot_id=snapshot_id, as_of=as_of, valid_until=valid_until, primary_regime=regime,
        risk_state=risk_state, risk_score=risk_score, liquidity_state=liquidity_state,
        liquidity_score=liquidity_score, inflation_quadrant=quadrant, rate_pressure_state=rate_state,
        rate_pressure_score=rate_score, liquidity_source_flows=source_flows,
        liquidity_target_flows=target_flows, asset_stances=stances,
        main_drivers=tuple(dict.fromkeys(rate_drivers + risk_drivers + liquidity_drivers)),
        confirming_signals=tuple(dict.fromkeys(risk_offsets + liquidity_drivers)),
        conflicting_signals=tuple(dict.fromkeys(rate_conflicts + risk_offsets + liquidity_offsets)),
        quality_flags=quality_flags, data_coverage=coverage, confidence=confidence,
        stale_series=stale, model_version=MACRO_MODEL_VERSION,
    )
