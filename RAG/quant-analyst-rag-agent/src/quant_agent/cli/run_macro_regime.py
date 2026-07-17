from __future__ import annotations

import argparse
import json
from datetime import datetime, time, timezone
from pathlib import Path

from domain.macro import MacroDocumentStatus, SeriesFeature
from quant_agent.config import Paths
from quant_agent.macro.data import (
    build_live_macro_features,
    fetch_live_macro_observations,
    load_macro_observations,
    publish_macro_observations,
)
from quant_agent.macro.document import build_macro_document, publish_macro_document
from quant_agent.macro.history import build_macro_history, detect_macro_changes, publish_macro_history
from quant_agent.macro.kimi_analysis import (
    MacroAnalysisCache,
    MacroPricingAnalysisError,
    build_macro_analysis_cache_key,
    build_macro_analysis_packet,
    build_macro_pricing_prompt,
    load_macro_pricing_inference,
    publish_macro_pricing_inference,
    request_macro_pricing_analysis,
)
from quant_agent.macro.report import publish_macro_outputs
from quant_agent.macro.rules import evaluate_macro
from quant_agent.llm.kimi_client import KimiAPIError, KimiClient, KimiConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run macro liquidity transmission and half-month repricing analysis.")
    parser.add_argument("--features", default="data/mock/macro_features_screenshot.json")
    parser.add_argument("--db", default="data/processed/phase1_research.db")
    parser.add_argument("--output-dir", default="outputs/macro")
    parser.add_argument("--draft", action="store_true")
    parser.add_argument("--live", action="store_true", help="Fetch real FRED/CBOE/AkShare inputs instead of a fixture.")
    parser.add_argument("--as-of", default=None, help="ISO timestamp or date; date-only means 23:59 UTC.")
    parser.add_argument("--lookback-days", type=int, default=365 * 6)
    parser.add_argument("--minimum-coverage", type=float, default=0.5)
    parser.add_argument("--reuse-cache", action="store_true", help="Use the last atomically published observation cache.")
    parser.add_argument("--history-days", type=int, default=14, help="Sensitive repricing window in calendar days.")
    parser.add_argument("--with-kimi", action="store_true", help="Request event-triggered Kimi pricing inference.")
    parser.add_argument("--kimi-model", default=None)
    parser.add_argument("--kimi-context", action="append", default=[], help="Optional official-context text file; max 3.")
    return parser.parse_args()


def load_feature_fixture(path: Path) -> tuple[datetime, dict[str, SeriesFeature], dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    as_of = datetime.fromisoformat(payload["as_of"])
    features: dict[str, SeriesFeature] = {}
    for item in payload["features"]:
        features[item["series_id"]] = SeriesFeature(
            series_id=item["series_id"], as_of=as_of, value=float(item["value"]), unit=item["unit"],
            source=item.get("source", "fixture"), observation_date=item.get("observation_date", payload["as_of"]),
            available_at=item.get("available_at", payload["as_of"]), is_realtime=bool(item.get("is_realtime", False)),
            stale_days=int(item.get("stale_days", 0)), delta_1d=item.get("delta_1d"),
            delta_5d=item.get("delta_5d"), delta_20d=item.get("delta_20d"),
            percentile_5y=item.get("percentile_5y"), z_change_5d_252=item.get("z_change_5d_252"),
            quality_flags=tuple(item.get("quality_flags", [])),
        )
    return as_of, features, payload.get("metadata", {})


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _parse_as_of(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if len(value) == 10:
        return datetime.combine(datetime.fromisoformat(value).date(), time(23, 59), tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _load_contexts(root: Path, values: list[str]) -> list[str]:
    contexts: list[str] = []
    for value in values[:3]:
        path = _resolve(root, value)
        contexts.append(path.read_text(encoding="utf-8")[:1000])
    return contexts


def main() -> None:
    args = parse_args()
    root = Paths().project_root
    db_path = _resolve(root, args.db)
    output_dir = _resolve(root, args.output_dir)
    if args.live:
        as_of = _parse_as_of(args.as_of)
        if args.reuse_cache:
            observations = load_macro_observations(db_path)
            source_errors: list[dict[str, object]] = []
        else:
            observations, source_errors = fetch_live_macro_observations(as_of, args.lookback_days)
        features = build_live_macro_features(observations, as_of)
        metadata = {
            "scope": "live_multi_source",
            "providers": ["local SQLite cache"] if args.reuse_cache else ["FRED", "CBOE", "AkShare/Sina"],
            "observation_rows": len(observations),
            "source_error_count": len(source_errors),
            "source_errors": source_errors,
        }
        if not args.reuse_cache:
            publish_macro_observations(db_path, observations, source_errors)
        input_label = "cache:macro_source_observations" if args.reuse_cache else "live:FRED+CBOE+AkShare"
    else:
        feature_path = _resolve(root, args.features)
        as_of, features, metadata = load_feature_fixture(feature_path)
        input_label = str(feature_path)
    snapshot = evaluate_macro(features, as_of)
    if snapshot.data_coverage < args.minimum_coverage:
        raise RuntimeError(
            f"macro coverage {snapshot.data_coverage:.0%} is below publish threshold "
            f"{args.minimum_coverage:.0%}; previous finalized document remains active"
        )
    status = MacroDocumentStatus.DRAFT_INTRADAY if args.draft else MacroDocumentStatus.FINALIZED_DAILY
    document = build_macro_document(snapshot, features, status=status)
    publish_macro_document(db_path, document)
    history_points = []
    change_events = []
    packet = None
    kimi_inference = None
    kimi_status = "not_requested"
    kimi_cache_hit = False
    if args.live:
        history_points = build_macro_history(observations, as_of, args.history_days)
        change_events = detect_macro_changes(history_points)
        publish_macro_history(db_path, history_points, change_events)
        packet = build_macro_analysis_packet(history_points, change_events)
        kimi_inference = load_macro_pricing_inference(db_path, packet.packet_id)
        if kimi_inference is not None:
            kimi_status = "loaded_from_published_cache"
            kimi_cache_hit = True
        if args.with_kimi and any(event.needs_kimi_analysis for event in change_events):
            contexts = _load_contexts(root, args.kimi_context)
            try:
                config = KimiConfig.from_env()
                model = args.kimi_model or config.model
                messages = build_macro_pricing_prompt(packet, contexts)
                cache = MacroAnalysisCache(root / ".cache" / "macro_kimi")
                cache_key = build_macro_analysis_cache_key(packet, contexts, model)
                kimi_inference, kimi_cache_hit = cache.get_or_compute(
                    cache_key,
                    lambda: request_macro_pricing_analysis(
                        KimiClient(config), model, messages, packet.packet_id
                    ),
                )
                publish_macro_pricing_inference(
                    db_path, packet, kimi_inference, model=model, cache_key=cache_key
                )
                kimi_status = "cache_hit" if kimi_cache_hit else "generated"
            except (KimiAPIError, MacroPricingAnalysisError, OSError) as exc:
                kimi_status = f"failed:{type(exc).__name__}"
    paths = publish_macro_outputs(
        output_dir, snapshot, document, history_points, change_events, kimi_inference, packet
    )
    result = {
        "input": input_label, "input_metadata": metadata, "snapshot_id": snapshot.snapshot_id,
        "as_of": snapshot.as_of.isoformat(), "valid_until": snapshot.valid_until.isoformat(),
        "primary_regime": snapshot.primary_regime.value, "risk_state": snapshot.risk_state.value,
        "risk_score": snapshot.risk_score, "liquidity_state": snapshot.liquidity_state.value,
        "liquidity_score": snapshot.liquidity_score, "rate_pressure_state": snapshot.rate_pressure_state.value,
        "rate_pressure_score": snapshot.rate_pressure_score, "inflation_quadrant": snapshot.inflation_quadrant.value,
        "coverage": snapshot.data_coverage, "confidence": snapshot.confidence,
        "quality_flags": list(snapshot.quality_flags), "outputs": {key: str(path) for key, path in paths.items()},
        "db": str(db_path), "document_status": document.status.value,
        "history_window_days": args.history_days if args.live else None,
        "history_points": len(history_points), "change_event_count": len(change_events),
        "analysis_packet_id": packet.packet_id if packet else None,
        "kimi_status": kimi_status, "kimi_cache_hit": kimi_cache_hit,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
