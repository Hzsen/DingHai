from __future__ import annotations

import csv
import io
import json
import sqlite3
from dataclasses import asdict, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from domain.macro import SeriesFeature
from quant_agent.data_sources.base import (
    BatchError,
    DataBatch,
    DataRequest,
    PermanentSourceError,
    SourceRecord,
    TransientSourceError,
    with_retry,
)
from quant_agent.macro.features import compute_macro_features, derived_ratio_feature


FRED_SERIES: dict[str, dict[str, Any]] = {
    "DFII10": {"provider_id": "DFII10", "unit": "percent", "frequency": "daily", "lag_days": 0},
    "DGS10": {"provider_id": "DGS10", "unit": "percent", "frequency": "daily", "lag_days": 0},
    "DGS30": {"provider_id": "DGS30", "unit": "percent", "frequency": "daily", "lag_days": 0},
    "DGS2": {"provider_id": "DGS2", "unit": "percent", "frequency": "daily", "lag_days": 0},
    "T10YIE": {"provider_id": "T10YIE", "unit": "percent", "frequency": "daily", "lag_days": 0},
    "BAMLC0A0CM": {"provider_id": "BAMLC0A0CM", "unit": "percent", "frequency": "daily", "lag_days": 0},
    # FRED's broad dollar index is a documented proxy, not the ICE DXY index.
    "DXY_PROXY": {"provider_id": "DTWEXBGS", "unit": "index", "frequency": "daily", "lag_days": 1},
    "WALCL": {"provider_id": "WALCL", "unit": "millions_usd", "frequency": "weekly", "lag_days": 1},
    "WTREGEN": {"provider_id": "WTREGEN", "unit": "millions_usd", "frequency": "weekly", "lag_days": 1},
    "RRPONTSYD": {"provider_id": "RRPONTSYD", "unit": "billions_usd", "frequency": "daily", "lag_days": 0},
}

MARKET_SERIES = (
    "SPY", "QQQ", "IWM", "RSP", "KRE", "SOXX", "GLD", "IEF", "TLT",
    "USO", "CPER", "FXY", "IBIT",
)
CBOE_SERIES = {
    "VIX": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
    "VIX3M": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX3M_History.csv",
}


def _available_at(observation_date: date, lag_days: int, close_hour_utc: int = 23) -> datetime:
    return datetime.combine(observation_date + timedelta(days=lag_days), time(close_hour_utc, 59), tzinfo=timezone.utc)


class FredMacroSource:
    """FRED adapter for rates, credit and Federal Reserve balance-sheet series."""

    name = "fred.graph_csv.macro"
    base_url = "https://fred.stlouisfed.org/graph/fredgraph.csv"

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    def _fetch_one(self, canonical_id: str, request: DataRequest) -> list[SourceRecord]:
        metadata = FRED_SERIES.get(canonical_id)
        if metadata is None:
            raise PermanentSourceError(f"unsupported FRED macro series: {canonical_id}")
        provider_id = str(metadata["provider_id"])

        def call() -> requests.Response:
            try:
                response = self.session.get(
                    self.base_url,
                    params={"id": provider_id, "cosd": request.start_date.isoformat(), "coed": request.end_date.isoformat()},
                    timeout=30,
                )
            except requests.RequestException as exc:
                raise TransientSourceError(f"FRED request failed for {canonical_id}") from exc
            if response.status_code == 429 or response.status_code >= 500:
                raise TransientSourceError(f"FRED temporary HTTP {response.status_code} for {canonical_id}")
            if response.status_code >= 400:
                raise PermanentSourceError(f"FRED rejected {canonical_id} with HTTP {response.status_code}")
            return response

        response = with_retry(call)
        rows = csv.DictReader(io.StringIO(response.text))
        records: list[SourceRecord] = []
        for row in rows:
            raw_date = row.get("observation_date") or row.get("DATE")
            raw_value = row.get(provider_id)
            if not raw_date or raw_value in {None, "", "."}:
                continue
            observed_date = date.fromisoformat(raw_date)
            event_time = datetime.combine(observed_date, time.min, tzinfo=timezone.utc)
            available_at = _available_at(observed_date, int(metadata["lag_days"]))
            records.append(SourceRecord(
                symbol=canonical_id,
                event_time=event_time,
                available_at=available_at,
                payload={
                    "series_id": canonical_id,
                    "provider_series_id": provider_id,
                    "observation_date": raw_date,
                    "value": float(raw_value),
                    "unit": metadata["unit"],
                    "frequency": metadata["frequency"],
                    "is_realtime": False,
                },
            ))
        return records

    def fetch(self, request: DataRequest) -> DataBatch:
        records: list[SourceRecord] = []
        errors: list[BatchError] = []
        for series_id in request.symbols:
            try:
                records.extend(self._fetch_one(series_id, request))
            except (TransientSourceError, PermanentSourceError) as exc:
                errors.append(BatchError(series_id, type(exc).__name__, str(exc), isinstance(exc, TransientSourceError)))
        return DataBatch.create(dataset=request.dataset, source=self.name, records=records, errors=errors)


class CboeVolatilitySource:
    """CBOE daily-close adapter for the VIX term-structure inputs."""

    name = "cboe.daily_indices"

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    def _fetch_one(self, series_id: str, request: DataRequest) -> list[SourceRecord]:
        url = CBOE_SERIES.get(series_id)
        if url is None:
            raise PermanentSourceError(f"unsupported CBOE series: {series_id}")

        def call() -> requests.Response:
            try:
                response = self.session.get(url, timeout=30)
            except requests.RequestException as exc:
                raise TransientSourceError(f"CBOE request failed for {series_id}") from exc
            if response.status_code == 429 or response.status_code >= 500:
                raise TransientSourceError(f"CBOE temporary HTTP {response.status_code} for {series_id}")
            if response.status_code >= 400:
                raise PermanentSourceError(f"CBOE rejected {series_id} with HTTP {response.status_code}")
            return response

        response = with_retry(call)
        records: list[SourceRecord] = []
        for row in csv.DictReader(io.StringIO(response.text)):
            raw_date = row.get("DATE")
            raw_value = row.get("CLOSE")
            if not raw_date or not raw_value:
                continue
            observed_date = datetime.strptime(raw_date, "%m/%d/%Y").date()
            if not request.start_date <= observed_date <= request.end_date:
                continue
            event_time = datetime.combine(observed_date, time(20, 0), tzinfo=timezone.utc)
            records.append(SourceRecord(
                series_id,
                event_time,
                event_time + timedelta(minutes=30),
                {
                    "series_id": series_id,
                    "provider_series_id": series_id,
                    "observation_date": observed_date.isoformat(),
                    "value": float(raw_value),
                    "unit": "index",
                    "frequency": "daily",
                    "is_realtime": True,
                },
            ))
        return records

    def fetch(self, request: DataRequest) -> DataBatch:
        records: list[SourceRecord] = []
        errors: list[BatchError] = []
        for series_id in request.symbols:
            try:
                records.extend(self._fetch_one(series_id, request))
            except (TransientSourceError, PermanentSourceError) as exc:
                errors.append(BatchError(series_id, type(exc).__name__, str(exc), isinstance(exc, TransientSourceError)))
        return DataBatch.create(dataset=request.dataset, source=self.name, records=records, errors=errors)


class AkShareUsMarketSource:
    """AkShare/Sina adapter for liquid US ETF closing-price proxies."""

    name = "akshare.sina_us_daily"

    def fetch(self, request: DataRequest) -> DataBatch:
        try:
            import akshare as ak
        except ImportError as exc:
            raise PermanentSourceError("akshare is required; install the research extra") from exc
        records: list[SourceRecord] = []
        errors: list[BatchError] = []
        for series_id in request.symbols:
            if series_id not in MARKET_SERIES:
                errors.append(BatchError(series_id, "PermanentSourceError", "unsupported US market proxy", False))
                continue
            try:
                frame = ak.stock_us_daily(symbol=series_id, adjust="")
                frame["date"] = pd.to_datetime(frame["date"])
                frame = frame.loc[
                    (frame["date"].dt.date >= request.start_date) & (frame["date"].dt.date <= request.end_date)
                ]
                for row in frame.itertuples(index=False):
                    observed_date = row.date.date()
                    event_time = datetime.combine(observed_date, time(20, 0), tzinfo=timezone.utc)
                    records.append(SourceRecord(
                        series_id,
                        event_time,
                        event_time + timedelta(minutes=30),
                        {
                            "series_id": series_id,
                            "provider_series_id": series_id,
                            "observation_date": observed_date.isoformat(),
                            "value": float(row.close),
                            "unit": "usd",
                            "frequency": "daily",
                            "is_realtime": True,
                        },
                    ))
            except Exception as exc:  # Provider payload errors are isolated per symbol.
                errors.append(BatchError(series_id, type(exc).__name__, "AkShare symbol fetch failed", True))
        return DataBatch.create(dataset=request.dataset, source=self.name, records=records, errors=errors)


def batches_to_observations(batches: list[DataBatch]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for batch in batches:
        for record in batch.records:
            payload = dict(record.payload)
            rows.append({
                "series_id": payload["series_id"],
                "observation_date": payload["observation_date"],
                "available_at": record.available_at.isoformat(),
                "value": float(payload["value"]),
                "unit": payload["unit"],
                "source": f"{batch.source}:{payload.get('provider_series_id', record.symbol)}",
                "is_realtime": bool(payload.get("is_realtime", False)),
                "batch_id": batch.batch_id,
                "fetched_at": batch.fetched_at.isoformat(),
            })
    if not rows:
        raise RuntimeError("all live macro data sources returned no observations")
    return pd.DataFrame(rows).sort_values(["series_id", "observation_date", "available_at"])


def fetch_live_macro_observations(as_of: datetime, lookback_days: int = 365 * 6) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    end_date = as_of.date()
    start_date = end_date - timedelta(days=lookback_days)
    dataset = "macro_regime_observations"
    batches = [
        FredMacroSource().fetch(DataRequest(dataset, tuple(FRED_SERIES), start_date, end_date)),
        CboeVolatilitySource().fetch(DataRequest(dataset, tuple(CBOE_SERIES), start_date, end_date)),
        AkShareUsMarketSource().fetch(DataRequest(dataset, MARKET_SERIES, start_date, end_date)),
    ]
    errors = [asdict(error) | {"source": batch.source} for batch in batches for error in batch.errors]
    return batches_to_observations(batches), errors


def build_live_macro_features(observations: pd.DataFrame, as_of: datetime) -> dict[str, SeriesFeature]:
    features = compute_macro_features(observations, as_of)
    for ratio_id, numerator_id, denominator_id in (
        ("QQQ_SPY", "QQQ", "SPY"),
        ("IWM_SPY", "IWM", "SPY"),
        ("KRE_SPY", "KRE", "SPY"),
        ("SOXX_QQQ", "SOXX", "QQQ"),
        ("IEF_SPY", "IEF", "SPY"),
        ("TLT_SPY", "TLT", "SPY"),
        ("GLD_SPY", "GLD", "SPY"),
    ):
        ratio = derived_ratio_feature(ratio_id, features.get(numerator_id), features.get(denominator_id), as_of)
        if ratio is not None:
            features[ratio_id] = ratio
    proxy = features.get("DXY_PROXY")
    if proxy is not None:
        features["DXY_PROXY"] = replace(
            proxy,
            quality_flags=tuple(sorted(set(proxy.quality_flags + ("BROAD_DOLLAR_PROXY_NOT_ICE_DXY",)))),
        )
    return features


def publish_macro_observations(db_path: Path | str, observations: pd.DataFrame, errors: list[dict[str, Any]]) -> None:
    """Atomically upsert the generic observation cache and one auditable fetch run."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = str(observations.iloc[-1]["batch_id"])
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS macro_source_observations (
            series_id TEXT NOT NULL, observation_date TEXT NOT NULL, available_at TEXT NOT NULL,
            value REAL NOT NULL, unit TEXT NOT NULL, source TEXT NOT NULL, is_realtime INTEGER NOT NULL,
            batch_id TEXT NOT NULL, fetched_at TEXT NOT NULL,
            PRIMARY KEY(series_id, observation_date, source)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS macro_source_runs (
            run_id TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, status TEXT NOT NULL,
            record_count INTEGER NOT NULL, error_count INTEGER NOT NULL, error_json TEXT NOT NULL
        )""")
        conn.execute("BEGIN IMMEDIATE")
        for row in observations.itertuples(index=False):
            conn.execute("""INSERT INTO macro_source_observations VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(series_id,observation_date,source) DO UPDATE SET
                available_at=excluded.available_at,value=excluded.value,unit=excluded.unit,
                is_realtime=excluded.is_realtime,batch_id=excluded.batch_id,fetched_at=excluded.fetched_at""",
                (row.series_id, str(row.observation_date), str(row.available_at), float(row.value), row.unit,
                 row.source, int(row.is_realtime), row.batch_id, row.fetched_at),
            )
        conn.execute(
            "INSERT OR REPLACE INTO macro_source_runs VALUES (?,?,?,?,?,?)",
            (run_id, now, "published_with_warnings" if errors else "published", len(observations), len(errors),
             json.dumps(errors, ensure_ascii=False, sort_keys=True)),
        )


def load_macro_observations(db_path: Path | str) -> pd.DataFrame:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"macro observation cache does not exist: {db_path}")
    with sqlite3.connect(db_path) as conn:
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='macro_source_observations'"
        ).fetchone()
        if table is None:
            raise RuntimeError("macro observation cache table has not been initialized")
        frame = pd.read_sql_query("SELECT * FROM macro_source_observations", conn)
    if frame.empty:
        raise RuntimeError("macro observation cache is empty")
    frame["is_realtime"] = frame["is_realtime"].astype(bool)
    return frame
