from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta, timezone

import requests

from quant_agent.data_sources.base import (
    DataBatch,
    DataRequest,
    PermanentSourceError,
    SourceRecord,
    TransientSourceError,
    with_retry,
)


SERIES_METADATA = {
    "WALCL": {"unit": "millions_usd", "frequency": "weekly", "availability_lag_days": 1},
    "WTREGEN": {"unit": "millions_usd", "frequency": "weekly", "availability_lag_days": 1},
    "RRPONTSYD": {"unit": "billions_usd", "frequency": "daily", "availability_lag_days": 0},
}


class FredCsvSource:
    name = "fred.graph_csv"
    base_url = "https://fred.stlouisfed.org/graph/fredgraph.csv"

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()

    def _fetch_one(self, series_id: str, request: DataRequest) -> list[SourceRecord]:
        if series_id not in SERIES_METADATA:
            raise PermanentSourceError(f"unsupported FRED series: {series_id}")

        def call() -> requests.Response:
            try:
                response = self.session.get(
                    self.base_url,
                    params={
                        "id": series_id,
                        "cosd": request.start_date.isoformat(),
                        "coed": request.end_date.isoformat(),
                    },
                    timeout=30,
                )
            except requests.RequestException as exc:
                raise TransientSourceError("FRED request failed") from exc
            if response.status_code == 429 or response.status_code >= 500:
                raise TransientSourceError(f"FRED temporary HTTP {response.status_code}")
            if response.status_code >= 400:
                raise PermanentSourceError(f"FRED rejected request with HTTP {response.status_code}")
            return response

        response = with_retry(call)
        metadata = SERIES_METADATA[series_id]
        rows = csv.DictReader(io.StringIO(response.text))
        records: list[SourceRecord] = []
        for row in rows:
            raw_value = row.get(series_id)
            raw_date = row.get("observation_date") or row.get("DATE")
            if not raw_date or raw_value in {None, ".", ""}:
                continue
            observed = datetime.fromisoformat(raw_date).replace(tzinfo=timezone.utc)
            available = observed + timedelta(days=int(metadata["availability_lag_days"]))
            records.append(
                SourceRecord(
                    symbol=series_id,
                    event_time=observed,
                    available_at=available,
                    payload={
                        "series_id": series_id,
                        "observation_date": raw_date,
                        "value": float(raw_value),
                        "unit": metadata["unit"],
                        "frequency": metadata["frequency"],
                    },
                )
            )
        return records

    def fetch(self, request: DataRequest) -> DataBatch:
        records: list[SourceRecord] = []
        for series_id in request.symbols:
            records.extend(self._fetch_one(series_id, request))
        return DataBatch.create(dataset=request.dataset, source=self.name, records=records)
