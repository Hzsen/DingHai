from __future__ import annotations

from quant_agent.data_sources.base import DataBatch, DataRequest, SourceRecord


class FixtureDataSource:
    """Deterministic offline source used by contract and integration tests."""

    def __init__(self, name: str, records: list[SourceRecord]) -> None:
        self.name = name
        self._records = tuple(records)

    def fetch(self, request: DataRequest) -> DataBatch:
        records = [
            record
            for record in self._records
            if record.symbol in request.symbols
            and request.start_date <= record.event_time.date() <= request.end_date
        ]
        return DataBatch.create(dataset=request.dataset, source=self.name, records=records)
