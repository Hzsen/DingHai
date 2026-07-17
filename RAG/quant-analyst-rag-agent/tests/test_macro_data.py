from __future__ import annotations

import sqlite3
from datetime import date

from quant_agent.data_sources.base import DataRequest
from quant_agent.macro.data import (
    CboeVolatilitySource,
    FredMacroSource,
    batches_to_observations,
    load_macro_observations,
    publish_macro_observations,
)


class _Response:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _Session:
    def __init__(self, text: str) -> None:
        self.text = text

    def get(self, *args, **kwargs) -> _Response:
        return _Response(self.text)


def test_fred_adapter_parses_offline_fixture_and_preserves_proxy_identity() -> None:
    source = FredMacroSource(_Session("observation_date,DTWEXBGS\n2026-07-10,120.5\n"))
    request = DataRequest("macro_regime_observations", ("DXY_PROXY",), date(2026, 7, 1), date(2026, 7, 15))
    batch = source.fetch(request)
    assert not batch.errors
    assert batch.records[0].payload["series_id"] == "DXY_PROXY"
    assert batch.records[0].payload["provider_series_id"] == "DTWEXBGS"
    assert batch.records[0].available_at.date() == date(2026, 7, 11)


def test_cboe_adapter_parses_offline_fixture() -> None:
    source = CboeVolatilitySource(_Session("DATE,OPEN,HIGH,LOW,CLOSE\n07/14/2026,17,18,16,16.5\n"))
    request = DataRequest("macro_regime_observations", ("VIX",), date(2026, 7, 1), date(2026, 7, 15))
    batch = source.fetch(request)
    assert not batch.errors
    assert batch.records[0].payload["value"] == 16.5
    assert batch.records[0].payload["is_realtime"] is True


def test_macro_observation_cache_is_idempotent(tmp_path) -> None:
    source = FredMacroSource(_Session("observation_date,DGS10\n2026-07-14,4.62\n"))
    request = DataRequest("macro_regime_observations", ("DGS10",), date(2026, 7, 1), date(2026, 7, 15))
    observations = batches_to_observations([source.fetch(request)])
    db = tmp_path / "macro.db"
    publish_macro_observations(db, observations, [])
    publish_macro_observations(db, observations, [])
    loaded = load_macro_observations(db)
    with sqlite3.connect(db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM macro_source_observations").fetchone()[0]
    assert count == 1
    assert len(loaded) == 1

