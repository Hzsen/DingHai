from __future__ import annotations

from domain.thesis import ThesisStatus
from thesis.cache import ThesisUpdateCache, build_cache_key
from thesis.rules import validate_thesis_state
from test_thesis_rules import _thesis


def test_same_input_hits_cache_without_recomputing(tmp_path) -> None:
    validation = validate_thesis_state(
        _thesis(ThesisStatus.THEME_WARMUP),
        {"distance_to_120d_high": -0.02, "amount_ratio_20d": 2, "rs_market_20d": 0.02},
    )
    contexts = ["context one", "context two"]
    key = build_cache_key(validation, contexts)
    cache = ThesisUpdateCache(tmp_path)
    call_count = 0

    def factory() -> dict:
        nonlocal call_count
        call_count += 1
        return {"short_summary": "cached result"}

    first, first_hit = cache.get_or_compute(key, factory)
    second, second_hit = cache.get_or_compute(key, factory)

    assert first == second
    assert first_hit is False
    assert second_hit is True
    assert call_count == 1
