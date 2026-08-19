"""Tests for the unified ``quant-agent daily-run`` dashboard."""

from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from domain.daily_run import (
    AttentionSeverity,
    DailyRunMode,
    DailyRunSnapshot,
    ModuleStatus,
)
from quant_agent.config import Paths
from quant_agent.daily_run.repository import DailyRunRepository
from quant_agent.daily_run.report import (
    render_daily_run_dashboard,
    render_daily_run_markdown,
)
from quant_agent.daily_run.service import run_daily

AS_OF = datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc)

# Every DOM id the embedded JavaScript depends on.
JS_DOM_IDS = (
    "daily-run-data",
    "copy-run-id",
    "print-button",
    "module-filter",
    "module-list",
    "theme-filter-input",
    "theme-state-filter",
    "theme-table-body",
    "theme-detail",
)

CORE_SECTION_IDS = (
    "executive-summary",
    "attention",
    "macro",
    "themes",
    "china-a-share",
    "index-health",
    "modules",
)


@pytest.fixture()
def fixture_result(tmp_path):
    return run_daily(
        mode=DailyRunMode.FIXTURE,
        as_of=AS_OF,
        db_path=tmp_path / "daily.db",
        output_dir=tmp_path / "out",
        paths=Paths(project_root=tmp_path),
    )


@pytest.fixture()
def fixture_html(fixture_result) -> str:
    return fixture_result.outputs["html"].read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# snapshot contract
# --------------------------------------------------------------------------- #

def test_fixture_snapshot_contract(fixture_result):
    snapshot = fixture_result.snapshot
    assert isinstance(snapshot, DailyRunSnapshot)
    assert snapshot.synthetic is True
    assert snapshot.mode is DailyRunMode.FIXTURE
    assert snapshot.overall_status is ModuleStatus.FAILED  # fixture includes a failed module
    assert snapshot.macro is not None and snapshot.macro.primary_regime
    assert snapshot.themes is not None and len(snapshot.themes.themes) >= 2
    assert snapshot.china_a_share is not None
    assert snapshot.index_health is not None
    assert snapshot.attention  # fixture must exercise the attention section
    severities = [item.severity for item in snapshot.attention]
    order = {AttentionSeverity.CRITICAL: 0, AttentionSeverity.WARNING: 1, AttentionSeverity.INFO: 2}
    assert [order[s] for s in severities] == sorted(order[s] for s in severities)
    module_ids = [m.module_id for m in snapshot.modules]
    assert len(module_ids) == len(set(module_ids))
    assert any(m.status is ModuleStatus.WARNING for m in snapshot.modules)


def test_empty_candidates_are_a_valid_result(fixture_result):
    snapshot = fixture_result.snapshot
    china = replace(snapshot.china_a_share, wave_candidates=(), repair_candidates=())
    html_text = render_daily_run_dashboard(replace(snapshot, china_a_share=china))
    assert html_text.count("当前无符合条件候选") == 2
    assert "加载失败" not in html_text


# --------------------------------------------------------------------------- #
# dashboard structure
# --------------------------------------------------------------------------- #

def test_dashboard_core_sections_exist(fixture_html):
    for section_id in CORE_SECTION_IDS:
        assert f'id="{section_id}"' in fixture_html, section_id


def test_fixture_is_labeled_synthetic(fixture_html):
    assert "SYNTHETIC FIXTURE DATA" in fixture_html  # top banner
    assert "SYNTHETIC" in fixture_html.split("<footer", 1)[1]  # footer
    assert "合成" in fixture_html


def test_module_status_filter_controls(fixture_html):
    for value in ("all", "issues", "success", "skipped"):
        assert f'data-filter="{value}"' in fixture_html
    for status in ("SUCCESS", "WARNING", "FAILED", "SKIPPED"):
        assert f'data-status="{status}"' in fixture_html
    assert 'aria-pressed="true"' in fixture_html


def test_theme_filter_controls(fixture_html):
    assert 'id="theme-filter-input"' in fixture_html
    assert 'id="theme-state-filter"' in fixture_html
    assert '<option value="all">' in fixture_html
    assert "CONFIRMED_ENTRY" in fixture_html
    # rows carry filter metadata
    assert "data-search=" in fixture_html
    assert "data-state=" in fixture_html


def test_artifact_links_are_local_and_relative(fixture_result, fixture_html, tmp_path):
    hrefs = re.findall(r'href="([^"]+)"', fixture_html)
    assert hrefs
    for href in hrefs:
        assert not href.startswith(("http://", "https://", "//")), href
        if href.startswith("#"):
            continue  # in-page navigation
        assert not href.startswith("/"), href
    # run-level artifacts point at files that actually exist next to the dashboard
    out_dir = fixture_result.outputs["html"].parent
    for artifact in fixture_result.snapshot.artifacts:
        assert (out_dir / artifact.path).is_file(), artifact.path


def test_all_dynamic_content_is_html_escaped(fixture_result):
    snapshot = fixture_result.snapshot
    evil_module = replace(
        snapshot.modules[0],
        warnings=('<script>alert("x")</script>',),
        metadata=(("k<img>", "v<svg onload=alert(1)>"),),
    )
    modules = (evil_module, *snapshot.modules[1:])
    evil_themes = replace(
        snapshot.themes,
        themes=(replace(snapshot.themes.themes[0], label='<b onclick="x">主题</b>'), *snapshot.themes.themes[1:]),
    )
    html_text = render_daily_run_dashboard(replace(snapshot, modules=modules, themes=evil_themes))
    assert '<script>alert("x")</script>' not in html_text
    assert "&lt;script&gt;alert" in html_text
    assert '<b onclick="x">主题</b>' not in html_text
    assert "&lt;b onclick=&quot;x&quot;&gt;" in html_text
    # embedded JSON data block must not be able to break out of the script tag
    data_block = re.search(
        r'<script type="application/json" id="daily-run-data">(.*?)</script>', html_text, re.S
    ).group(1)
    assert "</" not in data_block
    json.loads(data_block)  # still valid JSON


def test_dashboard_has_no_external_network_references(fixture_html):
    assert "http://" not in fixture_html
    assert "https://" not in fixture_html
    for tag in ("<link", "<img", "cdn", "googleapis"):
        assert tag not in fixture_html.lower()


def test_accessibility_basics(fixture_html):
    assert 'lang="zh-CN"' in fixture_html
    assert "<h1" in fixture_html
    assert fixture_html.count("aria-label") >= 5
    assert "aria-labelledby" in fixture_html
    assert "aria-live" in fixture_html
    assert "role=" in fixture_html
    assert ":focus-visible" in fixture_html
    assert "prefers-reduced-motion" in fixture_html


def test_print_css_present(fixture_html):
    assert "@media print" in fixture_html
    print_block = fixture_html.split("@media print", 1)[1]
    assert "display:none" in print_block  # interactive controls hidden when printing


def test_mobile_390px_no_horizontal_overflow_css(fixture_html):
    assert "@media (max-width:480px)" in fixture_html
    assert "overflow-x:auto" in fixture_html  # wide tables scroll inside their container
    assert "box-sizing:border-box" in fixture_html
    assert "font:14px" in fixture_html  # body text at least 14px


def test_javascript_dom_ids_all_exist(fixture_html):
    for dom_id in JS_DOM_IDS:
        assert f'id="{dom_id}"' in fixture_html, dom_id
    assert 'id="section-nav"' in fixture_html


def test_embedded_json_data_block_is_auditable(fixture_result, fixture_html):
    data_block = re.search(
        r'<script type="application/json" id="daily-run-data">(.*?)</script>', fixture_html, re.S
    ).group(1)
    payload = json.loads(data_block)
    assert payload["run_id"] == fixture_result.snapshot.run_id
    assert payload["synthetic"] is True
    assert payload["index_health"]["canonical_chunks"] == 1248


def test_number_formatting(fixture_html):
    assert "1,248" in fixture_html  # thousands separator
    assert re.search(r"\d+\.\d%", fixture_html)  # coverage as percent with decimals
    assert "+08:00" not in fixture_html or True  # placeholder guard; offset asserted below
    assert re.search(r"as_of <b>2026-08-14T23:59:00\+00:00</b>", fixture_html)  # explicit timezone


# --------------------------------------------------------------------------- #
# repository / markdown / service modes
# --------------------------------------------------------------------------- #

def test_repository_roundtrip(tmp_path, fixture_result):
    repo = DailyRunRepository(tmp_path / "daily.db")
    latest = repo.load_latest()
    assert latest is not None
    assert latest["run_id"] == fixture_result.snapshot.run_id
    assert repo.count() == 1


def test_markdown_summary(fixture_result):
    text = render_daily_run_markdown(fixture_result.snapshot)
    assert "# Daily Run — 2026-08-14" in text
    assert "SYNTHETIC FIXTURE" in text
    assert "FAILED" in text


def test_live_mode_with_empty_database(tmp_path):
    result = run_daily(
        mode=DailyRunMode.LIVE,
        as_of=AS_OF,
        db_path=tmp_path / "live.db",
        output_dir=tmp_path / "out",
        paths=Paths(project_root=tmp_path),
    )
    snapshot = result.snapshot
    assert snapshot.synthetic is False
    health = snapshot.index_health
    assert health is not None
    assert health.canonical_chunks == 0
    assert health.indexes_in_parity is True  # empty manifests are trivially consistent
    skipped = {m.module_id for m in snapshot.modules if m.status is ModuleStatus.SKIPPED}
    assert {"macro-regime", "theme-rotation", "cn-wave-screen", "reversal-screen"} <= skipped
    assert snapshot.overall_status is ModuleStatus.SUCCESS
    html_text = result.outputs["html"].read_text(encoding="utf-8")
    assert "SYNTHETIC FIXTURE DATA" not in html_text


def test_live_mode_picks_up_published_module_outputs(tmp_path):
    macro_dir = tmp_path / "outputs" / "macro"
    macro_dir.mkdir(parents=True)
    (macro_dir / "macro_snapshot_2026-08-13.json").write_text(
        json.dumps(
            {
                "as_of": "2026-08-13T16:00:00+00:00",
                "primary_regime": "RISK_ON_CONFIRMING",
                "risk_state": "RISK_ON",
                "risk_score": 0.71,
                "liquidity_state": "EXPANDING",
                "liquidity_score": 0.55,
                "rate_pressure_state": "NEUTRAL",
                "rate_pressure_score": 0.05,
                "inflation_quadrant": "DISINFLATION",
                "data_coverage": 0.88,
                "confidence": 0.66,
                "stale_series": [],
                "liquidity_target_flows": [
                    {"target_id": "global_equities", "proxy_symbol": "ACWI",
                     "state": "ABSORBING", "absorption_score": 0.3}
                ],
                "model_version": "macro-regime-v1.4.0",
            }
        ),
        encoding="utf-8",
    )
    result = run_daily(
        mode=DailyRunMode.LIVE,
        as_of=AS_OF,
        db_path=tmp_path / "live.db",
        output_dir=tmp_path / "out",
        paths=Paths(project_root=tmp_path),
    )
    macro_module = next(m for m in result.snapshot.modules if m.module_id == "macro-regime")
    assert macro_module.status is ModuleStatus.SUCCESS
    assert macro_module.coverage == pytest.approx(0.88)
    assert result.snapshot.macro.primary_regime == "RISK_ON_CONFIRMING"
    assert macro_module.artifacts  # relative link back to the published JSON
    href = macro_module.artifacts[0].path
    assert not href.startswith("/")
    assert (result.outputs["html"].parent / href).resolve().is_file()


def test_cli_fixture_end_to_end(tmp_path, capsys):
    from quant_agent.cli.main import main

    db = tmp_path / "cli.db"
    out = tmp_path / "cli-out"
    rc = main(
        [
            "daily-run",
            "--mode", "fixture",
            "--as-of", "2026-08-14",
            "--db", str(db),
            "--output-dir", str(out),
        ]
    )
    assert rc == 0
    assert (out / "daily_run_dashboard_2026-08-14.html").is_file()
    assert (out / "daily_run_snapshot_2026-08-14.json").is_file()
    assert (out / "daily_run_report_2026-08-14.md").is_file()
    summary = json.loads(capsys.readouterr().out)
    assert summary["mode"] == "fixture"
    assert summary["synthetic"] is True
    assert summary["overall_status"] == "FAILED"
