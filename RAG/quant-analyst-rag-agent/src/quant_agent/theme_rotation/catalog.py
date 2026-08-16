from __future__ import annotations

import json
from pathlib import Path

from domain.theme_rotation import ThemeDefinition, ThemeInstrumentKind, ThemeRotationConfig


def default_theme_definitions() -> tuple[ThemeDefinition, ...]:
    etf = ThemeInstrumentKind.ETF
    basket = ThemeInstrumentKind.EQUAL_WEIGHT_BASKET
    return (
        ThemeDefinition("semiconductor", "半导体", etf, "SMH", ("SMH",), "美国上市半导体龙头 ETF 代理。"),
        ThemeDefinition("software", "软件", etf, "IGV", ("IGV",), "北美软件行业 ETF 代理。"),
        ThemeDefinition("cloud", "云计算", etf, "SKYY", ("SKYY",), "云计算主题 ETF 代理。"),
        ThemeDefinition("cybersecurity", "网络安全", etf, "CIBR", ("CIBR",), "网络安全主题 ETF 代理。"),
        ThemeDefinition("robotics", "机器人", etf, "BOTZ", ("BOTZ",), "全球机器人与自动化 ETF 代理。"),
        ThemeDefinition("ai_broad", "AI综合", etf, "AIQ", ("AIQ",), "AI 与大数据主题 ETF 代理。"),
        ThemeDefinition(
            "ai_apps", "AI应用软件", basket, "BASKET",
            ("PLTR", "APP", "SNOW", "DDOG", "MDB"), "AI 应用软件成员的日收益等权合成指数。",
        ),
        ThemeDefinition(
            "optical_components", "光通信零部件", basket, "BASKET",
            ("COHR", "LITE", "AAOI"), "光通信与光器件成员的日收益等权合成指数。",
        ),
        ThemeDefinition(
            "ai_network", "AI网络", basket, "BASKET",
            ("ANET", "CIEN", "CSCO"), "AI 网络设备成员的日收益等权合成指数。",
        ),
        ThemeDefinition(
            "memory_storage", "储存", basket, "BASKET",
            ("MU", "WDC", "STX", "SNDK"), "存储芯片与设备成员的日收益等权合成指数。",
        ),
        ThemeDefinition(
            "data_center_infra", "数据中心电力散热", basket, "BASKET",
            ("VRT", "ETN", "PWR", "CEG", "GEV"), "数据中心电力、能源与散热成员的日收益等权合成指数。",
        ),
        ThemeDefinition(
            "hyperscalers", "云巨头/AI买方", basket, "BASKET",
            ("MSFT", "AMZN", "GOOGL", "META"), "云厂商及 AI 资本开支买方的日收益等权合成指数。",
        ),
        ThemeDefinition(
            "ai_hardware", "AI硬件卖方", basket, "BASKET",
            ("NVDA", "AVGO", "AMD", "MRVL"), "AI 芯片与硬件卖方的日收益等权合成指数。",
        ),
    )


def load_theme_rotation_catalog(
    path: Path | str | None = None,
) -> tuple[ThemeRotationConfig, tuple[ThemeDefinition, ...]]:
    if path is None:
        return ThemeRotationConfig(), default_theme_definitions()
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    config_payload = dict(payload.get("config", {}))
    config = ThemeRotationConfig(**config_payload)
    themes = tuple(
        ThemeDefinition(
            theme_id=str(item["theme_id"]),
            label=str(item["label"]),
            kind=ThemeInstrumentKind(str(item["kind"])),
            proxy_symbol=str(item["proxy_symbol"]),
            members=tuple(str(symbol) for symbol in item["members"]),
            description=str(item.get("description", "")),
        )
        for item in payload["themes"]
    )
    _validate_required_themes(themes)
    return config, themes


def _validate_required_themes(themes: tuple[ThemeDefinition, ...]) -> None:
    required = {
        "semiconductor", "software", "cloud", "cybersecurity", "robotics", "ai_broad", "ai_apps",
        "optical_components", "ai_network", "memory_storage", "data_center_infra", "hyperscalers",
        "ai_hardware",
    }
    ids = {item.theme_id for item in themes}
    missing = required - ids
    if missing:
        raise ValueError(f"theme catalog missing attribution dependencies: {sorted(missing)}")
    if len(ids) != len(themes):
        raise ValueError("theme catalog contains duplicate theme ids")
