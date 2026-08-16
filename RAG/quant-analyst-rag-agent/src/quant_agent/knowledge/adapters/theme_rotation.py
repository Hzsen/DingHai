from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from domain.knowledge import (
    KnowledgeChunkType,
    KnowledgeDocumentStatus,
    KnowledgeDocumentType,
    KnowledgeReliability,
    KnowledgeSourceType,
    content_sha256,
)
from quant_agent.knowledge.adapters.base import KnowledgeChunkDraft, KnowledgeDocumentDraft


class ThemeRotationKnowledgeAdapter:
    source_name = "theme-rotation-sqlite-adapter-v1"

    def __init__(self, db_path: Path | str, *, latest_only: bool = True) -> None:
        self.db_path = Path(db_path)
        self.latest_only = latest_only

    def load(self) -> tuple[KnowledgeDocumentDraft, ...]:
        if not self.db_path.exists():
            return ()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='theme_rotation_snapshots'"
            ).fetchone()
            if exists is None:
                return ()
            limit = " LIMIT 1" if self.latest_only else ""
            snapshots = conn.execute(
                f"SELECT * FROM theme_rotation_snapshots ORDER BY as_of DESC{limit}"
            ).fetchall()
            output: list[KnowledgeDocumentDraft] = []
            for row in snapshots:
                metrics = conn.execute(
                    "SELECT * FROM theme_rotation_metrics WHERE snapshot_id=? ORDER BY score DESC,theme_id",
                    (row["snapshot_id"],),
                ).fetchall()
                attributions = conn.execute(
                    "SELECT * FROM theme_rotation_attributions WHERE snapshot_id=? ORDER BY score DESC,attribution_id",
                    (row["snapshot_id"],),
                ).fetchall()
                alerts = conn.execute(
                    "SELECT * FROM theme_rotation_alerts WHERE snapshot_id=? ORDER BY alert_id",
                    (row["snapshot_id"],),
                ).fetchall()
                output.append(self._draft(row, metrics, attributions, alerts))
        return tuple(output)

    def _draft(
        self,
        row: sqlite3.Row,
        metrics: list[sqlite3.Row],
        attributions: list[sqlite3.Row],
        alerts: list[sqlite3.Row],
    ) -> KnowledgeDocumentDraft:
        as_of = datetime.fromisoformat(str(row["as_of"]))
        payload = json.loads(str(row["payload_json"]))
        snapshot = payload["snapshot"]
        top_themes = metrics[:5]
        summary = "；".join(
            f"{item['label']} {float(item['score']):.0f}分/{item['state']}，原因 {item['reason']}"
            for item in top_themes if item["score"] is not None
        ) or "没有形成有效主题分数。"
        theme_evidence = "\n".join(
            f"- {item['label']} ({item['theme_id']}): 5D/20D/60D相对收益 "
            f"{_fmt(item['relative_5d'])}/{_fmt(item['relative_20d'])}/{_fmt(item['relative_60d'])}，"
            f"绝对5D {_fmt(item['absolute_5d'])}，成交额 {_fmt(item['volume_ratio'], 'x')}，"
            f"广度 {_fmt(item['breadth'])}，分数 {_fmt(item['score'])}，状态 {item['state']}，原因 {item['reason']}。"
            for item in metrics
        )
        attribution_evidence = "\n".join(
            f"- {item['label']}: {float(item['score']):.0f}分；核心信号 {item['core_signal']}；"
            f"失效条件 {item['invalidation']}。"
            for item in attributions
        )
        alert_text = "\n".join(
            f"- {item['label']}: {float(item['previous_value']):.1f} → {float(item['current_value']):.1f}，"
            f"阈值 {float(item['threshold']):.0f}。"
            for item in alerts
        ) or "- 本次没有新的阈值穿越。"
        quality = ", ".join(snapshot.get("quality_flags", [])) or "none"
        limitations = (
            f"数据覆盖 {float(row['data_coverage']):.0%}；质量标记 {quality}。"
            "相对收益、量能、广度和分数均由市场数据确定性计算；归因分数是人工规则的证据强度，"
            "不是已核验资金流或因果事实，也不构成投资建议。"
        )
        content = (
            f"# 科技主题资金轮动 — {as_of.date().isoformat()}\n\n"
            f"## 摘要\n\n{summary}\n\n## 主题证据\n\n{theme_evidence}\n\n"
            f"## 轮动归因\n\n{attribution_evidence}\n\n## 告警\n\n{alert_text}\n\n"
            f"## 数据质量与限制\n\n{limitations}"
        )
        document_id = f"theme-rotation/{as_of.date().isoformat()}"
        chunks = (
            KnowledgeChunkDraft(
                f"{document_id}/summary", KnowledgeChunkType.SUMMARY, "主题轮动摘要", summary, 0,
                as_of, as_of, True, {"benchmark": str(row["benchmark_symbol"])},
            ),
            KnowledgeChunkDraft(
                f"{document_id}/themes", KnowledgeChunkType.EVIDENCE, "主题横截面证据", theme_evidence, 1,
                as_of, as_of, True, {"metric_count": len(metrics)},
            ),
            KnowledgeChunkDraft(
                f"{document_id}/attribution", KnowledgeChunkType.EVIDENCE, "轮动归因证据", attribution_evidence, 2,
                as_of, as_of, True, {"attribution_count": len(attributions)},
            ),
            KnowledgeChunkDraft(
                f"{document_id}/alerts", KnowledgeChunkType.STATE_CHANGE, "阈值穿越告警", alert_text, 3,
                as_of, as_of, True, {"alert_count": len(alerts)},
            ),
            KnowledgeChunkDraft(
                f"{document_id}/limitations", KnowledgeChunkType.RISK, "数据质量与限制", limitations, 4,
                as_of, as_of, True, {"quality_flags": snapshot.get("quality_flags", [])},
            ),
        )
        tickers = sorted({member for item in snapshot["themes"] for member in item["members"]})
        themes = tuple(str(item["theme_id"]) for item in snapshot["themes"])
        source_hash = content_sha256(str(row["payload_json"]))
        return KnowledgeDocumentDraft(
            document_id=document_id, document_type=KnowledgeDocumentType.THEME_RESEARCH,
            title=f"科技主题资金轮动 — {as_of.date().isoformat()}", content=content,
            tickers=tuple(tickers), themes=themes, thesis_id=None, event_time=as_of, as_of=as_of,
            available_at=as_of, status=KnowledgeDocumentStatus.FINALIZED,
            source_type=KnowledgeSourceType.SYSTEM_DERIVED,
            source_uri=f"sqlite://theme_rotation_snapshots/{row['snapshot_id']}", source_hash=source_hash,
            reliability=KnowledgeReliability.DERIVED, language="zh-CN", created_at=as_of, updated_at=as_of,
            metadata={
                "adapter": self.source_name, "snapshot_id": str(row["snapshot_id"]),
                "benchmark_symbol": str(row["benchmark_symbol"]), "model_version": str(row["model_version"]),
                "reference_model_version": str(row["reference_model_version"]),
                "data_coverage": float(row["data_coverage"]),
                "availability_semantics": "us_market_close",
            }, chunks=chunks,
        )


def _fmt(value: object, suffix: str = "%") -> str:
    return "na" if value is None else f"{float(value):.2f}{suffix}"
