# ADR 0006：周频研究文档采用 daily facts + weekly derived document

- 状态：Accepted for pilot
- 日期：2026-07-14
- Document version：`weekly-research-document-v1.0.0`

## Context

当前策略以一周左右为主要持仓和决策周期，但行情、成交额、风险标记和 thesis validation 仍需每日更新。如果每天为每只股票生成一份完整 KnowledgeDocument 并重新 embedding，会重复保存大量相同背景，也会诱发不必要的 Kimi 调用。

## Decision

- SQLite 中的结构化 daily observation 是事实源，保留每日粒度和 point-in-time 时间语义；
- 每只股票每个 ISO week 只有一份 `WeeklyResearchDocument`；
- 当周文档为 `DRAFT`，每天用确定性指标覆盖更新，并引用 daily observation ID，不复制五份日文档；
- 已结束周为 `FINALIZED`，生成一条可索引 `WEEKLY_SUMMARY`；
- draft summary 不进入向量索引；重要状态变化单独生成小型、可索引 `STATE_CHANGE` chunk；
- 只有重要 thesis 状态变化才设置 `llm_update_required`，常规日更不调用 Kimi；
- 重跑时默认只重建本周和上一周。相同 source hash 保持版本号，内容确实变化时才递增版本；
- 周频组合信号不取消日频风险检查。行情异常、数据质量失败或 thesis 失效仍可在周内触发降级或风险标记。

## Why

这把三个不同频率的问题分开：daily facts 保证风险及时性，weekly document 对齐研究和持仓节奏，event chunk 保存真正有检索价值的状态变化。下游 RAG 检索的是稳定摘要和稀疏事件，不必在五份高度重复的日文档中竞争召回。

## Measured pilot result

截至 2026-06-30，8 只 pilot 股票产生 1,692 份周文档、2,574 个 chunks，其中 1,946 个可索引。若每日都生成可索引文档，基线为 7,998 份；当前方案减少约 75.7% 的 embedding 单元。已有数据库重复运行只重建最近两周的 16 份文档，不增加重复行。

该比例只衡量当前 pilot 的索引单元，不等于固定的 token 或账单节省率；实际成本还受 chunk 长度、embedding provider 和状态变化频率影响。

## Failure semantics

- 周文档是可重建 derived data，不替代 Gold daily facts；
- draft 不作为完整周结论参与普通检索；
- 写入使用 document ID 和 chunk ID 幂等覆盖，失败不会删除上一版 daily Gold；
- 只有 source hash 改变才产生新版本，便于审计“同一周为何被修改”。
