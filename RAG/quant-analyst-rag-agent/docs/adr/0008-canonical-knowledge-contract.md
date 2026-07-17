# ADR 0008：RAG使用统一、时间可见的Canonical Knowledge Contract

- 状态：Accepted
- 日期：2026-07-15
- 范围：RAG Phase 1，领域契约；不包含持久化和索引实现

## Context

旧检索器直接读取 `data/docs/**/*.md`，`DocumentChunk` 只有标题、路径、section和文本。它无法按ticker、theme、document type、可靠性或 `available_at` 过滤，也没有版本、来源hash和indexable语义。与此同时，真实研究链路已经产生 weekly documents、state-change chunks、thesis updates 和 screening reports，但这些产物尚未使用同一种知识契约。

## Decision

新增 `domain.knowledge`，定义：

- `KnowledgeDocument`：稳定document ID、类型、内容、股票/题材关联、thesis ID、三类时间、状态、版本、来源、hash、可靠性、语言和JSON metadata；
- `KnowledgeChunk`：稳定chunk ID、document version、chunk type、section、ordinal、独立时间、hash、token count、indexable和metadata；
- `KnowledgeQuery`：query text、强制 `as_of`、ticker/theme/document type/status/event range/reliability filters和有界top-k；
- 文档、chunk、来源和可靠性枚举；
- 确定性content/JSON SHA-256 helper。

## Three time fields

- `event_time`：被描述事件发生的时间；
- `as_of`：文档所代表信息的截止时间；
- `available_at`：系统或研究员最早可以合法获知该文档/chunk的时间。

历史检索必须使用 `available_at <= query.as_of`。不能只看event time，因为财报、统计数据和研究总结可能在事件结束后才发布；也不能只看文档标题中的日期。

所有datetime强制timezone-aware，防止Asia/Shanghai、UTC和naive datetime静默比较。

## Document status and chunk indexability

文档状态与chunk索引资格是两层控制：

- `RETRACTED`、`SUPERSEDED`文档不可检索；
- `DRAFT`周文档仍可能包含已发生且应立即可检索的 `STATE_CHANGE`；
- `DRAFT_SUMMARY`强制 `indexable=False`；
- 最终是否可见同时要求document有效、document version匹配、chunk indexable且两者available_at不晚于query as-of。

这避免“为了隐藏未完成周总结而把当天重要风险事件也隐藏”的错误。

## Integrity rules

- `document_id` / `chunk_id` 非空且跨版本稳定；
- `version >= 1`，chunk必须声明对应document version；
- `content_hash` 必须与实际content/text一致；
- `source_hash` 必须是合法SHA-256；
- tickers/themes不得包含空值或重复值；
- metadata必须是有限、可JSON序列化的数据，拒绝NaN、set或任意Python对象；
- created/updated/available/as-of时间必须带时区；
- retracted文档不能通过query显式请求。

## Consequences

下一阶段SQLite Store可以直接用这些不变量保护写入、版本和索引队列；Temporal Retrieval可以在相似度计算前执行metadata和available-at过滤。代价是adapter必须显式转换旧weekly document和Markdown时间/来源字段，不能继续把缺失元数据当作无关细节。

本阶段刻意不实现数据库schema、chunking策略、embedding provider或检索排序，这些属于后续阶段，避免领域契约与具体存储技术耦合。
