# ADR 0009：Canonical Knowledge使用SQLite Store与Outbox式Index Jobs

- 状态：Accepted
- 日期：2026-07-16
- 范围：RAG Phase 2，持久化、版本、查询与索引任务；不包含文档adapter和检索排序

## Context

Phase 1已定义时间可见、版本化的 `KnowledgeDocument`、`KnowledgeChunk` 和 `KnowledgeQuery`。真实系统还需要保证重复ingestion不增加行、同一版本不被静默覆盖、批次失败不留下半份知识、chunk变化能可靠通知BM25/Vector索引器，并且历史查询不能读取 `available_at` 晚于query as-of的内容。

直接在ingestion事务里调用embedding provider会把数据库一致性与外部API可用性耦合：API超时可能导致文档写入失败，数据库提交后API失败又可能造成知识与索引不一致。因此使用SQLite持久化事实，并在同一个事务里写入index job outbox。

## Schema

- `knowledge_ingestion_runs`：source、STAGED/PUBLISHED/FAILED、批次行数、任务数和错误类型；
- `knowledge_documents`：以 `(document_id, version)` 为主键，`is_latest` 保证每个document只有一个当前版本；
- `knowledge_chunks`：以 `(chunk_id, document_version)` 为主键并外键关联document；
- `knowledge_index_jobs`：有序、幂等的UPSERT/DELETE任务及PENDING/PROCESSING/COMPLETED/FAILED/CANCELLED状态。

## Atomic ingestion

ingestion run先记录为STAGED，文档、chunks、旧版本状态和index jobs在一个 `BEGIN IMMEDIATE` 事务内发布。任意bundle验证、版本冲突或SQLite错误都会回滚整个知识批次，随后仅把run标为FAILED。上一版document和索引任务不受污染。

同一 `(document_id, version)` 被视为不可变：完全相同的重跑是幂等no-op；正文、来源、时间或metadata变化必须使用更高version，否则抛出 `VersionConflictError`。Chunking策略可以在document版本不变时调整，Store通过chunk diff产生增量任务。

## Version transition

新版本必须大于latest version。发布时旧版本标记为SUPERSEDED并取消latest资格。旧版indexable chunks产生DELETE，新版indexable chunks产生UPSERT。查询只返回latest document，因此即使异步索引尚未完成，Store本身不会把旧知识作为当前证据返回。

## Index job outbox

job ID由operation、chunk identity/version和完整chunk state hash确定；重复ingestion使用 `INSERT OR IGNORE`，不会产生重复embedding任务。State hash排除ingestion run ID，因此来源相同内容的重跑不会误判变化，但metadata、available-at或正文变化会触发UPSERT。

队列会合并尚未执行的过期状态：旧版UPSERT仍是PENDING时若新版本到达，旧job直接CANCELLED，不先embedding再DELETE；如果旧UPSERT已经PROCESSING、COMPLETED或FAILED，则保守生成DELETE，再UPSERT新版。

Worker通过 `BEGIN IMMEDIATE` 原子claim任务；失败job可显式retry，attempt count保留；PROCESSING worker崩溃后可按timezone-aware stale cutoff重新入队。只有PROCESSING job能进入COMPLETED/FAILED，防止重复ack。

## Temporal query

`query_chunks()`使用参数化SQL，顺序上先执行：

1. latest document；
2. document status；
3. document/chunk均满足 `available_at <= query.as_of`；
4. chunk indexable；
5. ticker/theme/document type/reliability/event-time filters。

SQLite `json_each` 对tickers/themes执行ANY匹配。该方法只提供经过metadata过滤的候选集合，不负责BM25/vector分数；检索排序属于后续Phase。

所有datetime在写入和SQL参数绑定前统一转换为UTC ISO-8601；否则带 `+08:00` 与 `+00:00` 的合法时间字符串不能安全使用字典序比较。领域对象仍保留timezone-aware语义，Store反序列化统一返回UTC。

## Consequences

优点是数据库发布与外部embedding解耦、可离线重放、可审计每次变更、失败可恢复，并把未来信息泄漏挡在检索候选层。代价是需要index worker消费outbox，且SQLite适用于当前单机研究规模；未来高并发时可保留契约迁移到PostgreSQL和独立队列。

本阶段不迁移旧Markdown、weekly documents或thesis notes，也不修改现有BM25/vector索引；这些属于RAG Phase 3 adapters与incremental index worker。
