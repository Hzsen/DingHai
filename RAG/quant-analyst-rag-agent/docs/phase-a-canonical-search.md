# Phase A：Canonical Knowledge Search

状态：Implemented（2026-07-17）

Phase A 把版本化 SQLite `KnowledgeStore` 接入一个统一、typed、可离线运行的搜索入口。它暂时不负责生成答案；它负责把“在指定时间点合法可见的证据”稳定找出来，为后续 FastAPI、Web UI、numeric evidence 和 Kimi synthesis 提供同一个 application boundary。

## 1. 数据流

```text
data/docs/**/*.md
  → idempotent Markdown migration
  → KnowledgeDocument / KnowledgeChunk
  → knowledge_index_jobs (transactional outbox)
  → KnowledgeIndexWorker
  → SQLite FTS5 Chinese/English lexical index
  → RAGQueryService
  → quant-agent search
```

为什么不让 CLI 直接扫描 Markdown：

- 路径、版本、status、reliability 和 `available_at` 无法由临时文件扫描统一约束；
- 新产生的 macro/thesis/private-derived documents 本来就在 SQLite，目录扫描看不到；
- outbox 将“文档发布成功”和“索引更新成功”解耦，索引失败不会破坏 canonical document；
- CLI、未来 API 和 Web UI 能共享完全相同的检索规则。

## 2. Typed query contract

`RAGQueryRequest` 明确规定：

- `query_text`：非空研究问题；
- `as_of`：必须 timezone-aware，用于 point-in-time filter；
- `mode`：search、answer、causal、macro 或 A 股研究；
- ticker/theme/document type/reliability/event-time filters；
- 默认只检索 `FINALIZED`，禁止请求 `RETRACTED`；
- `top_k` 上限防止无界 context。

返回的每个 `RetrievedEvidence` 都包含稳定的 document/chunk/version identity、时间字段、来源、可靠性、分项 score 和 reason codes。后续 LLM 只能引用这些 evidence IDs，不能自己发明来源。

## 3. Markdown 一次性迁移

迁移器使用相对路径生成稳定 identity，例如：

```text
data/docs/factor_definitions/liquidity.md
→ markdown/factor_definitions/liquidity
```

行为语义：

- 内容 hash 未变化：跳过，不新增 document/chunk/job；
- 内容变化：创建更高 version，旧 version 标记 `SUPERSEDED`；
- 按 Markdown heading 切 section，超长 section 再按字符窗口切分；
- source Markdown 保留，不删除；SQLite 是 query 的 canonical read model；
- 整批 publish 在事务内完成，部分失败不会产生半成品。

## 4. 中文 lexical index

SQLite FTS5 保存持久 lexical row。应用层 tokenizer 同时生成：

- English word、number、ticker、snake-case token；
- Chinese unigram、bigram、trigram；
- 少量金融 aliases，例如 `BOK/韩国央行/Bank of Korea`、`资金价格/funding cost`、`实际利率/real yield`。

这样不依赖在线 embedding，也不把 jieba 字典作为唯一召回条件。搜索 SQL 会再次 join canonical tables，并强制检查：

```text
is_latest = true
indexable = true
status allowed
document.available_at <= query.as_of
chunk.available_at <= query.as_of
metadata/time filters
```

索引里的过期 row 因此不能绕过 canonical policy。

## 5. Outbox worker

`KnowledgeStore.ingest*()` 与 `knowledge_index_jobs` 在同一 SQLite transaction 中提交。worker 使用 at-least-once 语义：

1. 原子 claim `PENDING` jobs；
2. UPSERT 前校验 chunk 仍是 latest、indexable 且 content hash 一致；
3. superseded/stale job 转成安全删除；
4. 成功标记 `COMPLETED`，异常标记 `FAILED`；
5. 相同 row UPSERT/DELETE 可重复执行，因此 crash 后可以 retry。

## 6. 使用命令

安装项目命令入口：

```bash
.venv/bin/python -m pip install . --no-deps --no-build-isolation
```

迁移、同步和查看状态：

```bash
quant-agent index migrate-markdown
quant-agent index sync
quant-agent index status
```

中文搜索：

```bash
quant-agent search "韩国加息是否导致亚洲半导体下跌？" \
  --as-of 2026-07-17 \
  --top-k 6
```

机器可读输出：

```bash
quant-agent search "资金价格与实际利率" --json
```

默认数据库为 `data/processed/phase1_research.db`，可用 `KNOWLEDGE_DB_PATH` 或每个命令的 `--db` 覆盖。搜索默认先做轻量 migration check 和 outbox sync；使用 `--no-bootstrap` 可以只读已有索引。

命令应从项目根目录运行；如果从其他目录启动，请设置 `QUANT_AGENT_PROJECT_ROOT` 指向项目根目录。

## 7. 当前验收结果与边界

验收结果：

- 8 个既有 Markdown documents 迁移为 27 个 chunks；
- 连同既有 canonical documents，共同步 33 个 FTS5 chunks；
- 第二次迁移：8 个文件全部 unchanged，0 个新 job；
- 第二次 worker：0 claimed；
- 中文问题能召回 `韩国加息与中日半导体下跌：事件窗口验证`；
- 全量测试 100 passed。

Phase A 只有 lexical search，不调用 Kimi，也不声称完成 semantic retrieval。下一阶段应在同一 contract 后加入 local multilingual embeddings、RRF、query-run audit 和 FastAPI/Web UI，而不是再建一条旁路。
