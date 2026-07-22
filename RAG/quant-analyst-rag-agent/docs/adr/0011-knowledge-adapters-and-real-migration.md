# ADR 0011：Knowledge Adapters与真实知识迁移

- 状态：Accepted
- 日期：2026-07-21
- 范围：RAG Phase 3，来源适配、canonical version解析和真实知识迁移

## Context

RAG Phase 1定义了统一的 `KnowledgeDocument / KnowledgeChunk` contract，Phase 2建立了SQLite Knowledge Store和transactional outbox。但已有研究资产仍分散在Markdown、`weekly_documents`、thesis note文件和Gold筛选表中。直接让retriever分别读取这些来源，会让时间语义、版本、来源可靠性和去重规则散落在查询路径里，也无法证明重复执行和部分失败的行为。

## Decision

增加两层边界：

1. `KnowledgeAdapter`只解释一个来源，把来源记录转换成没有canonical version的`KnowledgeDocumentDraft / KnowledgeChunkDraft`；
2. `KnowledgeMigrationService`查询当前latest version，跳过相同source/content，给变化分配新version，并以一个adapter snapshot为单位调用`KnowledgeStore.ingest_batch()`原子发布。

Adapter不写索引，也不调用LLM。发布和embedding解耦，migration只创建outbox job。这样断网可以完整重放，Kimi和embedding provider故障不会污染canonical knowledge。

## Source-specific semantics

### Static Markdown

- document ID由`data/docs`下的相对路径稳定生成；
- 按Markdown heading分块，超长section才继续切分；
- filesystem mtime明确记录为`availability_semantics`，不伪装成外部新闻发布时间；
- factor definition映射为`FACTOR_DEFINITION`，其他研究笔记映射为`THEME_RESEARCH`。

### Weekly research

- 直接迁移SQLite中的1,692份`weekly_documents`和2,574个`weekly_document_chunks`；
- 周内`DRAFT_SUMMARY`保持不可索引，重要`STATE_CHANGE`保留原来的indexable语义；
- finalized周摘要才进入outbox；
- 交易日事件时间以Asia/Shanghai 15:00表示，可获得时间使用收盘后10分钟；
- 原始daily observation IDs、source run和source schema version留在metadata中。

### Thesis update

- 从`outputs/thesis_notes/**/*.md`提取ticker和source thesis；
- `State Change / Numeric Evidence / Factor Status / Risk Notes`分别映射为语义chunk type；
- 目录为空是合法状态，不生成空document或job。

### Screening report

Markdown日报不是numeric source of truth。Adapter只生成一个可发现的`SCREENING_REPORT`指针，内容告诉retriever去查询`gold_cn_reversal_screen_results`，并记录Gold primary key、dataset hash、score version和报告路径。候选分数、排名和feature JSON不复制进knowledge text，避免SQLite Gold与RAG正文产生双重真相。

## Idempotency and failure behavior

同一document的source hash、content、status和关联键未变化时，migration直接skip，不创建ingestion run、document row或index job。变化时创建更高canonical version，旧版由Store标记`SUPERSEDED`。

一个adapter返回的所有draft先完成canonical对象构造，再作为单个batch提交。任意非法hash、时间、chunk identity或SQLite异常会回滚整个adapter batch，上一版latest knowledge保持不变。不同adapter是独立批次，因此失败来源可以单独重试，成功来源不需要重放。

## Real migration result

2026-07-21首次执行：

- 9份现有static Markdown与canonical Store内容相同，全部skip；
- 1,692份weekly documents和2,574个weekly chunks完成迁移，其中1,946个indexable chunks产生outbox jobs；
- thesis note目录当前为空，迁移0份；
- 2026-07-14 A股修复筛选建立1份Gold reference document和1个index job；
- 总计发现1,702份document，实际新增1,693份、2,575个chunks、1,947个jobs。

立即重复运行时，1,702份全部skip，新增document、chunk和job均为0。

## Consequences

下游retrieval不再需要知道周表、文件目录或Gold报告格式；它只读取canonical contract。来源增加时只新增adapter，不改Store与retriever。代价是必须明确维护每种来源的`available_at`语义，而且源表只保存latest weekly snapshot时，无法凭空恢复已丢失的历史draft versions。

下一阶段消费outbox，验证BM25/向量增量索引、point-in-time filter和hybrid ranking；本ADR不把1,947个pending jobs的消费结果当作Phase 3完成条件。
