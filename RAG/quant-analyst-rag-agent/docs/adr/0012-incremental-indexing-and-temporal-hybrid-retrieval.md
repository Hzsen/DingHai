# ADR 0012：Incremental Indexing与Temporal Hybrid Retrieval

- 状态：Accepted
- 日期：2026-07-22
- 范围：RAG Phase 4，增量索引、离线vector baseline、point-in-time retrieval与fusion

## Context

Phase 3已经把Markdown、weekly research和Gold report reference迁入canonical `KnowledgeStore`，并留下1,947个pending index jobs。已有SQLite FTS5实现认识document version和`available_at`，但旧vector/hybrid实现仍从Markdown pickle读取，无法过滤future document、latest version、ticker、status或reliability。两条检索链如果拥有不同数据源和过滤规则，fusion会把不可比较、甚至时间上不合法的候选混在一起。

## Decision

Phase 4把canonical Store作为lexical和vector的共同事实源：

```text
KnowledgeStore latest/indexable chunks
  -> transactional outbox UPSERT/DELETE
  -> CanonicalLexicalIndex (SQLite FTS5)
  -> CanonicalVectorIndex (offline local hash vector)
  -> temporal/metadata filters in each candidate query
  -> chunk-level weighted reciprocal-rank fusion
  -> typed RetrievedEvidence
```

`KnowledgeIndexWorker`只有在lexical和vector操作都成功后才ACK job。任一backend失败时job进入FAILED；重试会再次执行两个幂等upsert/delete。索引可能在短暂失败窗口中处于degraded parity，但canonical filters仍阻止旧version、future chunk和retracted document成为证据；CLI status明确暴露parity和outbox状态。

## Lexical index

继续使用SQLite FTS5和中英文确定性tokenization。Phase 4修复了短alias误命中：原实现用substring判断`ai`，导致`repair`被错误扩展为`artificial_intelligence`；现在Latin alias使用word boundaries。

Tokenizer行为变化使index version从`canonical-lexical-v1.0.0`升级为`v1.1.0`。`reconcile()`比较stored index version和当前version：版本变化时全量重建；同版本时只补missing/content-changed rows并删除stale rows。正常研究日更不全量扫描正文，仍由outbox做增量更新。

## Local vector baseline

`CanonicalVectorIndex`是完全离线、可复现的feature-hashing baseline：

- 输入与FTS一致，包含title、section、chunk text、ticker、theme和document type；
- 中英文tokens映射到固定1,024维稀疏向量并L2 normalize；
- 为`主升浪/main uptrend`、`急跌修复/selloff repair`、`光模块/optical module`等少量领域概念增加跨语言concept token；
- vector和content hash、document version、embedding version一起保存在SQLite；
- cosine低于0.08的弱碰撞被丢弃。

它不冒充neural embedding，也不调用Kimi或外部API。目的在于先验证vector lifecycle、versioning、temporal filters和fusion contract。未来替换成本地embedding模型或provider embedding时，Store、outbox和query contract不变，只需要升级vector index version并reconcile。

## Temporal candidate filtering

Lexical与vector分别在相似度/排序前执行相同SQL约束：

1. `d.is_latest = 1`；
2. document status在请求范围内；
3. document和chunk均满足`available_at <= query.as_of`；
4. chunk必须indexable；
5. ticker/theme/document type/reliability filters；
6. effective event time范围。

Vector不是先对全库算相似度再过滤future rows，而是SQL先返回合法候选，再在Python计算当前pilot规模的cosine。这样point-in-time correctness不依赖最终fusion实现。

## Fusion

系统按`(chunk_id, document_version)`合并候选，不按document ID合并，因此一份周文档的state change和weekly summary可以分别引用。BM25与cosine原始分数的尺度不同，不直接相加；使用weighted reciprocal rank fusion：lexical权重0.55、semantic权重0.45、`rrf_k=60`。输出仍保留raw normalized lexical score、cosine semantic score、fusion score和match reason codes。

## Reconciliation and recovery

- Outbox是正常增量路径；
- lexical reconciliation处理tokenizer/schema升级与缺失索引；
- vector reconciliation补齐在vector backend建立前已经COMPLETED的历史jobs，并删除不再latest/indexable的stale vectors；
- CLI status同时报告canonical、lexical、vector计数和`indexes_in_parity`；
- search默认只查FINALIZED，显式`--include-drafts`才允许检索DRAFT document中的重要event chunks。

## Real validation result

2026-07-22真实数据库执行结果：

- lexical因tokenizer升级全量重建：删除49个旧entries，重建1,996个current entries；
- outbox claimed 1,947，completed 1,947，failed 0，pending 0；
- vector通过outbox写入1,947条，通过reconciliation补齐49条；
- canonical indexable chunks、lexical entries和vectors均为1,996，parity为true；
- 重复sync claimed 0，两个reconciliation均无变化。

真实temporal query验证：

- `SCREENING_REPORT`在2026-07-14 15:30 Asia/Shanghai不可见，返回0条；
- 同一查询在16:00可见，返回Gold table reference；
- 带`ticker=300308.SZ`的查询只返回中际旭创weekly/state-change evidence；
- 全量pytest 115个测试通过，其中包含future exclusion、semantic-only recall、ticker pre-filter、双索引version retirement、backend failure与retry。

## Consequences

优点是索引与知识版本统一、可以断网复现、future leakage在候选层被拦截、任意backend可以增量替换。代价是当前vector是研究工程baseline而非高质量语义模型，并且vector search在1,996条规模上采用filter-then-brute-force；当chunks增长到数万以上时需要ANN或SQLite vector extension，但不能牺牲metadata pre-filter和point-in-time语义。

下一阶段应建立带标注的retrieval evaluation set、context budget/deduplication和evidence packet，再允许Kimi只对检索后的少量证据做grounded synthesis。
