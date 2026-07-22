# ADR 0013：Retrieval Evaluation、Evidence Packet 与 Grounded Synthesis

- 状态：Accepted
- 日期：2026-07-22
- 范围：RAG Phase 5

## Context

Phase 4 已经建立 canonical incremental index 与 temporal hybrid retrieval，但“能返回结果”不等于“召回正确证据”，更不等于可以把任意 Top K 全部发送给 LLM。直接将检索结果拼接进 prompt 有四个风险：重复 chunks 浪费 token、长文挤掉关键数字、未来或越权证据污染回答，以及模型生成无法追溯的事实。

## Decision

Phase 5 在 retrieval 与 Kimi 之间增加两个 hard boundaries：离线可复现的 labeled evaluation，以及 provider-bound `EvidencePacket`。

```text
RAGQueryRequest(as_of + filters)
  -> Temporal Hybrid Retrieval
  -> labeled retrieval evaluation
  -> exact / near context deduplication
  -> document/chunk/token limits
  -> EvidencePacket
  -> explicit --use-kimi only
  -> JSON grounded synthesis
  -> citation validation
  -> cache or extractive fallback
```

## Labeled retrieval evaluation

`data/evaluation/retrieval_cases.json` 首版包含 8 个标注 case，覆盖 factor definition、历史研究、宏观事件、A 股 weekly state change、Gold screening report，以及报告发布前必须返回空结果的 point-in-time negative case。

每个 case 固定 query、`as_of`、filters、Top K、relevant/forbidden document IDs。评估输出 pass rate、Recall@K、MRR、temporal violations、filter violations 与 forbidden hits。它不是用 LLM 评分，因此断网可以复现；新增 retrieval algorithm 或 tokenizer 时必须先跑这一集合。

真实 1,996-chunk 索引副本结果：8/8 passed，Recall@K 1.0，MRR 0.875，temporal/filter/forbidden violations 均为 0。

评估过程中发现 SQLite FTS5 的 `bm25()+canonical JOIN` 在当前 Python SQLite planner 下出现病态重复扫描。修复后 FTS5 只负责 materialized candidate generation；`available_at` 与 metadata hard filters 后再执行 deterministic token relevance 排序。这样性能问题不会通过减少 temporal checks 来“解决”。

## EvidencePacket policy

Provider 只能看到 `EvidencePacket`，不能看到原始数据库或未选中的 retrieval results：

- numeric evidence 优先进入预算且原值不改写；基础数据已超过预算时 fail closed；
- exact hash dedup 与 token Jaccard near-dedup；
- 最多 6 documents，每 document 最多 2 chunks；
- 单 context 最多 1,600 chars，总预算默认 2,400 estimated tokens；
- `available_at > as_of` 直接丢弃；secret-like context 不发送；
- 每个 dropped evidence 记录本地 reason code，但 dropped 正文不进入 provider payload；
- canonical packet hash 形成稳定 `packet_id`，相同 evidence boundary 可缓存。

token estimator 是 dependency-free 的保守近似，不声称等于 Kimi tokenizer。它的工程目的不是精确计费，而是提供稳定上界和回归测试；未来可替换为 provider tokenizer，但 packet policy 不变。

## Kimi grounded mode

默认 `quant-agent answer` 只返回 extractive evidence，不调用 Kimi。只有显式 `--use-kimi` 才读取 `MOONSHOT_API_KEY` 环境变量并调用 provider。

Kimi 输入只包含 EvidencePacket，`temperature=0`、`max_tokens=800`，要求 JSON-only，并禁止投资建议、交易指令、目标价与未来价格预测。每条 claim 必须引用本次 packet 中存在的 `evidence_id`；空 citation 或未知 ID 会使 response validation 失败。provider 失败时返回 extractive fallback，不把失败变成无依据回答。

成功结果按 `packet_id + model + prompt_version` 写入 `.cache/grounded_synthesis/`。相同 packet cache hit 不再调用 Kimi，从而把 token 消耗限定在“新证据边界 + 明确请求 synthesis”的场景。

## Consequences

优点是 retrieval quality 有稳定基线、Kimi context 可审计、token 支出有硬上限、claims 能追溯到版本化 chunks、断网仍有可用结果。代价是首版 evaluation set 较小，near-dedup 使用 lexical Jaccard，citation validator 只能验证“引用存在”，不能自动证明 claim 与证据语义完全一致。

下一阶段应扩充 hard negatives 与行业/时间切片标注，加入 claim-evidence entailment evaluation 和 query-run audit；在此之前不应让 Kimi扩大检索范围或自主浏览数据源。
