# ADR 0004：Phase 1 使用 provider-neutral contract 与原子化 Bronze/Silver/Gold warehouse

- 状态：Accepted for pilot
- 日期：2026-07-14
- 版本：`phase1-warehouse-v1.0.0`

## 决策

所有外部数据源实现统一 `DataSource.fetch(DataRequest) -> DataBatch`。记录必须同时带 `event_time` 与 `available_at`；batch 带 `batch_id`、source、requested/fetched time 和结构化错误。

错误分为 transient、permanent、schema 和 data-quality。只有 transient error 使用 exponential backoff。原始记录先写 Bronze；通过 schema、finite-value、OHLC、non-negative 和 duplicate-key 检查后，Silver 与 Gold 才在一个 SQLite transaction 内发布。失败 run 留在审计表，上一版 Gold 不变。

## 幂等与增量

Bronze 使用 source、symbol、time 和 canonical payload 的 SHA-256；Silver/Gold 使用业务主键 upsert。市场增量从 Gold watermark 向前回看七天，以容纳近期修订。重复运行不增加同内容 Bronze 或业务主键行。

## Pilot 数据

- A 股：Phase 0 已由 AkShare 获取的 8 只研究股票与沪深300缓存；
- 宏观：FRED `WALCL`、`WTREGEN`、`RRPONTSYD`；
- 数据库：`data/processed/phase1_research.db`。

FRED 当前 `available_at` 使用显式 frequency lag 近似，不是 ALFRED vintage。Phase 4 必须升级为真实发布日/vintage 语义。

## 后果

好处是 provider 故障不会直接污染研究表，所有 Gold 数字能追溯到 run 和 Bronze payload。代价是多了一层 schema、质量检查和 lineage 存储，但这是可复现研究所必需的复杂度。
