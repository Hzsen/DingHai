# ADR 0009：14日宏观重定价窗口与事件触发 Kimi inference

- 状态：Accepted for MVP
- 日期：2026-07-16
- History window：14 calendar days，通常约 10–11 个 weekday snapshots
- Prompt version：`macro-pricing-inference-v1.0.0`

## Context

单日宏观截面只能回答当前状态，不能回答市场在过去半个月如何改变定价。财政支出、Treasury cash、AI capex、真实利率和跨资产资金虹吸可能在较短窗口内快速改变传导结构，因此季度或月度切片过慢；仅使用 1D 变化又容易被噪音驱动。

系统需要在严格 point-in-time 前提下重建一个接近半个月的连续窗口，让规则检测 source flow、risk constraint 和 target absorption 的方向变化，再由 Kimi提出“市场可能正在计价什么”的可证伪 hypothesis。

## Decision

### 1. Window semantics

- 默认回看 14 calendar days；
- 每个 weekday 在 23:59 UTC 重建一份 snapshot；
- 典型窗口包含 10–11 个 points；
- 特征仍保留 1D、5D、20D，14D 负责比较两个 point-in-time states；
- 所有历史点只使用 `available_at <= snapshot.as_of` 的 observations。

14日不是经济周期，而是高敏感环境下的 repricing detection window。raw observations 仍永久保存，未来可用 60D/1Y 做历史位置和 analog research。

### 2. Persisted history

新增 SQLite 表：

- `macro_snapshots_history`：每日系统流动性、risk、real-rate pressure 和完整 payload；
- `macro_target_history`：每日每个 target 的 absorption score 与 state；
- `macro_change_events`：确定性变化事件；
- `macro_pricing_inferences`：Kimi JSON hypothesis、model、prompt version 和 cache key。

所有表使用稳定主键和 upsert，重复运行不产生重复行。

### 3. Deterministic change detection

首版触发条件：

- net USD liquidity 14D 变化绝对值不小于 50bn，或正负号反转；
- risk / real-rate pressure 变化不小于 15 分；
- target absorption 变化不小于 15 分或 state 改变；
- liquidity 强扩张但至少一半 risk targets 没有正 absorption；
- large-cap absorption 显著超过 AI/semiconductor，形成 selective transmission divergence。

`POSSIBLE_AI_CAPEX_OR_DURATION_CONSTRAINT` 只是 reason code，不能直接变成事实结论。Trump fiscal spending、AI capex crowding-out 等具体解释必须由 numeric evidence 与提供的 official context 同时支持。

### 4. Kimi boundary

Kimi 不读取 HTML，不读取完整 observations，不重新计算任何指标，也不调用同花顺。它只读取：

- 当前 snapshot；
- 14日首尾变化；
- 最多 11 个 compact daily history points；
- deterministic change events；
- 最多 3 条、每条 1000 字符的 official global macro context。

Kimi 输出只允许以下 hypothesis taxonomy：monetary policy、inflation、growth、fiscal/Treasury supply、liquidity plumbing、credit stress、geopolitical/commodity、positioning unwind、AI capex crowding-out、insufficient evidence。

每个 hypothesis 必须包含 supporting evidence、contradicting evidence、confidence 和 invalidation conditions。输出不构成投资建议，不预测价格。

### 5. Token and cache policy

- 没有 material change event 时不调用 Kimi；
- packet、contexts、model 和 prompt version 共同生成 SHA-256 cache key；
- temperature 为 0，max tokens 为 800；
- cache hit 直接复用；
- Kimi失败不影响 numeric snapshot、history 和 dashboard 发布；
- API key 只通过 `os.getenv("MOONSHOT_API_KEY")` 读取，任何输出不包含具体值。

### 6. Visualization

Dashboard 增加：

- 14D net USD liquidity trend；
- 14D risk 与 real-rate constraint trend；
- 每个 target 的 absorption small multiple；
- deterministic trigger list；
- 有 Kimi结果时显示 dominant hypothesis、flow interpretation、target rotation、supporting/contradicting evidence。

图表不会把 target absorption proxy 伪装成 audited ETF net fund flow。

## Failure semantics

- history coverage 不足时停止重建，不写不完整窗口；
- Kimi provider 失败时 dashboard 显示 pending/failed 状态，但保留确定性结果；
- 历史回跑不能 supersede 更晚的 active inference；
- prompt schema、risk type 或 packet ID 不合法时拒绝保存 Kimi结果。

## Commands

使用已发布 cache 重建14日窗口：

```bash
PYTHONPATH=src .venv/bin/python -m quant_agent.cli.run_macro_regime \
  --live --reuse-cache --as-of 2026-07-15 --history-days 14
```

在环境变量已导出后触发 Kimi：

```bash
PYTHONPATH=src .venv/bin/python -m quant_agent.cli.run_macro_regime \
  --live --reuse-cache --as-of 2026-07-15 --history-days 14 --with-kimi
```

可用最多三个 `--kimi-context path/to/official-note.txt` 提供官方政策 context。

