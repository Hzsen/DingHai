# ADR 0010：MarketThemeState 与三层宏观解释输出

- 状态：Accepted for MVP
- 日期：2026-07-21
- Theme model：`market-theme-v1.0.0`

## Context

现有 `MacroRegime` 擅长描述未来数周至数月的流动性、风险与真实利率约束，但一个慢状态不足以回答最近 1–5 日市场究竟在交易什么。若直接让 LLM 从价格和新闻生成主题，又会把数值计算、规则分类和因果解释混在一起，降低可复现性。

## Decision

系统采用三个相互独立但可串联的层次：

1. `MacroRegime` 是慢层，继续由确定性宏观规则生成；
2. `MarketThemeState` 是快层，用标准化跨资产变化、宏观 snapshot 与 14 日历史窗口识别当前交易主题；
3. RAG / LLM 是解释层，只解释确定性主题与检索材料的关系，不重算指标，也不能静默改写 `theme_id`。

### Fast 1–5D state

快层优先使用每个序列的 `z_change_5d_252`，并组合真实利率、美元、黄金、股票宽度、商品、日元代理、加密资产和 liquidity target absorption。每个主题包含：

- 必须同时成立的 trigger；
- 可计数的 confirmation；
- conflicting evidence；
- 明确的 invalidation conditions；
- data coverage、persistence 和分解后的 confidence。

主题可以同时成立。压力覆盖类拥有显式高优先级，其余主题按 confidence、priority 和 confirmation 数量排序。没有主题通过触发与确认门槛时，系统输出 `NONE`，不强制生成叙事。

### Repricing 14D state

14 日层只识别物质性变化：

- net USD liquidity 变化至少 50bn；
- risk 或 real-rate pressure 变化至少 15 分；
- 单个 target absorption 变化至少 20 分。

它描述“市场约束正在如何变化”，不替代 1–5 日价格共振主题。

### Confidence semantics

theme confidence 是规则证据完整度，不是未来收益概率。它由 activation、confirmation ratio、data coverage、conflict penalty 和 persistence 合成，并封顶为 95%，避免把启发式规则显示为确定事实。

### Data proxies

除原有市场序列外，live adapter 增加 RSP、USO、CPER、FXY 和 IBIT，分别作为股票宽度、能源、铜、日元和加密资产代理。缺失代理只降低主题 coverage 或使相关主题无法触发，不使用虚构值填充。

### RAG and output lifecycle

每个 active macro document 增加按 horizon 划分的 theme chunks，以及独立的 evidence/invalidation chunk。JSON、Markdown 和 HTML 从同一组 `MarketThemeState` 确定性生成。HTML 提供 horizon 与候选主题切换，展示支持证据、冲突和失效条件。

主题切换会生成 `MARKET_THEME_SHIFT` 事件，可触发解释层分析。Kimi packet 包含 deterministic market themes；LLM只能提出解释、替代假设和反证。

## Non-goals and limitations

- 首版不是经过校准的预测模型；
- 主题规则和阈值尚需历史标签、敏感性分析与滚动回测；
- ETF 是价格代理，不代表真实资金申赎；
- correlated confirmations 不能被解释为完全独立证据；
- 本实现学习“硬触发 + 旁证 + 持续性 + 推翻条件”的通用架构，主题规则、代码和文字为项目独立实现，不复制第三方发行脚本。

## Verification

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q
PYTHONPATH=src .venv/bin/python -m quant_agent.cli.run_macro_regime \
  --output-dir outputs/macro-theme-demo --db /tmp/dinghai-macro-theme-demo.db
```
