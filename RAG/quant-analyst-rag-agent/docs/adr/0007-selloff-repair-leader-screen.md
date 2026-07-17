# ADR 0007：急跌修复期复用主升特征，增加 selloff-resilience 与 repair-confirmation

- 状态：Accepted for research pilot
- 日期：2026-07-14
- Feature version：`reversal-features-v1.0.0`
- Score version：`reversal-score-v1.0.0`

## Context

原有 WaveScore 擅长识别接近新高、持续趋势和放量突破，但市场急跌后，原强势股可能暂时跌破 MA20 或远离阶段高点。如果仍要求当日满足完整突破条件，会漏掉处在“主升 thesis 尚未完全失效、但正在左侧止跌修复”的股票；如果只按当日涨幅排序，又会把普涨反弹和超跌垃圾股混在一起。

## Decision

筛选拆为三层：

1. `MarketRepairRegime`：沪深300此前出现三日急跌或五日回撤，当前交易日上涨且收盘位于日内高位，才进入 `SELLOFF_REPAIR`。
2. `ReversalScore`：分别计算急跌抗跌性、当日修复确认、原主升质量、资金容量和风险扣分。
3. `focus_selected`：只保留此前20日 leader signature >= 80、急跌和修复日均跑赢市场、20日RS为正、距离120日高点不超过15%、全市场成交额前300且无风险标记的股票。

原主升因子没有删除，而是转换成 `prior_leader_score_20d`：检查急跌前20日是否曾具备趋势、突破、重复新高、相对强度和活跃度特征。新的修复因子回答“它是否在下跌中保持相对强势，并在今天重新获得价格接受”。

## Data path

- 新浪全A快照：当日 OHLCV、成交额与全市场成交额排名；
- 腾讯前复权日线：预筛股票的历史 OHLCV；
- 新浪沪深300：市场急跌与修复基准；
- cheap prefilter：高流动性核心池与强修复池并集，之后才下载历史；
- 输出：全量 CSV、Focus Candidates 和 Broader Repair Watchlist Markdown，并幂等写入 SQLite Gold 表 `gold_cn_reversal_screen_results`。

腾讯接口对少数股票返回的历史 volume 单位不稳定。系统比较历史中位量级和当日快照，明显不合理时把放量因子设为缺失并标记 `volume_unit_unreliable`，不把异常值当成真实缩量。

## 2026-07-14 pilot

- 全A有效快照 5,200 行；
- 历史预筛 500 只，成功率 100%；
- 493 只满足至少120个历史观测，feature coverage 为 98.6%；
- 沪深300此前五日回撤约 3.71%，当日上涨约 2.15%，收盘位于日内高点，状态为 `SELLOFF_REPAIR`；
- 结果属于当日横截面研究，不是经过历史验证的反转策略。

## Limitations

- 历史只覆盖 cheap prefilter 的500只股票，不是5,200只完整历史截面；
- 暂无历史 ST、行业成分和题材广度数据；
- `prior_leader_score_20d` 阈值来自案例启发，必须进行多次急跌事件回测；
- 当日修复不能证明反转成立，下一交易日仍需验证不回补长阳、成交可达和市场状态延续；
- 题材/叙事只用于对 Focus Candidates 做后置 enrichment，不参与本版数值分数。
