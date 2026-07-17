# ADR 0005：Phase 2～3 采用透明 WaveScore 与 t+1 open 事件回测

- 状态：Accepted for pilot, not accepted as performance evidence
- 日期：2026-07-14
- Feature version：`wave-features-v1.0.0`
- Score version：`wave-score-v1.0.0`
- Backtest version：`wave-backtest-v1.1.0`

## Phase 2 决策

先做 point-in-time eligibility，再计算 trend、breakout、momentum、market-relative strength、volume 和 risk-quality component。硬过滤与扣分分开，输出 exclusion reasons、top reasons、risk flags、feature/score version 和 source run。

当前股票池只有 8 个事后研究案例，因此横截面字段命名为 `amount_rank_pilot` 与 `rs_rank_pilot_pct`，不能描述为全市场排名。历史 ST 状态尚不可得，tradability Gold 表把 `status_known=0` 和降级说明写出，而不是假装数据完整。

## Phase 3 协议

- 默认只在每个 ISO calendar week 的最后一个交易日生成组合信号；
- 周内仍每日更新行情、特征、可交易性和 thesis 风险状态；
- `t` 日收盘后生成信号；
- `t+1` 开盘成交；
- 默认持有 5 个交易日，并在下一周信号后开盘调仓；
- 日线一字涨停或无成交量视为不可买；
- 成本按双边换手扣除；
- 报告基准、Sharpe、最大回撤、hit rate、turnover、OOS 日期分段；
- sensitivity 覆盖 holding days 1/5/20 与 minimum score 45/55/65；敏感性分支使用按持有期采样，避免 20 日持有窗口在每周信号下机械重叠。

## 关键限制

股票池来自已知正负案例，存在严重 universe selection bias。2025 年后的日期分段不是真正独立样本，因为 universe 本身由事后信息选择。因此当前收益、OOS 收益和 sensitivity 只能验证计算和无未来函数协议，不能证明策略有效。进入正式研究前必须接入按日期生效的全市场证券主表、历史 ST/停牌、行业成分和公司行动 vintage。
