# RAG Research Note: 韩国加息与中日半导体下跌

- As of: 2026-07-17 13:10 Asia/Shanghai
- Research question: 今天中日股指及半导体走弱，是否与韩国加息正相关？
- Local viewpoint retrieved: `mv/benjamin-usachi/funding-cost-duration-ai-20260714`
- Evidence boundary: local abstracted viewpoint + public global data + live A-share index snapshot
- Kimi called: false

## Conclusion

韩国加息与亚洲风险资产下跌在事件窗口内同向发生，但这不能证明统计意义上的正相关，更不能证明韩国加息是中日半导体下跌的主要原因。若变量定义为“韩国政策利率变化”与“半导体收益率”，观察到的方向实际上是负相关：利率上升、半导体收益率下降。

当前证据更支持以下因果排序：

1. 全球 AI/半导体拥挤交易与高估值在较高资金价格下重新定价；
2. 美股芯片股隔夜下跌、TSMC 强业绩但更高 Capex 仍被卖出，向亚洲供应链传导；
3. 油价、地缘冲突和全球长端利率压力共同强化 discount-rate shock；
4. 韩国加息对韩国本地高久期、杠杆和半导体权重资产形成放大，但更像共同资金价格变化的 confirmation/amplifier，而不是中日市场的独立首因。

Direct BOK-causality confidence: low to medium.

Common funding-cost / AI-valuation repricing confidence: high.

## Point-in-time Evidence

### Bank of Korea

- 2026-07-16，韩国央行将 Base Rate 从 2.50% 上调 25bp 至 2.75%，七名委员一致同意。
- 官方理由包括：半导体出口和投资推动增长、通胀持续高于目标、住房与家庭债务风险。
- 韩国央行表示需要维持与进一步加息一致的政策立场。
- 该次 25bp 加息高度预期：Reuters 调查中 37 名经济学家仅一人没有预测本次加息，因此 surprise component 有限。

### Event timeline

- 7月16日：KOSPI 下跌约 6.4%，上海约跌 1.8%，东京约跌 2.8%；韩国加息对首尔市场构成额外压力。
- 7月17日：韩国因公共假日休市，但日本、台湾和中国风险资产继续下跌。韩国没有新的可交易价格，跨市场抛售仍然延续。
- 7月17日午间：日本 Nikkei 跌幅接近 5%，台湾股市跌逾 5%，主要压力来自芯片和 AI 相关资产。
- 7月17日 13:10 A 股 live snapshot：上证指数 -1.915%，沪深300 -2.668%，科创50 -4.915%，创业板指 -5.248%。
- 前一晚美国芯片股指数跌逾 4%；TSMC 美国存托凭证在强于预期的利润后仍下跌，市场同时关注更高 Capex 计划。

## RAG Viewpoint Validation

本地观点卡提出：系统流动性正常不等于长期资金便宜；财政融资、AI Capex 和基础设施融资共同增加久期供给，而稳定买盘、官方吸收和杠杆吸收不足时，资金价格可能压制长久期资产估值。

### Supported

- `AI_DURATION_VALUATION_CONSTRAINT`：强盈利信息没有阻止芯片股下跌，说明分母端和仓位因素在短期压过分子端。
- `SELECTIVE_EQUITY_LIQUIDITY`：宽基指数跌幅小于科创50和创业板，资金并非均匀撤出，而是集中削减高久期成长暴露。
- `REAL_YIELD_PRESSURE`：全球利率和油价压力为高估值 AI 资产提供共同 discount-rate channel。

### Not established

- 仅凭一次韩国加息无法估计“韩国政策利率—中日半导体收益”的稳定相关系数。
- 由于加息高度预期，不能把当天全部跌幅当成 monetary-policy surprise effect。
- 尚未分离美国芯片隔夜收益、TSMC Capex surprise、油价、美元、美国实际利率和韩国政策 surprise 的边际贡献。

## Counterfactual Test

如果韩国加息是主要原因，应更可能观察到：

- 韩国资产明显弱于其他市场，且跌幅集中在决议公布后的 surprise window；
- KRW、韩国利率和韩国半导体的反应强于美股/台湾/日本共同因子；
- 在控制 SOX、TSMC、美国实际利率和油价后，韩国加息 surprise 仍能解释中日芯片收益。

今天韩国休市而日本、台湾和中国继续下跌，且前一晚美股芯片股已显著下跌，这个 counterfactual 更支持全球共同因子，而不是韩国本地政策单因子。

## Next Quantitative Test

建立 2020–2026 Bank of Korea event-study dataset：

```text
event_date
bok_actual_change_bp
bok_expected_change_bp
bok_surprise_bp
kospi_semiconductor_return_0_1d
nikkei_semiconductor_return_0_1d
china_semiconductor_return_0_1d
sox_overnight_return
us_10y_real_yield_change
dxy_change
oil_change
```

用 `bok_surprise_bp` 而不是实际加息幅度做解释变量，并分别估计 0D、1D、3D 窗口。样本较少时不追求复杂模型，先报告 sign consistency、median response 和 bootstrap interval。

## Sources

- Bank of Korea, Monetary Policy Decision, 2026-07-16: https://www.bok.or.kr/eng/bbs/E0000634/view.do?depth=400069&menuNo=400069&nttId=11062944&oldMenuNo=400007&programType=newsDataEng&relate=Y
- AP, Asian shares and AI-led selloff, 2026-07-17: https://apnews.com/article/stocks-markets-ai-iran-trump-rates-65449e9565fba441a617f9517e097f5a
- AP, global AI stock selloff and Korea decision, 2026-07-16: https://apnews.com/article/stock-markets-iran-inflation-oil-e1c646be279423406586c67c79e738e4
- Reuters poll syndicated by Investing.com, BOK expectation: https://www.investing.com/news/economy-news/south-korea-central-bank-to-raise-rates-for-first-time-in-over-three-years-on-july-16-reuters-poll-4789633
- A-share live index snapshot: AkShare `stock_zh_index_spot_sina`, fetched 2026-07-17 13:10 Asia/Shanghai.
