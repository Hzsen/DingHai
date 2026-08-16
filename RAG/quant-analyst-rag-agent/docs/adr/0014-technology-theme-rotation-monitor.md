# ADR 0014：科技主题资金轮动监控与 RAG 发布

- 状态：implemented
- 模型版本：`tech-theme-rotation-v2.6.1r-python.1`
- 参考规则：授权提供的 Pine `v2.6.1r`

## 背景

项目已有宏观跨资产 `MarketThemeState`，但缺少科技行业内部的横截面轮动监控。参考指标包含
13 个主题、5/20/60 日相对收益、趋势、成交额倍数、宽度、五段评分、二次归因加减分、
10 个轮动剧本和阈值穿越告警。

原 Pine 源文件包含限制再分发声明，因此仓库不复制原文件；这里只保存经过授权迁移的领域模型、
公式实现、默认主题目录和规则版本元数据。

## 决策

新增 `quant_agent.theme_rotation` vertical slice：

```text
CSV/Parquet 或 AkShare US 日线
  → point-in-time 对齐
  → ETF/等权篮子指数
  → 主题基础指标与分数
  → 跨主题归因与二次加减分
  → 状态/原因/阈值告警
  → SQLite + JSON/Markdown/HTML
  → ThemeRotationKnowledgeAdapter
  → canonical KnowledgeStore / hybrid retrieval
```

### 主题口径

- ETF 主题直接使用 ETF 收盘价；ETF 宽度只做代理价格是否站上自身 20 日均线的占位值：70/30。
- 篮子按有效成员的一日收益等权合成指数；宽度是站上成员自身 20 日均线的比例。
- 所有相对收益均以配置中的 `QQQ` 为默认基准。
- 成交额是价格乘成交量，篮子取有效成员均值，再除以自身 20 日均值。
- 缺失日允许沿用最近有效市场数据，但当日成员覆盖率单独披露，不把缺失伪装成完整数据。

### 基础评分

- 趋势最多 30：比价站上快/慢均线、快均线相对 5 日前抬升。
- 资金最多 34：5D/20D 相对收益为正、5D 相对改善、5D 快过 20D 平均节奏。
- 量能最多 20：相对上涨且成交额达到平量/放量门槛。
- 宽度最多 15：达到 50%/70%。
- 不过热 8：60D 相对收益和相对 MA50 偏离均未超过阈值。
- 原始理论上限 107，最终截断至 100。

### 确定性边界

数值、状态和原因优先级全部由规则引擎计算。RAG 只检索、引用和解释已发布的 snapshot，不能重算
分数，也不能把“可能原因”写成已验证的资金流因果事实。KnowledgeAdapter 发布五类 chunk：摘要、
主题证据、归因证据、阈值告警、数据质量与限制。

## 持久化

同一个 `phase1_research.db` 增加：

- `theme_rotation_runs`
- `theme_rotation_price_observations`
- `theme_rotation_snapshots`
- `theme_rotation_metrics`
- `theme_rotation_attributions`
- `theme_rotation_alerts`

snapshot 使用稳定主键；同一日期、模型与基准重复运行幂等覆盖数值层。canonical KnowledgeStore
使用按日 document id 和内容 hash 管理版本。

## 运行

文件输入必须至少包含 `date,symbol,close,volume`：

```bash
quant-agent theme-rotation --prices /path/to/us_daily.parquet --as-of 2026-08-01
quant-agent index sync
quant-agent search "软件和半导体是否正在强弱对调" --theme software
```

安装 research extra 后可以直接刷新：

```bash
quant-agent theme-rotation --live --as-of 2026-08-01
```

## 限制

- 默认 ETF 宽度是代理占位，不是 ETF 官方成分宽度。
- 默认篮子成员是配置快照，不代表历史任意时点的官方指数成分。
- 阈值和归因权重来自经验规则，尚未完成 walk-forward 回测校准。
- 价格相对强弱不能证明真实 ETF 申赎、机构持仓或资金流因果。
