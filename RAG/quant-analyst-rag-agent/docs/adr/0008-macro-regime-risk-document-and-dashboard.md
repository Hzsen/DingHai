# ADR 0008：宏观规则模型、时效性文档与资产方向仪表盘

- 状态：Accepted for MVP
- 日期：2026-07-15
- Model version：`macro-regime-v1.1.0`

## Context

宏观分析的输入异步更新：美债收益率和 ETF 是日频，美联储资产负债表和 TGA 是周频，数据还有发布延迟。若按自然日简单 forward fill，系统会把“沿用旧值”误写成“今日新信息”；若每天生成永久可索引的长文档，RAG 又会召回大量已过期结论。

本模块只回答研究状态：风险、美元流动性、真实利率压力、通胀补偿，以及股指、美债价格和黄金在三个 horizon 下的规则方向。它不输出投资建议，也不预测目标价格。

## Decision

### 1. Point-in-time observation contract

每条 observation 保存 `series_id`、`observation_date`、`available_at`、`value`、`unit`、`source`、`batch_id` 和 `fetched_at`。特征计算只读取 `available_at <= as_of` 的记录。

真实 MVP 使用三类 adapter：

- FRED：真实利率、名义利率、BEI、信用利差、广义美元代理、WALCL、TGA、RRP；
- CBOE：VIX 与 VIX3M；
- AkShare/Sina：SPY、QQQ、IWM、KRE、SOXX、GLD、IEF、TLT 收盘价。

`DTWEXBGS` 只作为美元方向 proxy，保存 `BROAD_DOLLAR_PROXY_NOT_ICE_DXY` 标记；MOVE 缺失会降低 coverage，而不是用虚构值补齐。

### 2. Deterministic rule models

模型分成四块，避免一个总分掩盖不同传导路径：

1. `rate pressure`：真实利率位置与 5/20 日变化、长端名义利率、黄金确认；
2. `inflation quadrant`：将 10Y nominal 拆成 real yield 与 BEI，并检查异步发布时间造成的恒等式偏差；
3. `risk`：信用利差、VIX term structure、利率波动、美元和 IWM/KRE 相对强弱；
4. `liquidity`：`ΔWALCL - ΔTGA - ΔRRP`，再由美元与信用条件确认。

所有美元流量在合并前统一成 `millions_usd`。这是强制单位契约；否则 billions 与 millions 混算会产生方向正确但幅度错误的假信号。

V1.1 将主输出从“股指涨跌方向”调整为两层 liquidity flow：

- source flow 直接使用 `ΔWALCL - ΔTGA - ΔRRP`，展示 Fed、TGA、RRP 对 20 日美元流动性的注入或抽离金额；
- target absorption 使用系统流动性冲击、跨资产相对表现、真实利率、信用与风险传导，估计资金被 large cap、AI/半导体、small cap、banks、Treasury、gold 或 dollar/cash 吸收的相对强弱。

target absorption 是 transparent transmission proxy，不是 ETF 申赎份额或托管账户意义上的真实净流入。价格仅作为“是否完成传导”的确认变量，不再作为 dashboard 的主要输出。

### 3. Time-sensitive KnowledgeDocument lifecycle

每个 finalized 日快照生成一份 `MacroRiskDocument` 和五个短 chunks：风险、流动性、股指、美债和黄金。新 finalized document 发布时：

- 前一份 active document 标记为 `SUPERSEDED`；
- 前一份 chunks 设置 `indexable=0`；
- 新文档设置 `valid_until=as_of+1 day`；
- 相同 document ID 重跑使用 upsert，不生成重复行。

覆盖率低于 CLI 的 publish threshold 时直接停止，上一份 finalized document 继续有效，避免部分 provider 失败污染当前 Gold/RAG 状态。

### 4. Visualization is derived output

HTML dashboard、Markdown 和 JSON 都由同一个 `MacroSnapshot` 确定性生成。可视化不拥有业务逻辑，因此 UI 数字和 RAG 文档不会出现两套计算口径。

仪表盘优先展示 source decomposition、net system impulse、target absorption ranking、传导阻力与 supporting/conflicting evidence；同时显示 coverage、confidence、quality flags 和 stale series。

## Why

- adapter 把第三方接口变化隔离在数据边界；
- `available_at` 防止未来数据泄漏，也准确表达“今天沿用上周值”；
- 四个子模型便于解释冲突，例如“流动性扩张但真实利率仍压制久期资产”；
- TTL + supersede 让 RAG 只召回当前宏观结论，历史文档仍可审计；
- deterministic dashboard 让 LLM 不参与日常数字计算，节省 token 并降低幻觉。

## Current limitations

- 当前美元输入是 broad-dollar proxy，不是 ICE DXY；
- MOVE 尚未接入，coverage 上限因此低于 100%；
- FRED CSV 不提供严格的 real-time vintage history，`available_at` 使用保守发布假设；
- 规则分数是研究假设，尚未完成跨周期历史验证和阈值敏感性分析；
- 流动性与资产表现是条件关联，不应被表述为单变量因果关系。

## Reproduce

Fixture、无网络：

```bash
PYTHONPATH=src .venv/bin/python -m quant_agent.cli.run_macro_regime
```

刷新真实数据：

```bash
PYTHONPATH=src .venv/bin/python -m quant_agent.cli.run_macro_regime --live --as-of 2026-07-15
```

使用已原子发布的 SQLite observation cache：

```bash
PYTHONPATH=src .venv/bin/python -m quant_agent.cli.run_macro_regime --live --reuse-cache --as-of 2026-07-15
```
