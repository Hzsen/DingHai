# TradingView 自动画缠论结构 Pine 脚本需求规格 v0.3

> 文档状态：工程实现基线（Normative Baseline）
>
> v0.2 新增的第 14–21 节属于规范性约束；v0.3 新增第 22 节。若早期描述与后续章节冲突，以编号更后的章节为准。任何实现若有意偏离规范，必须在代码注释和阶段交付说明中列出偏离项，不得静默改变算法。

## 0. 目标

在 TradingView 中用 Pine Script 编写一个自动识别并绘制缠论结构的指标脚本。

脚本目标不是一次性实现完整主观缠论，而是将缠论拆解为可计算、可验证、可调参的结构识别系统：

1. K线包含处理；
2. 分型识别；
3. 笔识别；
4. 线段识别；
5. 中枢识别；
6. 背驰/类背驰提示；
7. 买卖点候选提示；
8. 多级别结构扩展。

第一阶段只实现 1–4，第二阶段实现中枢，第三阶段再做背驰和买卖点。

---

## 1. Pine Script 工程边界

### 1.1 执行模型

Pine Script 不是一次性批处理历史数据，而是从历史第一根 bar 到当前 bar 逐根执行。

因此所有结构必须设计为：

* 每根 bar 到来时增量更新；
* 不依赖未来数据；
* 已确认结构尽量不重绘；
* 临时结构可以更新，但必须明确标记为 tentative；
* confirmed 与 tentative 必须分层管理。

### 1.2 可视化对象限制

脚本需要控制线段、标签、box、polyline 的数量。

设计原则：

* 只绘制最近 N 个笔、线段、中枢；
* 使用对象池复用 line / label / box；
* 不在每根 bar 无脑创建新对象；
* 历史结构使用数组保存必要字段，不一定全部绘制。

### 1.3 Pine 中的数据结构

核心结构用 array 保存。

由于 Pine 不适合复杂嵌套对象，优先使用并行数组或轻量 type：

* fractalIndex[]
* fractalPrice[]
* fractalType[]
* biStartIndex[]
* biEndIndex[]
* biStartPrice[]
* biEndPrice[]
* biDirection[]

如果使用 Pine v6 type，需要确保 Claude 实现时不要过度抽象。

---

## 2. 缠论结构层级

### 2.1 原始 K 线 Raw Bar

每根 TradingView bar 包含：

* index = bar_index
* time
* open
* high
* low
* close
* volume

原始 K 线不直接用于分型，必须先进入包含处理模块。

---

## 3. 包含关系处理

### 3.1 包含定义

对于相邻两根 K 线 A、B：

若满足：

* A.high >= B.high 且 A.low <= B.low，则 A 包含 B；
* A.high <= B.high 且 A.low >= B.low，则 B 包含 A。

则称二者存在包含关系。

### 3.2 方向判定

包含处理必须依赖当前合成 K 线序列的方向。

方向定义：

* up：最近两个非包含合成 K 线高低点整体抬高；
* down：最近两个非包含合成 K 线高低点整体降低；
* unknown：初始化阶段或无法判断。

### 3.3 合并规则

若当前方向为 up：

* mergedHigh = max(A.high, B.high)
* mergedLow = max(A.low, B.low)

若当前方向为 down：

* mergedHigh = min(A.high, B.high)
* mergedLow = min(A.low, B.low)

若方向 unknown：

* 暂不强行识别分型；
* 或使用最近非包含关系确定方向后再回补。

### 3.4 合成 K 线字段

每根合成 K 线至少需要保存：

* startBarIndex
* endBarIndex
* high
* low
* highBarIndex
* lowBarIndex
* directionContext

注意：合成 K 线的 high/low 可能来自不同原始 bar，因此必须保存 highBarIndex 与 lowBarIndex，不能只保存 endBarIndex。

---

## 4. 分型定义

### 4.1 顶分型

在包含处理后的合成 K 线序列中，连续三根合成 K 线 A、B、C：

B.high > A.high 且 B.high > C.high，且 B.low > A.low 且 B.low > C.low。

则 B 构成顶分型。

工程简化版可使用：

B.high > A.high 且 B.high > C.high。

但默认采用严格定义。

### 4.2 底分型

连续三根合成 K 线 A、B、C：

B.low < A.low 且 B.low < C.low，且 B.high < A.high 且 B.high < C.high。

则 B 构成底分型。

工程简化版可使用：

B.low < A.low 且 B.low < C.low。

但默认采用严格定义。

### 4.3 分型确认延迟

分型至少需要右侧一根合成 K 线确认。

因此：

* 当前 bar 上不能确认自己是分型；
* 只有当 C 出现后，才能确认 B 是否为分型；
* 分型确认天然存在延迟。

### 4.4 分型字段

每个分型保存：

* fractalType: TOP / BOTTOM
* fractalIndex: 对应 B 的极值所在原始 bar index
* fractalPrice: 顶分型使用 high，底分型使用 low
* sourceMergedKIndex
* confirmed = true

---

## 5. 笔定义

### 5.1 笔的基本定义

一笔由相邻的反向分型构成：

* 底分型 -> 顶分型：向上一笔；
* 顶分型 -> 底分型：向下一笔。

### 5.2 最小间隔要求

两端分型之间必须满足最小 K 线间隔。

默认规则：

* 两个分型之间至少间隔 4 根合成 K 线；
* 或者起点分型与终点分型之间至少存在 1 根独立合成 K 线。

该参数必须可配置：

* minMergedBarsBetweenFractals = input.int(4)

### 5.3 破坏与替换规则

当连续出现同类型分型时，不直接生成笔，而是保留更极端的那个：

* 连续顶分型：保留价格更高者；
* 连续底分型：保留价格更低者。

当出现反向分型但不满足成笔条件时：

* 不生成 confirmed 笔；
* 可作为 candidate fractal 暂存；
* 后续若出现更极端同类分型，替换 candidate。

### 5.4 笔确认

一笔只有在终点反向分型确认且满足间隔条件后才 confirmed。

confirmed 笔原则上不重绘。

但最后一笔可以有两种模式：

* strict mode：最后一笔也必须 confirmed；
* realtime mode：允许绘制 tentative bi，随行情更新。

默认实现 strict mode，后续添加 realtime mode。

### 5.5 笔字段

每一笔保存：

* biStartIndex
* biEndIndex
* biStartPrice
* biEndPrice
* biDirection: UP / DOWN
* startFractalId
* endFractalId
* confirmed

---

## 6. 线段定义

### 6.1 基本线段

线段由至少三笔构成。

一个上涨线段通常表现为：

* 上笔高点不断抬高；
* 下笔低点不破关键低点；
* 直到出现反向破坏。

一个下跌线段相反。

### 6.2 工程化线段初版

第一版不要实现复杂特征序列与线段破坏，只做简化版：

* 每三笔形成一个线段候选；
* 若第 3 笔的终点突破第 1 笔终点方向，则确认线段延续；
* 若反向笔破坏前一线段关键点，则线段结束。

### 6.3 线段字段

* segmentStartIndex
* segmentEndIndex
* segmentStartPrice
* segmentEndPrice
* segmentDirection
* startBiId
* endBiId
* confirmed

---

## 7. 中枢定义

### 7.1 中枢基本定义

中枢由至少连续三段走势类型重叠构成。

工程第一版可以用“三笔重叠区间”近似：

给定连续三笔或三段的价格区间：

* intervalHigh_i = max(startPrice, endPrice)
* intervalLow_i = min(startPrice, endPrice)

重叠区间：

* zg = min(intervalHigh_1, intervalHigh_2, intervalHigh_3)
* zd = max(intervalLow_1, intervalLow_2, intervalLow_3)

若 zg >= zd，则存在中枢。

### 7.2 中枢字段

* centerStartIndex
* centerEndIndex
* zd
* zg
* gg = max(highs)
* dd = min(lows)
* directionContext
* level: BI_CENTER / SEGMENT_CENTER
* confirmed

---

## 8. 背驰与买卖点：后续阶段

### 8.1 背驰暂不在第一版实现

第一版只预留接口，不实现强判断。

原因：

* 背驰依赖力度比较；
* 力度可用 MACD 面积、价格幅度、斜率、成交量等多种代理变量；
* 主观解释空间大，容易让 Claude 一步写歪。

### 8.2 后续可选力度指标

* MACD histogram sum
* price change / bar count
* volume-weighted movement
* slope
* ATR-normalized movement

---

## 9. 显示需求

### 9.1 必须显示

* 合成 K 线分型点；
* confirmed 笔；
* confirmed 线段；
* 最近 N 个中枢 box；
* 最后一笔 candidate/tentative 状态。

### 9.2 可配置显示

* showFractals
* showBi
* showSegments
* showCenters
* showDebugLabels
* showTentativeBi

### 9.3 对象数量控制

默认：

* max_lines_count = 500
* max_labels_count = 500
* max_boxes_count = 200

内部参数：

* maxVisibleBi = 80
* maxVisibleSegments = 40
* maxVisibleCenters = 20

---

## 10. 开发阶段拆分

### Phase 1：包含处理 + 分型

Claude 任务：

1. 写 Pine v6 indicator skeleton；
2. 实现合成 K 线数组；
3. 实现包含合并；
4. 实现顶/底分型识别；
5. 只画分型 label；
6. 加 debug 开关。

验收标准：

* 不画笔；
* 只看分型是否稳定；
* 分型确认后不应频繁重绘；
* label 数量受控。

### Phase 2：笔识别

Claude 任务：

1. 在 confirmed fractal 基础上生成笔；
2. 实现同类型分型替换；
3. 实现最小间隔过滤；
4. 绘制 confirmed bi line；
5. 可选绘制 tentative bi。

验收标准：

* 笔必须连接顶底交替分型；
* 连续顶/连续底不能乱连；
* 最后一笔是否重绘由参数控制。

### Phase 3：线段识别

Claude 任务：

1. 基于 confirmed bi 生成 segment；
2. 使用简化三笔规则；
3. 绘制 segment 粗线；
4. 输出 debug table。

验收标准：

* 线段数量明显少于笔；
* 线段方向与笔结构一致；
* 不追求完全正统，只追求稳定工程版。

### Phase 4：中枢识别

Claude 任务：

1. 基于笔或线段识别中枢；
2. 绘制 box；
3. 支持中枢延伸；
4. 标注 zd / zg。

验收标准：

* box 不无限生成；
* 中枢边界稳定；
* 中枢重叠区间计算正确。

---

## 11. Claude 开发规则

每次只允许 Claude 完成一个阶段。

禁止一次性实现全部功能。

每个阶段必须输出：

1. 完整 Pine v6 代码；
2. 当前阶段做了什么；
3. 哪些功能故意没有做；
4. 如何在 TradingView 验证；
5. 已知限制。

Claude 不允许：

* 修改已通过验收的核心定义；
* 偷懒跳过包含处理；
* 直接用 ta.pivothigh / ta.pivotlow 代替缠论分型，除非作为 fallback debug；
* 无限制创建 label / line；
* 把未确认结构画成 confirmed；
* 混淆原始 K 线 index 与合成 K 线 index。

---

## 12. 已确认的设计决策

### 12.1 分型定义

采用宽松定义。

顶分型：

* 在包含处理后的三根合成 K 线 A、B、C 中；
* 若 B.high > A.high 且 B.high > C.high；
* 则 B 为顶分型。

底分型：

* 在包含处理后的三根合成 K 线 A、B、C 中；
* 若 B.low < A.low 且 B.low < C.low；
* 则 B 为底分型。

不强制要求 B.low 同时高于两侧，也不强制要求 B.high 同时低于两侧。

原因：

* Pine 中先做工程稳定性；
* 宽松分型更接近实盘可用结构；
* 后续可以通过参数切换 strict / loose。

---

### 12.2 成笔最小间隔

采用 4 根合成 K 线作为默认成笔间隔。

参数：

```pine
minMergedBarsBetweenFractals = input.int(4, "Min merged bars between fractals")
```

成笔要求：

* 起点分型与终点分型必须类型相反；
* 两个分型之间至少间隔 4 根合成 K 线；
* 不满足间隔时，不生成 confirmed bi，只更新 candidate fractal。

---

### 12.3 最后一笔实时变动

允许最后一笔实时变动。

因此系统中必须区分：

1. confirmed bi：已经确认的笔，原则上不重绘；
2. tentative bi：最后一笔/候选笔，可以随着新高或新低实时延伸、替换、失效。

显示规则：

* confirmed bi 使用实线；
* tentative bi 使用虚线或半透明线；
* 不允许把 tentative bi 当作 confirmed bi 参与高级别结构确认；
* 只有 confirmed bi 才能进入线段、中枢、背驰计算。

---

### 12.4 多级别显示设计

第一版不做无限递归多级别，而是固定为三层：

1. 当前级别 current level；
2. 次级别 lower level；
3. 高一级别 higher level。

例如：

* 当前图表为日线；
* 当前级别 = 日线；
* 次级别 = 4H 或 1H，需要由参数指定；
* 高一级别 = 周线。

再例如：

* 当前图表为 15m；
* 当前级别 = 15m；
* 次级别 = 5m 或 1m；
* 高一级别 = 1H。

由于 TradingView Pine 的多周期数据必须通过 request.security 获取，且低级别数据在高级别图上存在压缩与性能限制，因此多级别系统必须分阶段实现：

Phase A：只计算并显示当前图表级别。

Phase B：允许用户手动指定 lowerTf 与 higherTf，并分别调用同一套 Chanlun engine。

Phase C：支持三层同时显示，但每层必须使用独立颜色与独立对象池。

级别颜色约定：

* higher level：橙色 / 红色系；
* current level：黄色 / 白色系；
* lower level：蓝色系。

注意：

* 1m 以下暂不支持；
* 如果当前级别已经是 1m，则不计算 lower level；
* 如果当前级别过高，例如月线，则 higher level 可关闭；
* 不允许所有级别互相污染状态数组；
* 每个级别都必须独立完成：包含处理、分型、笔、线段、中枢。

---

### 12.5 中枢第一版

中枢第一版基于 confirmed bi 计算。

暂不基于线段计算。

原因：

* 线段定义本身更复杂；
* 如果先做线段级中枢，Claude 容易把线段和笔混写；
* 第一版需要先保证结构稳定。

中枢使用连续三笔的价格区间重叠近似：

* intervalHigh_i = max(biStartPrice, biEndPrice)
* intervalLow_i = min(biStartPrice, biEndPrice)
* zg = min(intervalHigh_1, intervalHigh_2, intervalHigh_3)
* zd = max(intervalLow_1, intervalLow_2, intervalLow_3)

若 zg >= zd，则存在笔级别中枢。

---

### 12.6 背驰第一版

背驰第一版使用 MACD 代理。

暂不直接使用复杂主观力度判断。

可选 MACD 力度代理：

1. MACD histogram 面积；
2. MACD DIF/DEA 距离；
3. 线段/笔对应区间内 histogram 绝对值累加；
4. 价格新高/新低但 MACD 力度未创新高/新低。

第一版背驰定义：

上涨背驰候选：

* 当前笔或线段价格创新高；
* 对应 MACD histogram 正面积小于上一同向结构；
* 标记为 bearish divergence candidate。

下跌背驰候选：

* 当前笔或线段价格创新低；
* 对应 MACD histogram 负面积绝对值小于上一同向结构；
* 标记为 bullish divergence candidate。

注意：

* 背驰只做 candidate 提示；
* 不作为买卖点强信号；
* 不参与结构确认。

---

## 13. 分型、笔失效与实时更新规则

这是 Pine 脚本中最容易写错的部分。

核心原则：

> 分型可以被替换，候选笔可以失效，confirmed 笔原则上不回滚。
> 如果一定要回滚，只允许回滚最后一笔，不允许回滚更早历史结构。

---

### 13.1 分型状态分层

分型必须分为两类：

1. candidate fractal：候选分型；
2. confirmed fractal：确认分型。

candidate fractal 可以被替换。

confirmed fractal 原则上不修改，除非它还没有生成 confirmed bi，或者它属于最后一个可变结构。

---

### 13.2 同类型分型替换规则

如果当前最后一个有效分型是顶分型，后面又出现新的顶分型：

* 若新顶分型价格更高，则替换旧顶分型；
* 若新顶分型价格不更高，则忽略新顶分型。

如果当前最后一个有效分型是底分型，后面又出现新的底分型：

* 若新底分型价格更低，则替换旧底分型；
* 若新底分型价格不更低，则忽略新底分型。

该规则适用于：

* 尚未成笔的 candidate fractal；
* 最后一笔的 tentative endpoint；
* 最后一笔尚未 confirmed 时的 endpoint 替换。

---

### 13.3 底分型后出现新低怎么办

场景：

1. 系统标记了一个底分型 B1；
2. 之后价格继续下跌；
3. 出现一个更低的底分型 B2。

处理规则：

#### 情况 A：B1 尚未参与 confirmed bi

直接用 B2 替换 B1。

结果：

* B1 删除或标记 invalid；
* B2 成为新的有效底分型；
* 不生成新的笔。

#### 情况 B：B1 是 tentative bi 的起点或终点

若 B2 更低，则更新 tentative bi：

* 如果 B1 是下跌 tentative bi 的终点，则把终点延伸到 B2；
* 如果 B1 是上涨 tentative bi 的起点，则通常说明上涨 tentative bi 尚未成立，应重置候选结构。

结果：

* tentative bi 重新画；
* 旧线删除或复用 line 对象更新坐标；
* 不影响 confirmed bi。

#### 情况 C：B1 已经是 confirmed bi 的终点

默认不回滚 confirmed bi。

但如果 B1 是最后一个 confirmed bi 的终点，且用户开启 aggressive realtime 修正模式，则允许回滚最后一笔：

* 删除最后一笔 confirmed bi；
* 将其降级为 tentative bi；
* 用 B2 替换 B1；
* 重新等待反向顶分型确认。

默认模式不启用该回滚。

---

### 13.4 顶分型后出现新高怎么办

与底分型逻辑对称。

场景：

1. 系统标记了一个顶分型 T1；
2. 之后价格继续上涨；
3. 出现一个更高的顶分型 T2。

处理规则：

#### 情况 A：T1 尚未参与 confirmed bi

直接用 T2 替换 T1。

#### 情况 B：T1 是 tentative bi 的起点或终点

若 T2 更高，则更新 tentative bi：

* 如果 T1 是上涨 tentative bi 的终点，则把终点延伸到 T2；
* 如果 T1 是下跌 tentative bi 的起点，则通常说明下跌 tentative bi 尚未成立，应重置候选结构。

#### 情况 C：T1 已经是 confirmed bi 的终点

默认不回滚 confirmed bi。

只有在 aggressive realtime 修正模式中，且 T1 属于最后一个 confirmed bi，才允许回滚最后一笔。

---

### 13.5 反向分型破坏笔怎么办

一笔的成立条件：

* 起点和终点分型类型相反；
* 间隔满足 minMergedBarsBetweenFractals；
* 终点分型已经确认；
* 终点价格相对起点方向有效。

例如向上一笔：

* 起点是底分型；
* 终点是顶分型；
* 顶分型价格必须高于底分型价格；
* 中间满足最小合成 K 线间隔。

如果已经画出 tentative bi 后，后续走势破坏该笔：

#### 情况 A：tentative bi 被破坏

直接失效。

处理：

* 删除或隐藏 tentative line；
* 保留更极端的同类型分型作为新 candidate；
* 等待新的反向分型。

#### 情况 B：confirmed bi 被后续价格穿越

不叫“笔被破坏”，而是进入下一笔或线段破坏判断。

confirmed bi 不因为后续价格穿越而删除。

例如：

* 已经确认一笔向上，从底到顶；
* 后面价格跌破该笔起点；
* 这不是删除向上一笔；
* 而是说明后续下跌笔力度很强，可能破坏线段或形成反向结构。

#### 情况 C：最后一笔 confirmed bi 需要实时修正

仅在 aggressive realtime 修正模式允许：

* 只允许回滚最后一笔；
* 不允许回滚倒数第二笔及之前结构；
* 回滚后必须重新计算后续线段、中枢、背驰候选。

---

### 13.6 推荐默认模式

默认采用 stable realtime 模式：

* confirmed fractal 可以生成 confirmed bi；
* confirmed bi 一旦生成，不因后续新高/新低删除；
* 最后一笔可以有 tentative extension；
* 同类型新极值只更新 tentative endpoint；
* 只有未 confirmed 的结构可以自由替换。

不默认使用 aggressive realtime 修正。

原因：

* aggressive 模式更接近主观看盘；
* 但会导致 Pine 图上历史结构跳动；
* 对 Claude 实现难度更高；
* 容易污染线段和中枢。

---

### 13.7 实现建议：事件驱动状态机

每次新增 confirmed fractal 时，执行以下流程：

1. 读取 lastEffectiveFractal；
2. 若新分型与 lastEffectiveFractal 同类型：

   * 顶分型保留更高者；
   * 底分型保留更低者；
   * 更新 candidate，不生成笔；
3. 若新分型与 lastEffectiveFractal 反向：

   * 检查间隔；
   * 检查方向有效性；
   * 满足则生成 confirmed bi；
   * 不满足则仅作为 candidate；
4. 若生成 confirmed bi：

   * 更新 bi arrays；
   * 清理 tentative bi；
   * 尝试更新 segment；
   * 尝试更新 center；
5. 每根 bar 更新最后 tentative bi：

   * 若当前价格刷新同方向极值，则延伸；
   * 若出现反向 confirmed fractal 且满足条件，则转 confirmed。

---

### 13.8 Claude 禁止行为补充

Claude 不允许：

* 一出现分型就永久锁死；
* 连续顶分型全部画出来并参与成笔；
* 连续底分型全部画出来并参与成笔；
* 后面出新高/新低时直接删除历史 confirmed bi；
* 把价格穿越旧笔起点理解为“旧笔失效”；
* 用 pivot 函数替代分型状态机；
* 不区分 candidate、tentative、confirmed。

---

## 14. v0.2 规范术语与不变量

### 14.1 术语唯一含义

后续代码、调试面板和验收记录必须使用以下术语，不再用一个 `confirmed` 同时表达多个阶段：

1. `raw bar`：TradingView 图表的一根原始 K 线。
2. `active merged bar`：当前仍可能吸收后续 raw bar 的合成 K 线；它尚未封口，不参与分型确认。
3. `sealed merged bar`：因下一根非包含 K 线出现而封口的合成 K 线；封口后不可修改。
4. `detected fractal`：由连续三根 sealed merged bar 确认的原始分型事件；一经产生不可修改。
5. `effective endpoint`：笔状态机当前采用的有效端点；尚未形成冻结笔时允许同类型择优替换。
6. `confirmed bi`：两个有效端点满足全部成笔条件后生成的冻结笔。
7. `tentative bi`：从最后一个冻结端点指向当前行情候选极值的显示对象；不进入高级结构。
8. `confirmed segment`：仅由 confirmed bi 生成的工程简化线段。
9. `active center`：已经由三笔确认、仍可能延伸的中枢。
10. `closed center`：已退出延伸状态的冻结中枢。

### 14.2 冻结与依赖不变量

默认模式为 `STABLE`，必须满足：

* sealed merged bar 不修改；
* detected fractal 不修改、不删除；
* confirmed bi 的起点、终点、价格、方向不修改；
* confirmed segment 不读取 tentative bi；
* center、divergence 和 signal 不读取 tentative bi；
* 每个高级结构只依赖已经冻结的低级结构；
* 显示开关只改变对象可见性，不改变算法数组和结构结果；
* 相同品种、周期、参数和历史数据在刷新前后必须得到相同的 confirmed 结构。

`AGGRESSIVE` 回滚模式不属于 v0.2 实现范围。相关旧描述仅作为未来扩展，不得在 Phase 1–4 中实现。

### 14.3 ID 与索引

每种冻结结构使用单调递增 ID。数组下标不是持久 ID，数组裁剪后不得用旧数组下标引用对象。

必须区分：

* `rawBarIndex`：原始 `bar_index`；
* `mergedSeq`：sealed merged bar 的单调序号，从 0 开始；
* `fractalId`、`biId`、`segmentId`、`centerId`：对应结构的持久 ID。

所有间隔判断使用 `mergedSeq` 或 `biId/biSeq`，所有绘图横坐标使用 raw `bar_index` 或 `time`。禁止混用。

---

## 15. Pine 执行、实时更新与资源约束

### 15.1 确认时机

结构引擎只在 raw bar 收盘后消费该 bar：

```text
processRawBar := barstate.isconfirmed and bar_index != lastProcessedRawBar
```

历史 bar 在历史回放中视为已确认。实时未收盘 bar 的 high/low/close 只能更新 tentative 显示，不得写入 sealed merged bar、detected fractal、confirmed bi、segment 或 center 数组。

### 15.2 幂等性

每个模块保存最后消费的上游事件 ID：

* 包含模块：`lastProcessedRawBar`；
* 分型模块：`lastCheckedMergedSeq`；
* 笔模块：`lastProcessedFractalId`；
* 线段模块：`lastProcessedBiId`；
* 中枢模块：`lastProcessedCenterBiId`。

同一事件重复执行不得产生第二份结构或第二个绘图对象。禁止仅以价格或 raw bar index 去重，因为同一 raw bar 可能承载不同语义事件。

### 15.3 计算数组与显示对象分离

计算历史和显示窗口必须分离：

* 计算数组保存算法需要的历史；
* `line/label/box` 数组只保存当前可见对象；
* 删除显示对象不得删除算法结构；
* 输入开关变化后，脚本重算时必须能从算法结果重新生成可见窗口。

### 15.4 资源预算

默认预算：

```text
maxVisibleFractals = 120
maxVisibleBi       = 80
maxVisibleSegments = 40
maxVisibleCenters  = 20
maxStoredMerged    = 5000
maxStoredFractals  = 2500
maxStoredBi        = 1200
maxStoredSegments  = 400
maxStoredCenters   = 200
```

要求：

* 可见对象超过预算时删除最旧对象，绝不等待 TradingView 隐式回收；
* tentative line 每层最多一个，使用 `line.set_*` 复用；
* debug table 使用单例；
* debug label 同样受 `maxVisibleFractals` 或独立的更小预算约束；
* 并行数组必须始终等长；每次 push/pop/shift 后在 debug 模式显示一致性状态；
* v0.2 允许对最旧冻结结构做前缀裁剪，但裁剪前必须确保没有保留结构仍引用它；
* 若安全裁剪尚未实现，必须提供 `maxBarsToProcess` 并在交付限制中说明，而不是无限增长。

### 15.5 支持边界

v0.2 只保证标准 OHLC 图表的当前周期：

* 不保证 Heikin Ashi、Renko、Kagi、Point & Figure 等非标准图表；
* 不处理复权方式改变造成的历史结构迁移；
* 允许交易时段缺口，缺口不插入虚拟 K 线；
* 第一版不实现 lower timeframe engine；
* higher/lower timeframe、多级别状态必须在单级别 Phase 1–4 验收后另立规格。

---

## 16. 包含处理的确定性算法

### 16.1 合成 K 线字段

active 和 sealed merged bar 均至少保存：

```text
mergedSeq
startRawBar
endRawBar
high
low
highRawBar
lowRawBar
openOfFirstRawBar
closeOfLastRawBar
directionContext  // UP, DOWN
```

同价极值索引采用“较早者优先”：只有严格更高或严格更低时才更新 `highRawBar/lowRawBar`。这样刷新前后坐标确定。

### 16.2 包含判定

对 active merged bar `A` 与新收盘 raw bar `B`：

```text
A_contains_B := A.high >= B.high and A.low <= B.low
B_contains_A := B.high >= A.high and B.low <= A.low
hasContain   := A_contains_B or B_contains_A
```

边界相等仍算包含。若不包含，则 B 相对 A 必然可归为：

```text
UP   := B.high > A.high and B.low > A.low
DOWN := B.high < A.high and B.low < A.low
```

不能满足其中之一时，在 debug 模式记录 invariant error，不静默猜测。

### 16.3 方向状态

引擎保存 `lastResolvedDirection`。

* 每次遇到非包含关系时，按 16.2 更新为 UP 或 DOWN；
* 处理包含关系时沿用 `lastResolvedDirection`；
* 初始化尚无方向时，使用以下唯一 bootstrap 规则产生临时方向：

```text
若 B.close > A.close                    => UP
若 B.close < A.close                    => DOWN
若 close 相等且 midpoint(B)>midpoint(A) => UP
若 close 相等且 midpoint(B)<midpoint(A) => DOWN
完全相等                               => UP
```

bootstrap 方向只负责消除初始化歧义；首个非包含关系出现后立即由正式方向覆盖，不回算已经封口的历史。

### 16.4 合并与封口

若 `hasContain`：

```text
UP:
  newHigh = max(A.high, B.high)
  newLow  = max(A.low,  B.low)

DOWN:
  newHigh = min(A.high, B.high)
  newLow  = min(A.low,  B.low)
```

并更新 `endRawBar`、`closeOfLastRawBar` 及实际贡献新 high/low 的 raw bar index。不得将合并后并未采用的 B.high/B.low 的索引写入极值索引。

若 `not hasContain`：

1. 将 A 作为 sealed merged bar push；
2. A 获得下一个 `mergedSeq`；
3. 用 B 初始化新的 active merged bar；
4. 发出且只发出一个 `MERGED_SEALED` 事件。

active merged bar 永不参与三 K 分型。数据集最后一个 active merged bar 因缺少右侧非包含确认，允许始终不封口。

---

## 17. 分型检测规范

### 17.1 检测输入与确认延迟

仅在新的 sealed merged bar `C` 产生后，检查最后三根 sealed merged bar `A,B,C`。B 是唯一候选中心。

默认 `LOOSE`：

```text
TOP    := B.high > A.high and B.high > C.high
BOTTOM := B.low  < A.low  and B.low  < C.low
```

可选 `STRICT`：

```text
TOP    := looseTop    and B.low  > A.low  and B.low  > C.low
BOTTOM := looseBottom and B.high < A.high and B.high < C.high
```

全部使用严格大于/小于；相等不构成分型。

### 17.2 双重分型防御

正常的非包含 sealed 序列不应让同一 B 同时成为 TOP 和 BOTTOM。若实现检测到二者同时成立：

* 不产生 detected fractal；
* debug 面板增加 `dualFractalError`；
* 不使用 `if/else` 顺序静默选择顶或底。

### 17.3 detected fractal 字段

```text
fractalId
type              // TOP=+1, BOTTOM=-1
price             // TOP=B.high, BOTTOM=B.low
rawBarIndex       // TOP=B.highRawBar, BOTTOM=B.lowRawBar
sourceMergedSeq   // B.mergedSeq
confirmedOnSeq    // C.mergedSeq
```

所有 detected fractal 只属于底层事件流，可用于 debug 显示。它们不得直接参与成笔；必须先经过第 17.4 节的当前级别选择器。

### 17.4 当前级别分型选择器 v1

`LEVEL_FRACTAL_V1` 是工程化尺度选择器，不声称仅凭三 K 分型就推导出唯一正统级别。它在 detected fractal 事件流上组合两个因果门槛：

```text
minLevelMergedSpan = 4
levelAtrLength = 14
levelAtrMultiplier = 1.0
requiredDistance = max(tentative.atr, opposite.atr) * levelAtrMultiplier
```

ATR 在 detected fractal 确认时冻结，此后不得随新 bar 重算旧事件的阈值。状态数组最后一个元素始终是可变的 `tentativeExtreme`，此前元素全部是 `confirmedLevelFractal`。

每个新 detected fractal F 按以下规则处理：

```text
若没有 tentativeExtreme:
    F 成为 tentativeExtreme

若 F 与 tentativeExtreme 同类型:
    TOP 仅在 F.price 更高时替换
    BOTTOM 仅在 F.price 更低时替换
    不生成 confirmedLevelFractal

若 F 与 tentativeExtreme 反向:
    directionValid := TOP->更低BOTTOM 或 BOTTOM->更高TOP
    spanValid := F.sourceMergedSeq - tentative.sourceMergedSeq >= minLevelMergedSpan
    scaleValid := abs(F.price - tentative.price) >= requiredDistance

    若三个条件全部成立:
        冻结旧 tentativeExtreme 为 confirmedLevelFractal
        F 成为新的 tentativeExtreme
    否则:
        忽略 F，继续等待更充分的反向运动
```

不变量：

* confirmedLevelFractal 顶底严格交替；
* confirmedLevelFractal 冻结后不移动、不删除；
* 最后一个 tentativeExtreme 可以被同类型更极端事件替换；
* 离开段内未达到 ATR 与跨度门槛的小反向分型保留在 detected 层，但不升级；
* MA5/MA10 和 MACD 只作为后续趋势/力度辅助，不参与 v1 硬过滤；
* 默认隐藏 detected fractal，只显示 confirmedLevelFractal；两者使用不同样式。

字段：

```text
levelFractalId
type
price
rawBarIndex
sourceMergedSeq
atrAtDetection
sourceFractalId
confirmed
```

---

## 18. 笔状态机规范

### 18.1 输入和成笔条件

笔模块只消费第 17.4 节输出的 `confirmedLevelFractal`，不得再次消费 detected fractal。尺度、同类择优和最小跨度已经由 Level Selector 完成，笔模块不重复发明第二套筛选规则。

每当新的 confirmedLevelFractal E 产生：

```text
若没有 previousConfirmedLevelFractal:
    保存 E，等待下一端点

否则断言：
    E.type == -previous.type
    E.sourceMergedSeq - previous.sourceMergedSeq >= minLevelMergedSpan
    previous=BOTTOM and E=TOP    => E.price > previous.price
    previous=TOP    and E=BOTTOM => E.price < previous.price

断言全部成立：
    直接生成 confirmed bi(previous -> E)
    previous = E

断言失败：
    报告 invariant error，拒绝 push，不静默修正 Level Selector 输出
```

### 18.2 职责边界

* Level Selector 负责局部分型到当前级别端点的尺度升级；
* 笔模块负责连接相邻的冻结当前级别端点；
* confirmed bi 不回滚；
* 最新 tentativeExtreme 不进入 confirmed bi；
* MA、MACD 不直接改变笔端点。

### 18.3 confirmed bi 字段

```text
biId
startLevelFractalId
endLevelFractalId
startMergedSeq
endMergedSeq
startRawBar
endRawBar
startPrice
endPrice
direction          // UP or DOWN
confirmedOnSeq
```

相邻 confirmed bi 必须首尾连接：后一笔 `startLevelFractalId ==` 前一笔 `endLevelFractalId`，方向必须交替。若不满足，在 debug 模式报告 invariant error 并拒绝 push。

### 18.4 tentative bi

tentative bi 仅是显示投影，不是算法结构：

* 起点为最后一个 confirmed bi 的 `lockedAnchor`；没有 confirmed bi 时可从 `bootstrapEndpoint` 显示；
* 若起点是 BOTTOM，终点取起点之后已收盘 raw bar 与当前实时 bar 的最高 high；
* 若起点是 TOP，终点取起点之后已收盘 raw bar 与当前实时 bar 的最低 low；
* 新极值只用 `line.set_xy*` 更新单例虚线；
* 它不得进入 confirmed bi 数组；
* 关闭显示后删除该 line，但候选计算状态保留；
* confirmed bi 产生后，旧 tentative line 复用为下一方向，不转成 confirmed line 对象。

### 18.5 不重绘定义

“不重绘”特指：raw bar 收盘并生成 confirmed bi 后，该笔坐标和字段在后续 bar、刷新、切换显示选项时保持不变。实时未收盘 bar 上 tentative line 的移动不算重绘。

---

## 19. 工程简化线段 v1

### 19.1 范围声明

本节定义的是 `ENGINEERING_SEGMENT_V1`，不是正统缠论特征序列线段。界面和代码注释必须明确标注“工程线段”。未来正统算法必须使用新的模式名，不得静默替换 v1 结果。

### 19.2 输入与核心规则

输入只允许 confirmed bi endpoints。把 confirmed bi 端点视为序列 `P0,P1,...,Pn`。

工程线段使用“笔端点上的高一级摆动”算法：

```text
segment candidate 起点 S 为当前有效线段端点
BOOTSTRAP 阶段同类型端点仅保留更极端者
ANCHORED 阶段 locked anchor 不替换，反向 pending endpoint 才允许同类择优
新端点 E 与 S 反向时，只有满足：
    E.biEndpointSeq - S.biEndpointSeq >= 3
    且方向价格有效
才确认一条 segment
```

这里差值 `>= 3` 表示线段跨越至少三笔。方向价格有效规则与笔对称：底端到顶端必须上涨，顶端到底端必须下跌。

### 19.3 状态转换

算法复用第 18.2 节修正后的双阶段模型，但输入事件是“新 confirmed bi 产生后新增的冻结终点”，间隔单位是笔数：

* 第一条 segment 产生前，`bootstrapSegmentEndpoint` 可同类择优；
* 第一条 segment 产生后，最后一个 segment 终点成为 `lockedSegmentAnchor`；
* locked segment anchor 不得被后续同类型端点替换；
* 只允许 `pendingOppositeSegmentEndpoint` 同类择优；
* pending endpoint 满足跨度和价格方向条件后生成新 confirmed segment，并成为新的 locked anchor。

confirmed segment 一旦产生即冻结，不因后续端点修改。

字段：

```text
segmentId
mode = ENGINEERING_SEGMENT_V1
startBiEndpointId
endBiEndpointId
startBiId
endBiId
startRawBar
endRawBar
startPrice
endPrice
direction
confirmed
```

验收不再使用“数量明显少于笔”这种主观标准，而使用：

* 每条线段跨越至少 3 条 confirmed bi；
* 线段端点类型交替；
* 相邻线段首尾连接；
* confirmed segment 坐标冻结；
* 同一 confirmed bi 事件不得重复生成线段。

---

## 20. 笔级中枢 v1 状态机

### 20.1 输入与重叠

`BI_CENTER_V1` 只读取 confirmed bi。对笔 i：

```text
intervalHigh(i) = max(startPrice, endPrice)
intervalLow(i)  = min(startPrice, endPrice)
```

三个连续笔 `i-2,i-1,i` 的初始公共区：

```text
candidateZG = min(intervalHigh(i-2), intervalHigh(i-1), intervalHigh(i))
candidateZD = max(intervalLow(i-2),  intervalLow(i-1),  intervalLow(i))
```

默认要求 `candidateZG > candidateZD`，即必须有正宽度重叠。`==` 仅是接触，不构成中枢。

### 20.2 创建、延伸与关闭

状态只有 `NONE` 和 `ACTIVE`；关闭后 push 为 `CLOSED`。

```text
NONE:
    每个新 confirmed bi 到来时检查最新三笔
    若 candidateZG > candidateZD:
        创建 ACTIVE center
        startBiId = i-2
        endBiId = i
        zd = candidateZD
        zg = candidateZG
        gg/dd = 三笔完整价格区间的最大/最小值

ACTIVE:
    对新笔 i 计算 overlap：
        overlapZG = min(center.zg, intervalHigh(i))
        overlapZD = max(center.zd, intervalLow(i))
    若 overlapZG > overlapZD:
        center.zg = overlapZG
        center.zd = overlapZD
        center.endBiId = i
        center.gg/dd 扩展到包含该笔
    否则:
        将当前 center 冻结为 CLOSED
        状态设为 NONE
        立即用包含新笔 i 的最新三笔再尝试创建新 center
```

中枢核心区在延伸时只允许保持或收窄，不允许扩大。`gg/dd` 可以扩大。一个新笔最多延伸一个 active center，并最多触发一个新 center，禁止同一滑动窗口重复创建 box。

### 20.3 时间坐标和字段

```text
centerId
level = BI_CENTER_V1
startBiId
endBiId
startRawBar = 第一个构成笔的 startRawBar
endRawBar   = 最后纳入笔的 endRawBar
zd
zg
dd
gg
status      // ACTIVE or CLOSED
```

ACTIVE box 可以更新右边界和上下边界；CLOSED box 冻结。关闭显示不改变状态。

---

## 21. 分阶段交付、测试与验收门槛

### 21.1 每阶段交付物

每个阶段必须同时交付：

1. 可完整复制到 TradingView 的 Pine v6 源码；
2. 本阶段实现范围；
3. 明确未实现范围；
4. 状态数组及关键不变量说明；
5. TradingView 手工验证步骤；
6. 至少一组小型确定性序列的逐事件预期结果；
7. 已知限制；
8. 与本规范的偏离清单；无偏离时明确写“无”。

### 21.2 Phase 0：骨架与诊断

只实现输入、枚举/常量、单例 debug table、资源计数和 invariant error 展示。验收：代码编译；开关不创建无限对象；未运行结构算法。

### 21.3 Phase 1A：包含处理

只实现第 16 节。必须用至少以下合成序列验收：

```text
上行包含：A[10,5], B[9,6]，方向 UP   => merged[10,6]
下行包含：A[10,5], B[9,6]，方向 DOWN => merged[9,5]
向上非包含：A[10,5], B[11,6]         => seal A, active B
向下非包含：A[10,5], B[9,4]          => seal A, active B
连续包含：三根输入只产生一个 active/最终 sealed 结果
```

验收：sealed 数据不再变化；极值 raw index 与实际采用值一致；同一 raw bar 不重复消费。

### 21.4 Phase 1B：分型

只实现第 17 节。构造 merged 序列验证：单顶、单底、相等不成分型、严格/宽松模式差异、同一 B 只检查一次。

### 21.5 Phase 2：笔

只实现第 18 节。至少覆盖：

* 同顶取更高、同底取更低；
* 同类型不更极端时忽略；
* 反向但 span 不足；
* span 足够但价格方向无效；
* 正常上下笔交替；
* confirmed 笔生成后出现新高/新低仍不移动；
* tentative line 盘中更新但不写入高级数组。

Phase 2 未通过前禁止实现 segment 和 center。

### 21.6 Phase 3：工程线段

严格实现第 19 节，并用人工 bi endpoint 序列独立验证。不得通过修改笔算法来迎合线段输出。

### 21.7 Phase 4：笔级中枢

严格实现第 20 节。至少验证：无重叠、恰好接触、三笔创建、第四笔延伸并收窄、离开后关闭、同一新笔触发旧中枢关闭并尝试新中枢。

### 21.8 回归门槛

每完成一个阶段，必须重新验证此前全部阶段。任何阶段只有同时满足以下条件才算通过：

* Pine v6 编译无错误；
* 历史加载与刷新后 confirmed 结构一致；
* 实时未收盘变化只影响 tentative 对象；
* 无数组越界；
* 并行数组等长；
* 对象数量不超过配置预算；
* debug 关闭不改变结构数量和坐标；
* 未实现模块没有占位结果冒充 confirmed 输出。

---

## 22. 快速反转桥接与小转大代理 v1

### 22.1 目的与边界

第 17.4 节的普通 Level Selector 会直接忽略未同时满足跨度和 ATR 门槛的反向 detected fractal。在高波动趋势末端，可能出现：

```text
T1 -> B1 -> T2
```

或：

```text
B1 -> T1 -> B2
```

中间反向点因运动过快而跨度不足，随后同型端点又覆盖旧 tentative，导致一组肉眼明确的快速 V/N 型日线转折整体消失。

Phase 1D 增加 `FAST_REVERSAL_BRIDGE_V1`。它是尚未接入 60m/4h 次级别结构前的“小转大代理”，不是正式区间套或背驰。不得将其描述成已经完成多级别小转大判断。

### 22.2 pending opposite 缓冲

普通反向确认失败但价格方向有效时，不直接删除事件，而写入单例 `pendingOpposite`：

```text
type
price
rawBar
mergedSeq
frozenAtr
sourceFractalId
```

约束：

* pending 不属于 level endpoint，不得进入笔、线段或中枢；
* 同类型 pending 只保留更极端者；
* 普通反向确认成功后立即清空；
* tentative 被后续更极端同型分型替换后立即清空，禁止 pending 跨越新锚；
* pending 未经桥接不得绘制正式“顶/底”标签。

### 22.3 三点桥接确认

设当前 tentative 为 `A`，pending opposite 为 `B`，新到达的同型 detected fractal 为 `C`。仅当以下条件全部成立时恢复：

```text
A.type == C.type
B.type == -A.type
A.seq < B.seq < C.seq

leg1Span = B.seq - A.seq
leg2Span = C.seq - B.seq
outerSpan = C.seq - A.seq

leg1Span >= fastRecoveryMinLegSpan
leg2Span >= fastRecoveryMinLegSpan
outerSpan >= minLevelMergedSpan

abs(B.price-A.price) >= max(A.atr,B.atr) * fastRecoveryAtrMultiplier
abs(C.price-B.price) >= max(B.atr,C.atr) * fastRecoveryAtrMultiplier

abs(B.price-A.price)/leg1Span >= max(A.atr,B.atr) * fastRecoverySpeedAtr
abs(C.price-B.price)/leg2Span >= max(B.atr,C.atr) * fastRecoverySpeedAtr
```

价格方向还必须满足：

```text
TOP-BOTTOM-TOP: B < A and C > B
BOTTOM-TOP-BOTTOM: B > A and C < B
```

不要求 `C` 突破 `A`，因为更低的第二顶或更高的第二底同样可能是反转结构的一部分。

### 22.4 状态写入

桥接成功时必须以一次事务完成：

1. 将旧 tentative `A` 冻结为 confirmed，reason=`FAST_REVERSAL`；
2. 将 pending `B` 写入 confirmed level endpoint，reason=`FAST_REVERSAL`；
3. 将 `C` 写为新的 tentative，reason=`NONE`；
4. 清空 pending；
5. 维护顶底交替、持久 ID 和并行数组等长；
6. 为 A/B 创建的标签使用普通顶底文本，但保留 `FAST_REVERSAL` 内部原因和 tooltip，以免污染图面。

桥接失败时不得部分 push，也不得冻结 A/B。

### 22.5 默认参数

```text
enableFastReversalRecovery = true
fastRecoveryMinLegSpan = 2
fastRecoveryAtrMultiplier = 0.75
fastRecoverySpeedAtr = 0.30
```

ATR 在本通道中只证明快速异常运动具有足够幅度和速度，不再单独决定一个分型的级别。不得通过全局降低 `minLevelMergedSpan` 来替代本通道。

### 22.6 震荡保护与验收

三月中枢内近水平、低速度的小摆动即使形成 `A-B-C`，也应因任一腿的 ATR 幅度或单位合成 K 速度不足而被拒绝。五月末至六月初的快速下杀和反抽应在两腿及总跨度达标后恢复旧顶和中间底。

至少验证：

* 快速 `TOP-BOTTOM-TOP` 成功恢复；
* 快速 `BOTTOM-TOP-BOTTOM` 成功恢复；
* C 未突破 A 但两腿合格，仍可恢复；
* 单腿跨度不足时拒绝；
* 外层总跨度不足时拒绝；
* 横盘幅度不足时拒绝；
* 幅度够但速度不足时拒绝；
* pending 被新 tentative 换锚后清空；
* 普通反向确认优先，成功时不得再触发桥接；
* 刷新图表后 FAST_REVERSAL 端点坐标一致。
