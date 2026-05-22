# TradingView 自动画缠论结构 Pine 脚本需求规格 v0.1

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
