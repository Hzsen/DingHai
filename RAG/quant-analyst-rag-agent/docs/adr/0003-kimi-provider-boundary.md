# ADR 0003：Kimi 用于叙事抽取，不作为数值行情真相源

- 状态：Accepted
- 日期：2026-07-13

## 背景

项目可以使用 Kimi Allegretto 会员中的专业数据库，也可以申请 Moonshot API Key。两者能力和计费边界不同。

Kimi 官方帮助中心说明：

- Allegretto 产品会员与 API 是独立计费体系，额度不互通；
- 产品会员中的专业数据库支持同花顺等数据源；
- API 提供 Chat Completions、JSON Mode、Structured Output 和 Function Calling；
- API 联网搜索来自公开网页搜索，不等同于产品端的同花顺专业数据库。

参考：

- [Kimi 会员权益](https://www.kimi.com/zh-cn/help/membership/membership-overview)
- [Kimi API 常见问题](https://www.kimi.com/zh-cn/help/kimi-api/api-troubleshooting)
- [Kimi Chat Completions API](https://platform.kimi.com/docs/api/chat)

## 决策

Kimi 的自动化角色是：

1. 从已采集且带来源的公告、新闻、调研和财报文本中抽取结构化叙事字段；
2. 识别公司直接相关、间接相关、市场误读和公告澄清；
3. 总结候选入选原因与风险；
4. 在 LangGraph 中承担可替换的 LLM provider。

Kimi 不负责：

- 返回价格、成交额、换手率或财务数值作为唯一真相源；
- 直接决定 `leader_score`；
- 将产品端同花顺能力假定为 Moonshot API 内置工具；
- 在没有来源文本时自动补全题材和基本面事实。

数值行情继续由数据 adapter 采集并由 dataframe / SQL 计算。Kimi 输出必须保留模型、请求 ID、token usage、输入文档来源和 `available_at`。

## 两条使用路径

### 自动化 API 路径

仓库通过 `MOONSHOT_API_KEY` 调用 `https://api.moonshot.cn/v1/chat/completions`，默认模型为 `kimi-k2.6`，关闭思考并启用 JSON Object 输出。输出经过本地 enum、类型和分数范围校验后才能进入叙事候选表。

API Key 只存在本地 `.env`，禁止进入 Git、异常信息、日志和测试 fixture。

### Allegretto 专业数据库路径

可以在 Kimi 产品端利用同花顺专业数据库做探索研究，再将结果导出为待审核证据。导入仓库前仍必须补齐证券代码、发布时间、来源标题、URL / 文档标识和原文证据。

在 Kimi 官方公开 API 明确提供专业数据库接口之前，这条路径是人工研究入口，不伪装成可自动调用的行情 API。

## 后果

优点：可以利用 Kimi 的中文长文本和结构化抽取能力，同时不破坏数字可复现与 point-in-time 原则。

代价：Allegretto 会员不能抵扣 API token 费用；专业数据库研究结果需要一次显式导出和审核；同一文档的模型输出仍需做回归评估。
