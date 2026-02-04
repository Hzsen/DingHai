# TODO

## Current Build (ETL + UI + MCP)
- [x] Create `processed/` and `src/` module layout
- [x] Extract ETL core into `src/core/screener_logic.py`
- [x] Add watchdog service `src/services/watcher.py`
- [x] Build Streamlit UI `src/ui/app.py`
- [x] Add MCP server `src/mcp/server.py`
- [x] Add `config.yaml` and `requirements.txt`

## Phase 0 - Repo prep
- [ ] Identify all current data formats in `data/` and document expected columns
- [ ] Confirm which script(s) produce the desired output dataset(s)
- [ ] Define output file naming convention and storage location

## Phase 1 - Ingestion + auto-run
- [ ] Add a file watcher for `data/` (drag-and-drop trigger)
- [ ] Validate new files and detect their type (xls/xlsx/csv)
- [ ] Normalize encodings and headers into a canonical dataframe
- [ ] Run the existing merge pipeline automatically
- [ ] Write outputs to `data/processed/` with timestamps

## Phase 2 - Metrics + labels
- [ ] Define quantitative indicators to compute (e.g., rank delta, momentum)
- [ ] Create label rules (thresholds and tags)
- [ ] Persist metrics and labels in a single table for UI

## Phase 3 - UI + visualization
- [ ] Build a minimal dashboard (table + charts)
- [ ] Add filters for labels/metrics ranges
- [ ] Add export buttons for filtered results

## Phase 4 - Packaging
- [ ] Create a one-command launcher
- [ ] Add README usage guide
- [ ] Add basic tests for ingestion and metrics
## 目标
在现有数据脚本基础上，做一个可视化小软件：拖拽新数据到 `data/` 后自动处理、输出数据集，并能用量化指标标签筛选股票。

## 组件与里程碑
### 1. 数据处理内核
- 抽象数据读取/清洗/合并/指标计算为可复用模块
- 支持 `data/` 目录批量输入，输出到 `data/outputs/`
- 统一输入/输出格式与列名规范

### 2. 自动化触发
- 监听 `data/` 目录变更（文件新增/替换）
- 自动触发处理流程并记录日志
- 失败可回退并提示错误原因

### 3. 可视化与筛选
- 构建指标标签面板与筛选条件
- 支持表格筛选、排序、导出
- 可视化图表（分布、相关性、排名变动等）

### 4. 应用形态与打包
- 选择 UI 框架（Web 或桌面）
- 支持一键启动/打包分发

## TODO（按阶段完成）
### Phase A - 需求与规范
- [ ] 列出核心输入文件类型与列名样例
- [ ] 定义输出数据集字段规范
- [ ] 明确最小可用指标/标签集合

### Phase B - 数据处理模块
- [ ] 建立 `pipeline/` 目录与模块结构
- [ ] 实现统一读取（CSV/XLS/XLSX）
- [ ] 实现清洗与标准化（列名、类型）
- [ ] 实现指标计算与结果导出
- [ ] 增加可测试样例与单元测试

### Phase C - 自动化触发
- [ ] 文件监听（watchdog）
- [ ] 任务队列/去重策略
- [ ] 日志与错误报告

### Phase D - 可视化应用
- [ ] UI 原型（指标筛选+表格+图表）
- [ ] 前端与后端数据接口
- [ ] 导出与历史结果管理

### Phase E - 发布
- [ ] 本地一键启动脚本
- [ ] 打包（可选）
