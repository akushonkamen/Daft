# 任务看板 — Daft
> Tech Lead 写入 | Teammate 执行

## 状态：⏳ 待启动 | 🔄 进行中 | 🔍 待巡检 | ✅ 通过 | ❌ 打回

---

## 当前任务

### TASK-PROD-001：分布式Ray集成
**状态**：🔄 进行中  |  **优先级**：🟢 中（分布式扩展）

**任务目标**：让Daft DataFrame可以使用Ray进行分布式执行，同时支持DuckDB backend。

**验收标准**：
- [ ] 多节点Ray集群运行验证
- [ ] DuckDB backend在每个worker上可用
- [ ] 端到端演示（CIFAR-10数据集、4节点Ray集群、AI_filter分布式执行）
- [ ] 性能基准测试
- [ ] 在 Daft/ submodule 内完成 commit，通知 Tech Lead sync

**推荐方案**：单机DuckDB + Ray数据分片
- Ray Cluster中每个Worker独立运行Daft + DuckDB CLI
- 利用现有RayRunner基础设施
- DuckDB单机执行性能极高（向量化）

**风险预警**：
- Extension分发（需在每个Ray worker上可用）
- CLI依赖
- 聚合下推

---

## 历史任务

### ✅ TASK-D-001：M0 环境验证与架构摸底
**状态**：✅ 通过（由Tech Lead完成）

**完成内容**：
- ✅ ARCH_NOTES.md 完整
- ✅ Runner体系分析
- ✅ LogicalPlan关键节点分析
- ✅ 推荐方案：LogicalPlan → DuckDB SQL转译层

### ✅ TASK-D-004：SQL转译层实现
**状态**：✅ 通过

### ✅ TASK-INTEGRATION-001：集成测试
**状态**：✅ 通过

### ✅ TASK-13：端到端Daft-DuckDB集成测试
**状态**：✅ 通过

### ✅ TASK-17：M4 Daft AI算子API
**状态**：✅ 通过

### ✅ TASK-DEMO-001：真实CIFAR-10数据集Demo
**状态**：✅ 通过

### ✅ TASK-REVIEW-001：代码审查
**状态**：✅ 通过

### ✅ TASK-DEMO-002：修复demo_real.py API响应解析
**状态**：✅ 通过

### ✅ TASK-DEMO-003：修复demo_real.py执行问题
**状态**：✅ 通过
