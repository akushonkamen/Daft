# 任务看板 — Daft
> Tech Lead 写入 | Teammate 执行

## 状态：⏳ 待启动 | 🔄 进行中 | 🔍 待巡检 | ✅ 通过 | ❌ 打回

---

## 当前任务

### TASK-PROD-003：性能基准测试
**状态**：🔄 进行中  |  **优先级**：🟢 中（性能优化）

**任务目标**：建立完整的性能基准测试，为后续优化提供数据支撑。

**验收标准**：
- [ ] 单机 DuckDB CLI 性能基准
- [ ] Ray 分布式执行性能对比
- [ ] AI API 调用延迟分析
- [ ] 不同数据规模下的性能表现（10/100/1000 行）
- [ ] 生成性能报告（CSV/JSON + Markdown）

**CI/CD 要求**：
- 支持 Mock 模式（CI 环境快速验证，< 30 秒）
- 支持真实模式（本地完整性能测试）
- 自动检测环境并选择模式

**交付物**：
- benchmark_ray_performance.py（主测试脚本）
- benchmark_results.json（性能数据）
- run_tests.sh 更新（添加基准测试）

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

### ✅ TASK-PROD-001：分布式Ray集成（优先级1 MVP）
**状态**：✅ 通过

**完成内容**：
- ✅ Ray 集群初始化（本地多进程）
- ✅ 多 Worker 并行执行验证（4 任务 / 2 进程）
- ✅ Worker 进程隔离验证
- ✅ DuckDB CLI 在 Worker 上执行
- ✅ AI Extension 在每个 Worker 上独立加载
- ✅ demo_ray_simple.py（架构验证脚本）
- ✅ demo_ray_distributed.py（完整分布式演示）

**技术方案**：
```
Ray Cluster (本地多进程)
├── Worker 1: Ray task → subprocess → DuckDB CLI + Extension
└── Worker 2: Ray task → subprocess → DuckDB CLI + Extension
```
