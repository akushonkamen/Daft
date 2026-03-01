# 讨论区 — Daft
> Tech Lead ↔ Teammate | 每条注明【发起方】和日期

---

### 【Tech Lead】项目启动

请读 Task.md 和 CLAUDE.md，然后在此回复：
1. 对当前任务的**理解确认**
2. 已知**风险预警**

回复后 SendMessage 通知 Tech Lead 已就绪。

---

### 【Tech Lead】M0 架构调研完成 【2026-03-01】

**背景**：daft-engineer 初始化失败，由 Tech Lead 直接完成 M0 架构调研。

**交付物**：
- ✅ `Daft/ARCH_NOTES.md` - 完整架构调研报告

**关键发现**：

1. **Runner 体系**：
   - 现有 `RayRunner` (分布式) 和 `NativeRunner` (本地多线程)
   - 扩展点：`Runner` 枚举可添加 `DuckDB(DuckDBRunner)` 变体

2. **LogicalPlan**：
   - 文件：`src/daft-logical-plan/src/logical_plan.rs`
   - 已有 `VLLMProject` 操作，说明 Daft 对 AI 算子有原生支持
   - 支持多模态类型：Image, Embedding, Audio

3. **推荐方案**：
   - **M1-M2**：LogicalPlan → DuckDB SQL 转译层 (快速验证)
   - **M3-M4**：评估是否需要完整 DuckDBRunner

4. **类型系统**：
   - Daft 已有 Image/Embedding/Audio 类型
   - 与 DuckDB 多模态 AI 算子天然匹配

**风险预警**：
- 🟡 SQL 转译复杂性：分阶段实现
- 🔴 分布式一致性：M1 专注单机
- 🟡 类型系统差异：需建立映射表

**状态**：✅ TASK-D-001 完成，等待与 duckdb-engineer 协调 M1 实施方案
