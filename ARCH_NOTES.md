# Daft 架构调研报告 (M0 阶段)

**日期**：2026-03-01
**调研者**：Tech Lead + Explore Agent

---

## 1. 项目结构概览

Daft 是一个 Rust + Python 混合架构的分布式 DataFrame 框架：

```
Daft/
├── src/
│   ├── daft-core/              # 核心数据结构 (Series, DataFrame)
│   ├── daft-logical-plan/      # 逻辑计划定义
│   ├── daft-local-plan/        # 本地执行计划转译
│   ├── daft-local-execution/   # 本地执行引擎
│   ├── daft-distributed/       # 分布式执行 (Ray)
│   ├── daft-runners/           # Runner 抽象层
│   ├── daft-dsl/               # 表达式 DSL
│   ├── daft-schema/            # 类型系统
│   └── python/daft/            # Python API 绑定
```

---

## 2. Runner 体系分析

### 2.1 现有 Runner 类型

**文件位置**：`src/daft-runners/src/runners.rs`

```rust
pub enum Runner {
    Ray(RayRunner),      // 分布式执行 (Ray 集群)
    Native(NativeRunner), // 本地多线程执行
}
```

| Runner | 使用场景 | 配置方式 |
|--------|----------|----------|
| `RayRunner` | 分布式集群 | `DAFT_RUNNER=ray` 或自动检测 Ray 环境 |
| `NativeRunner` | 单机多线程 | `DAFT_RUNNER=native` 或默认 |

### 2.2 扩展点分析

**添加 DuckDBRunner 的可行性**：✅ **可行**

- `Runner` 枚举可以添加新的变体
- 需要实现 `run_iter_tables` 方法
- 通过环境变量或配置自动检测

**关键代码**：
```rust
// src/daft-runners/src/runners.rs:72-75
pub enum Runner {
    Ray(RayRunner),
    Native(NativeRunner),
    // DuckDB(DuckDBRunner), // 扩展点
}
```

---

## 3. LogicalPlan / PhysicalPlan

### 3.1 LogicalPlan

**文件位置**：`src/daft-logical-plan/src/logical_plan.rs`

Daft 使用代数风格的 LogicalPlan，支持的操作：

| 操作类型 | 说明 |
|----------|------|
| `Source` | 数据源扫描 |
| `Project` / `UDFProject` | 列投影 / UDF 投影 |
| `Filter` | 过滤 |
| `Aggregate` | 聚合 |
| `Join` | 连接 |
| `Sort` / `TopN` | 排序 |
| `Repartition` | 重新分区 |
| `Sink` | 结果输出 |
| `VLLMProject` | LLM 推理（已有 AI 算子示例！）|

**重要发现**：Daft 已经有 `VLLMProject` 操作节点，说明框架对 AI 算子有支持。

### 3.2 计划转译

- **本地转译**：`src/daft-local-plan/src/translate.rs`
- **分布式转译**：`src/daft-distributed/src/plan/`

**扩展点**：可以添加针对 DuckDB 的计划转译层。

---

## 4. DuckDB 接入方案对比

### 方案 A：LogicalPlan → DuckDB SQL 转译层 (推荐)

| 方面 | 说明 |
|------|------|
| **实现方式** | 将 Daft LogicalPlan 转译为 DuckDB SQL，通过 DuckDB 执行 |
| **优势** | - 充分利用 DuckDB SQL 优化器<br>- 实现相对简单<br>- 不破坏现有 Runner 体系 |
| **劣势** | - SQL 转译复杂度随功能增加<br>- 部分 Daft 特有操作难以表达 |
| **可行性** | ✅ **高** |
| **适用阶段** | M1-M2（快速验证） |

**架构示意**：
```
Daft LogicalPlan → SQLTranslator → DuckDB SQL → DuckDB Engine → Results
```

### 方案 B：DuckDBRunner (完整 Execution Backend)

| 方面 | 说明 |
|------|------|
| **实现方式** | 添加 `DuckDBRunner` 变体到 `Runner` 枚举 |
| **优势** | - 深度集成，性能最优<br>- 统一的执行接口 |
| **劣势** | - 实现复杂度高<br>- 需要重写大量执行逻辑<br>- 与现有代码耦合深 |
| **可行性** | ⚠️ **中** |
| **适用阶段** | M3-M4（深度优化） |

**架构示意**：
```
Daft LogicalPlan → PhysicalPlan → DuckDBRunner → DuckDB C API → Results
```

### 方案 C：混合模式 (UDF Extension)

| 方面 | 说明 |
|------|------|
| **实现方式** | DuckDB 作为 UDF 引擎嵌入 Daft |
| **优势** | - 灵活性高 |
| **劣势** | - 架构复杂<br>- 性能开销大 |
| **可行性** | ❌ **低** |
| **适用阶段** | 不推荐 |

---

## 5. 推荐实施方案

### 阶段化路线图

#### M0 (当前) - 环境验证
- ✅ Daft 编译环境验证
- ✅ 架构调研完成（本文档）

#### M1 - MVP 接入
- 实现 **方案 A**：LogicalPlan → SQL 转译层
- 支持基本操作：Scan, Filter, Project, Aggregate
- 单机验证

#### M2 - SQL 转译完善
- 扩展 SQL 转译覆盖更多操作
- 处理类型转换
- 基准测试

#### M3 - AI 算子集成
- 与 DuckDB Extension AI 算子对接
- 实现 AI_filter / AI_aggregation / AI_transform

#### M4 - 完整 Backend (可选)
- 评估是否需要实现方案 B
- 根据性能测试结果决定

---

## 6. Expression 系统

**文件位置**：`src/daft-dsl/`

Daft 的表达式系统支持：
- 列引用 (`col()`)
- 字面量 (`lit()`)
- 函数调用 (`Expression` trait)
- 聚合函数

**扩展点**：可以添加 DuckDB 特有的表达式转换。

---

## 7. 类型系统

**文件位置**：`src/daft-schema/`

Daft 支持丰富的数据类型：
- 基础类型：Null, Boolean, Integer, Float, String
- 复合类型：List, Struct, FixedSizeList
- 多模态类型：Image, Embedding, Audio (已有支持！)

**重要发现**：Daft 已经有 `Image`, `Embedding` 类型，这与 DuckDB 多模态 AI 算子的集成非常匹配。

---

## 8. 风险与挑战

| 风险 | 等级 | 缓解措施 |
|------|------|----------|
| SQL 转译复杂性 | 🟡 中 | 分阶段实现，优先支持常见操作 |
| 类型系统差异 | 🟡 中 | 建立类型映射表 |
| 分布式一致性 | 🔴 高 | M1 阶段专注单机，M2+ 再考虑分布式 |
| AI 算子性能 | 🟢 低 | DuckDB Extension 处理 |

---

## 9. 下一步行动

1. **环境验证**：确保 Daft 可编译、测试通过
2. **设计 SQL 转译层**：定义 LogicalPlan → SQL 的映射规则
3. **实现 MVP**：支持基础的 Scan + Filter + Project
4. **与 duckdb-engineer 协调**：确定 AI 算子接口边界

---

**附录**：关键文件路径

```
src/daft-runners/src/runners.rs          # Runner 枚举定义
src/daft-logical-plan/src/logical_plan.rs # LogicalPlan 定义
src/daft-local-plan/src/translate.rs      # 计划转译
src/daft-dsl/src/expr.rs                  # 表达式定义
src/daft-schema/src/dtype.rs              # 类型定义
```
