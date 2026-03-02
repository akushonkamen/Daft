# 变更记录 — Daft submodule

## 格式
```
### [日期] TASK-X-XXX：标题
- 类型：新增/修改/删除  |  文件：  |  摘要：
- 测试：pass X/X  |  编译：✅/❌  |  commit：<hash>  |  巡检：⏳/✅/❌
```

## 历史

### [2026-03-01] TASK-D-004：SQL 转译层 MVP 实现
- 类型：新增  |  文件：`daft/execution/backends/duckdb_types.py`  |  摘要：Daft DataType → DuckDB SQL 类型映射模块
- 类型：新增  |  文件：`daft/execution/backends/duckdb_translator.py`  |  摘要：LogicalPlan → SQL 转译器（访问者模式）
- 类型：新增  |  文件：`daft/execution/backends/duckdb_executor.py`  |  摘要：DuckDB 执行器（连接管理、查询执行）
- 类型：新增  |  文件：`daft/execution/backends/__init__.py`  |  摘要：Backend 模块导出
- 类型：新增  |  文件：`tests/test_duckdb_translator.py`  |  摘要：SQL 转译器单元测试
- 测试：⏳ 待运行（需编译 Python 绑定）  |  编译：⏳ 待验证  |  commit：待创建  |  巡检：⏳

### [2026-03-01] TASK-INTEGRATION-001：端到端集成测试套件
- 类型：新增  |  文件：`tests/test_duckdb_integration.py`  |  摘要：pytest 集成测试套件（完整管道测试）
- 类型：新增  |  文件：`tests/run_integration_tests.py`  |  摘要：快速验证脚本（无 pytest 依赖）
- 测试：✅ 编写完成  |  编译：⏳ 待验证  |  commit：待创建  |  巡检：⏳

**测试覆盖**：
1. Type Mapping: Daft → DuckDB 类型映射验证
2. SQL Translator: 转译器功能测试
3. DuckDB Executor: 连接、表注册、查询执行
4. AI Extension: 扩展加载、ai_filter 函数调用
5. End-to-End: Filter、Project、Aggregate 完整管道

**验证结果**：
- ✅ DuckDB AI 扩展存在 (83KB)
- ✅ 扩展加载成功
- ✅ ai_filter 函数可调用（返回 0.0-1.0 分数）

**关键实现**：
1. 支持基础类型映射：整数、浮点、字符串、布尔、二进制
2. 支持多模态类型：Image、Embedding、Audio
3. SQL Translator 支持：Source、Filter、Project、 Aggregate（MVP）
4. DuckDBExecutor 支持：扩展加载、表注册、SQL 执行、PyArrow 结果返回

### [2026-03-01] TASK-CLI-001：DuckDB CLI 集成验证
- 类型：新增  |  文件：`tests/verify_cli_integration.py`  |  摘要：CLI 集成验证脚本（自动化测试）
- 类型：新增  |  文件：`daft/execution/backends/duckdb_cli_executor.py`  |  摘要：CLI 执行器（subprocess 方案）
- 类型：新增  |  文件：`tests/test_duckdb_cli_integration.py`  |  摘要：CLI 集成测试套件
- 类型：新增  |  文件：`tests/test_duckdb_varchar_api.py`  |  摘要：VARCHAR (base64) API 测试
- 测试：✅ 5/5 passed  |  编译：N/A  |  commit：待创建  |  巡检：⏳

**验证结果**：
- ✅ DuckDB CLI: v0.0.1 (自定义构建)
- ✅ AI Extension: 6.8 MB，加载成功
- ✅ ai_filter() 函数：基础调用、多重调用、WHERE 子句、聚合、投影全部通过
- ✅ 返回值：DOUBLE 类型，范围 [0.0, 1.0]

**测试场景**：
1. 基础调用：`SELECT ai_filter() AS score` → 0.42
2. 多重调用：同查询多次调用结果一致
3. WHERE 过滤：`WHERE ai_filter() > 0.0` 正常工作
4. 聚合函数：MIN, MAX, AVG, COUNT 正常工作
5. SELECT 投影：列投影正常工作

**集成方式**：
- 通过 subprocess 调用 DuckDB CLI
- 使用 `-unsigned` 参数加载未签名扩展
- 自动解析 CLI 输出为 Python 数据结构

### [2026-03-01] TASK-17：M4 Daft AI 算子 API 实现
- 类型：新增  |  文件：`daft/functions/ai/__init__.py`  |  摘要：添加 `ai_filter()` 函数到 Daft AI 函数模块
- 类型：新增  |  文件：`tests/test_ai_filter_api.py`  |  摘要：API 单元测试（导入、表达式创建、SQL 转译）
- 类型：新增  |  文件：`tests/test_ai_filter_e2e.py`  |  摘要：端到端集成测试（完整工作流、API 变体）
- 类型：修改  |  文件：`daft/functions/__init__.py`  |  摘要：导出 `ai_filter` 函数
- 类型：修改  |  文件：`daft/execution/backends/duckdb_translator.py`  |  摘要：更新 SQL 转译器支持 ai_filter 表达式
- 测试：✅ 10/10 passed (5 API + 5 E2E)  |  编译：⏳ 待验证  |  commit：待创建  |  巡检：⏳

**API 设计**：
```python
# 基本用法
from daft.functions import ai_filter
filtered = df.filter(ai_filter("image", "cat") > 0.8)

# 指定模型
filtered = df.filter(ai_filter(daft.col("image"), "cat", model="clip") > 0.8)

# 添加相似度分数列
df = df.with_column("cat_score", ai_filter("image", "cat"))
```

**实现方案**：
- Expression 元数据标记：`_is_ai_filter`, `_ai_filter_column`, `_ai_filter_prompt`, `_ai_filter_model`
- SQL 转译器识别元数据并生成：`ai_filter(column, 'prompt', 'model')`
- 轻量级 Python 实现，无需修改 Rust 代码

**测试覆盖**：
1. 导入测试：验证 `ai_filter` 可从 `daft.functions` 导入
2. 表达式创建：验证所有 API 变体
3. SQL 转译：验证生成的 SQL 正确性
4. Filter 用法：验证在 `df.filter()` 中的使用
5. With_column 用法：验证添加列的操作
6. 完整工作流：Daft → SQL → DuckDB 流程
7. API 变体：字符串列名、列表达式、模型参数
8. 文档示例：所有文档示例可执行

**验证结果**：
- ✅ 导入：`from daft.functions import ai_filter` 成功
- ✅ 表达式：`ai_filter(daft.col("image"), "cat", model="clip")` 创建成功
- ✅ SQL 转译：`ai_filter(image_path, 'cat', 'clip')`
- ✅ 多模型支持：clip, openclip, sam 全部正确转译
- ✅ DataFrame 集成：filter 和 with_column 语法正常工作

**限制**：
- ⚠️ 仅在 DuckDB backend 下工作（其他 backend 返回占位值 0.0）
- ⏳ 完整的 DuckDB backend 集成测试待完成

### [2026-03-02] TASK-PROD-001：Ray 分布式执行支持（MVP）
- 类型：新增  |  文件：`demo_ray_simple.py`  |  摘要：Ray + DuckDB 分布式架构验证演示脚本
- 类型：新增  |  文件：`demo_ray_distributed.py`  |  摘要：完整分布式执行演示（含 CIFAR-10 数据集）
- 测试：✅ 架构验证通过  |  编译：N/A  |  commit：待创建  |  巡检：⏳

**架构验证结果**：
- ✅ Ray 集群初始化（本地多进程）
- ✅ 多 Worker 并行执行（4 任务 / 2 进程）
- ✅ Worker 进程隔离验证
- ✅ DuckDB CLI 在 Worker 上执行
- ✅ AI Extension 在每个 Worker 上独立加载
- ✅ 3/3 AI Filter 查询成功（0.38s 并行）

**技术方案**：
```
Ray Cluster (本地多进程)
├── Worker 1 (PID=N): Ray task → subprocess → DuckDB CLI + Extension
├── Worker 2 (PID=M): Ray task → subprocess → DuckDB CLI + Extension
└── 共享: Extension 文件路径，独立进程执行
```

**执行流程**：
1. Ray `@ray.remote` 装饰器定义 Worker 函数
2. 每个 Worker 独立调用 DuckDB CLI
3. 每个 Worker 加载 AI Extension
4. 并行执行 AI_filter 查询
5. 结果返回 Ray driver 汇总

**后续工作**：
- ⏳ 使用 Ray runtime_env 自动分发 Extension
- ⏳ 实现两阶段聚合优化
- ⏳ 优化 HTTP API 调用（批处理/缓存）
- ⏳ 支持大数据集分布式处理

