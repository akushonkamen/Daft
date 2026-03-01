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
