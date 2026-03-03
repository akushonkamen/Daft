# Daft DuckDB Integration API Reference

本文档描述 Daft 与 DuckDB 集成的 API 使用方法。

---

## 目录

- [DuckDB Backend](#duckdb-backend)
- [AI Functions](#ai-functions)
- [SQL Translator](#sql-translator)
- [DuckDB Executor](#duckdb-executor)
- [Performance Monitoring](#performance-monitoring)

---

## DuckDB Backend

### SQLTranslator

将 Daft LogicalPlan 转译为 DuckDB SQL 查询。

```python
from daft.execution.backends.duckdb_translator import SQLTranslator

translator = SQLTranslator()
sql = translator.translate(logical_plan)
```

**方法**：
- `translate(logical_plan: LogicalPlan) -> str`：转译 LogicalPlan 为 SQL
- `_translate_expression(expr: Expression) -> str`：转译表达式为 SQL 片段
- `_format_literal(value: Any) -> str`：格式化字面量为 SQL 格式

### DuckDBExecutor

执行 DuckDB SQL 查询并返回结果。

```python
from daft.execution.backends.duckdb_executor import DuckDBExecutor

executor = DuckDBExecutor(
    extension_path="/path/to/ai.duckdb_extension"
)

# 执行查询
result = executor.execute_sql("SELECT * FROM table")
```

**方法**：
- `execute_sql(sql: str) -> List[Dict]`：执行 SQL 返回字典列表
- `execute_sql_to_arrow(sql: str) -> pyarrow.Table`：执行 SQL 返回 Arrow 表
- `register_table(name: str, data: Union[pd.DataFrame, pa.Table])`：注册数据表

**上下文管理器**：
```python
with DuckDBExecutor(extension_path="...") as executor:
    result = executor.execute_sql("SELECT * FROM table")
```

---

## AI Functions

### ai_filter

基于 AI 的图像过滤函数，计算图像与文本描述的相似度。

```python
from daft.functions import ai_filter

# 基本用法
filtered = df.filter(ai_filter("image", "cat") > 0.8)

# 使用列引用
filtered = df.filter(ai_filter(daft.col("image"), "cat") > 0.8)

# 指定模型
filtered = df.filter(ai_filter("image", "cat", model="clip") > 0.8)
```

**参数**：
- `image` (str | Expression)：图像列名或列引用
- `prompt` (str)：文本描述
- `model` (str, 可选)：AI 模型名称（默认: "clip"）

**返回**：
- `Expression`：相似度分数表达式 (0.0-1.0)

**支持的模型**：
- `"clip"` - CLIP 默认模型
- `"openclip"` - OpenCLIP 变体
- `"sam"` - Segment Anything Model

**SQL 转译**：
```python
# Daft 表达式
ai_filter("image", "cat", model="clip")

# 转译为 SQL
ai_filter(image, 'cat', 'clip')
```

### 使用示例

**过滤图像**：
```python
import daft
from daft.functions import ai_filter

df = daft.read_parquet("images.parquet")
cats = df.filter(ai_filter("image", "cat") > 0.8)
cats.collect()
```

**添加相似度分数列**：
```python
df = df.with_column(
    "cat_similarity",
    ai_filter("image", "cat", model="clip")
)
```

**多条件过滤**：
```python
filtered = df.filter(
    (ai_filter("image", "cat") > 0.8) &
    (ai_filter("image", "dog") < 0.3)
)
```

---

## SQL Translator

### 类型映射

| Daft 类型 | DuckDB SQL 类型 |
|-----------|----------------|
| `null` | `NULL` |
| `bool_` | `BOOLEAN` |
| `int8`, `int16`, `int32`, `int64` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` |
| `uint8`, `uint16`, `uint32`, `uint64` | `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT` |
| `float32`, `float64` | `REAL`, `DOUBLE` |
| `str` | `VARCHAR` |
| `Image` | `VARCHAR` (base64 编码) |
| `Embedding` | `FLOAT[]` |
| `Audio` | `VARCHAR` (路径) |

### 支持的操作

| 操作 | 转译 |
|------|------|
| Filter | `WHERE ...` |
| Project | `SELECT ...` |
| Aggregate | `GROUP BY ..., MIN/MAX/AVG/SUM(...)` |
| Limit | `LIMIT ...` |
| Sort | `ORDER BY ...` |

---

## DuckDB Executor

### 表注册

```python
# 注册 Pandas DataFrame
import pandas as pd
df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
executor.register_table("my_table", df)

# 注册 PyArrow Table
import pyarrow as pa
table = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
executor.register_table("my_table", table)

# 注册 Parquet 文件
executor.register_table("my_table", "path/to/data.parquet")
```

### 结果格式

**字典列表格式**：
```python
result = executor.execute_sql("SELECT * FROM table")
# [{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}]
```

**Arrow 表格式**：
```python
result = executor.execute_sql_to_arrow("SELECT * FROM table")
# <pyarrow.Table>
```

---

## Performance Monitoring

### 日志模块

```python
from monitoring import get_logger

logger = get_logger("my_module", level="INFO")
logger.info("Processing started", rows=100)
logger.error("Processing failed", error="...")
```

### 指标收集

```python
from monitoring import get_collector

metrics = get_collector()

# 计时
timer_id = metrics.start_timer("operation")
# ... 执行操作 ...
metrics.stop_timer(timer_id, "operation", rows=100)

# 计数器
metrics.increment_counter("errors", delta=1)

# 记录指标
metrics.record_metric("throughput", 100.5, "rows/s")

# 导出报告
metrics.export_json("metrics.json")
metrics.print_summary()
```

---

## 配置参数

### 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `DUCKDB_CLI_PATH` | `../duckdb/build/duckdb` | DuckDB CLI 路径 |
| `AI_EXTENSION_PATH` | `../duckdb/build/test/extension/ai.duckdb_extension` | AI Extension 路径 |
| `LOG_LEVEL` | `INFO` | 日志级别 (DEBUG/INFO/WARN/ERROR) |
| `AI_FILTER_TIMEOUT` | 30 | HTTP 超时时间（秒） |
| `AI_FILTER_MAX_RETRIES` | 3 | 最大重试次数 |
| `AI_FILTER_DEFAULT_SCORE` | 0.5 | 降级分数 |

---

## 错误处理

### 降级策略

当 AI API 调用失败时，系统返回降级分数：

```python
# 配置降级分数
os.environ["AI_FILTER_DEFAULT_SCORE"] = "0.3"

# API 失败时返回 0.3 而不是默认的 0.5
score = ai_filter("image", "cat")  # 失败返回 0.3
```

### 重试机制

HTTP 调用失败时自动重试（指数退避）：

- 第 1 次失败：等待 100ms 后重试
- 第 2 次失败：等待 200ms 后重试
- 第 3 次失败：返回降级分数

---

## 参考链接

- [Daft 官方文档](https://www.daft.readthedocs.io/)
- [DuckDB 文档](https://duckdb.org/docs/)
- [AI Filter 规范](../AI_FILTER.md)
