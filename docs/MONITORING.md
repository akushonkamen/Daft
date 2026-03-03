# 监控和日志使用指南

本文档介绍如何使用 Daft-DuckDB 集成的监控和日志功能。

## 概述

监控系统提供以下功能：

- **结构化日志**：JSON 格式日志，便于日志聚合和分析
- **性能指标**：查询耗时、AI API 延迟、吞吐量等
- **错误统计**：错误率统计和错误日志追踪
- **导出功能**：支持导出为 JSON 格式

## 日志模块

### 基本使用

```python
from monitoring.logger import get_logger

# 获取日志器
logger = get_logger("my_module", level="INFO")

# 记录不同级别的日志
logger.info("Query started", query_id="123", table="products")
logger.debug("Processing row", row_id=456)
logger.warning("High latency detected", latency_ms=5000)
logger.error("Query failed", error_code=500, message="Connection timeout")
```

### 日志级别

| 级别     | 用途                           |
|----------|--------------------------------|
| DEBUG    | 调试信息，生产环境通常关闭     |
| INFO     | 一般信息，记录关键操作流程     |
| WARNING  | 警告信息，不影响系统运行       |
| ERROR    | 错误信息，需要关注             |
| CRITICAL | 严重错误，可能影响系统运行     |

### 日志输出格式

```json
{
  "timestamp": "2026-03-03T12:34:56.789Z",
  "level": "INFO",
  "logger": "my_module",
  "message": "Query started",
  "module": "query_executor",
  "function": "execute_query",
  "line": 42,
  "query_id": "123",
  "table": "products"
}
```

## 性能指标模块

### 基本使用

```python
from monitoring.metrics import get_collector

# 获取全局指标收集器
metrics = get_collector()

# 记录指标
metrics.record_metric("query_latency", 123.45, unit="ms", query_type="select")

# 计数器
metrics.increment_counter("queries_executed", delta=1, status="success")
metrics.increment_counter("queries_failed", delta=1, status="error")

# 计时器
timer_id = metrics.start_timer("ai_filter_execution")
# ... 执行操作 ...
elapsed = metrics.stop_timer(timer_id, "ai_filter_execution", model="clip")
```

### 查看指标摘要

```python
# 打印摘要
metrics.print_summary()

# 获取摘要数据
summary = metrics.get_summary()
print(f"Total metrics: {summary['total_metrics']}")
print(f"Total counters: {summary['total_counters']}")
```

### 导出指标

```python
# 导出为 JSON
metrics.export_json("performance_metrics.json")
```

## 在 DuckDB Backend 中使用监控

### 启用监控

```python
from daft import DataFrame
from monitoring.logger import get_logger
from monitoring.metrics import get_collector

logger = get_logger("duckdb_backend")
metrics = get_collector()

# 使用 DuckDB backend 执行查询
timer_id = metrics.start_timer("duckdb_query")
try:
    df = DataFrame.from_pydict({"data": [1, 2, 3]})
    result = df.with_column("doubled", df["data"] * 2).collect()
    logger.info("Query completed successfully", rows=len(result))
    metrics.increment_counter("duckdb_queries", status="success")
except Exception as e:
    logger.error("Query failed", error=str(e))
    metrics.increment_counter("duckdb_queries", status="error")
finally:
    metrics.stop_timer(timer_id, "duckdb_query")
```

## AI 扩展监控

### 监控 AI Filter 调用

```python
from monitoring.metrics import get_collector

metrics = get_collector()

# 记录 AI filter 调用
metrics.record_metric("ai_filter_latency", 234.5, unit="ms", model="clip")
metrics.record_metric("ai_filter_batch_size", 10, unit="rows")
metrics.increment_counter("ai_filter_calls", model="clip")
```

### 监控降级策略

```python
# 记录降级事件
metrics.increment_counter("ai_degradation", reason="timeout")
metrics.increment_counter("ai_degradation", reason="api_error")

logger.warning("AI filter degraded", fallback_score=0.5, reason="timeout")
```

## 性能基准测试

### 运行基准测试

```bash
# 运行性能基准测试（完整模式）
cd integration_tests
python benchmark_ray_performance.py

# CI 模式（快速验证）
CI=true python benchmark_ray_performance.py
```

### 基准测试输出

基准测试生成以下文件：

- `benchmark_results.json`：性能数据（JSON 格式）
- `benchmark_report.md`：可读性报告（Markdown 格式）

### 基准测试指标

| 指标            | 说明                         |
|-----------------|------------------------------|
| 单机吞吐量      | 单进程 DuckDB CLI 处理速度   |
| Ray 吞吐量      | Ray 分布式执行处理速度       |
| AI API 延迟     | 单次 AI filter 调用延迟      |
| 批处理效率      | 不同批次大小的效率对比       |
| 并发性能        | 并发查询的性能表现           |

## 日志配置

### 环境变量

```bash
# 设置日志级别
export DAFT_LOG_LEVEL=INFO

# 启用/禁用遥测
export DO_NOT_TRACK=true
```

### 代码配置

```python
import logging
from monitoring.logger import get_logger

# 自定义日志级别
logger = get_logger("my_app", level=logging.DEBUG)
```

## 集成到现有系统

### 与 ELK Stack 集成

由于日志是 JSON 格式，可以直接与 ELK Stack 集成：

```bash
# 使用 Filebeat 收集日志
# filebeat.yml
filebeat.inputs:
- type: log
  paths:
    - /var/log/daft/*.log
  json.keys_under_root: true
  json.add_error_key: true

output.elasticsearch:
  hosts: ["localhost:9200"]
```

### 与 Prometheus 集成

可以将指标导出为 Prometheus 格式（需要额外适配器）：

```python
# 伪代码示例
from prometheus_client import Counter, Histogram

# 基于 metrics collector 创建 Prometheus 指标
query_counter = Counter('daft_queries_total', 'Total queries', ['status'])
query_latency = Histogram('daft_query_latency_seconds', 'Query latency')
```

## 故障排查

### 日志未输出

**问题**：日志没有输出到控制台

**解决方案**：
1. 检查日志级别设置
2. 确保没有重复添加 handler
3. 检查日志消息是否被过滤

### 指标未记录

**问题**：指标没有记录

**解决方案**：
1. 确保调用了正确的记录函数
2. 检查是否有线程安全问题
3. 验证 metrics collector 已初始化

## 参考资源

- `monitoring/logger.py`：日志模块源码
- `monitoring/metrics.py`：指标模块源码
- `integration_tests/benchmark_ray_performance.py`：基准测试示例
