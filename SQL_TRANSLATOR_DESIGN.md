# Daft SQL 转译层设计文档

**日期**: 2026-03-01
**阶段**: M1 - MVP 设计

---

## 1. 架构设计

### 1.1 核心转译器架构

采用访问者模式（Visitor Pattern）遍历 LogicalPlan 树：

```python
class SQLTranslator:
    def translate(self, plan: LogicalPlan) -> str:
        """根据计划类型分发到具体的转译方法"""
        pass

    def visit_source(self, source: Source) -> str:
        """转译 FROM 子句"""
        pass

    def visit_filter(self, filter: Filter) -> str:
        """转译 WHERE 子句"""
        pass

    def visit_project(self, project: Project) -> str:
        """转译 SELECT 列投影"""
        pass

    def visit_aggregate(self, aggregate: Aggregate) -> str:
        """转译 GROUP BY + 聚合"""
        pass
```

### 1.2 LogicalPlan 节点结构

| 节点类型 | 字段 | SQL 映射 |
|----------|------|----------|
| Source | output_schema, source_info | FROM / read_parquet() |
| Filter | input, predicate | WHERE |
| Project | input, projection | SELECT |
| Aggregate | input, aggregations, groupby | GROUP BY + 聚合函数 |

---

## 2. 类型映射

### 2.1 Daft → DuckDB 类型映射

```python
DAFT_TO_DUCKDB_TYPES = {
    # 基础类型
    DataType.Int8: "TINYINT",
    DataType.Int16: "SMALLINT",
    DataType.Int32: "INTEGER",
    DataType.Int64: "BIGINT",
    DataType.UInt8: "UTINYINT",
    DataType.UInt16: "USMALLINT",
    DataType.UInt32: "UINTEGER",
    DataType.UInt64: "UBIGINT",
    DataType.Float32: "FLOAT",
    DataType.Float64: "DOUBLE",
    DataType.Boolean: "BOOLEAN",
    DataType.Utf8: "VARCHAR",
    DataType.Binary: "BLOB",

    # 时间类型
    DataType.Date: "DATE",
    DataType.Timestamp: "TIMESTAMP",

    # 复杂类型
    DataType.List: "LIST",
    DataType.Struct: "STRUCT",

    # 多模态类型
    DataType.Image: "VARCHAR",      # 存储路径
    DataType.Embedding: "FLOAT[]",  # 向量数组
}
```

---

## 3. SQL 生成策略

### 3.1 Source 操作

```python
def visit_source(self, source):
    source_info = source.source_info

    if isinstance(source_info, InMemoryInfo):
        return f"SELECT * FROM {source_info.table_name}"

    elif isinstance(source_info, PhysicalScanInfo):
        paths = source_info.scan_state.get_paths()
        if len(paths) == 1:
            return f"SELECT * FROM read_parquet('{paths[0]}')"
        else:
            return " UNION ALL ".join(
                f"SELECT * FROM read_parquet('{path}')"
                for path in paths
            )
```

### 3.2 Filter 操作

```python
def visit_filter(self, filter):
    input_sql = self.translate(filter.input)
    condition = self._translate_expr(filter.predicate)
    return f"{input_sql} WHERE {condition}"

def _translate_expr(self, expr):
    if isinstance(expr, Column):
        return expr.name
    elif isinstance(expr, Literal):
        return self._format_literal(expr.value)
    elif isinstance(expr, BinaryOp):
        left = self._translate_expr(expr.left)
        right = self._translate_expr(expr.right)
        return f"({left} {expr.op.sql_op()} {right})"
```

### 3.3 Project 操作

```python
def visit_project(self, project):
    input_sql = self.translate(project.input)
    select_clause = ", ".join(
        self._translate_expr(expr) for expr in project.projection
    )
    return f"{input_sql} SELECT {select_clause}"
```

### 3.4 Aggregate 操作

```python
def visit_aggregate(self, aggregate):
    input_sql = self.translate(aggregate.input)

    # GROUP BY
    groupby_clause = ", ".join(
        self._translate_expr(expr) for expr in aggregate.groupby
    )

    # SELECT (groupby cols + aggregates)
    select_items = []
    for expr in aggregate.groupby:
        select_items.append(self._translate_expr(expr))

    for agg in aggregate.aggregations:
        select_items.append(self._translate_agg_expr(agg))

    select_clause = ", ".join(select_items)

    if groupby_clause:
        return f"{input_sql} SELECT {select_clause} GROUP BY {groupby_clause}"
    else:
        return f"{input_sql} SELECT {select_clause}"
```

---

## 4. 文件结构

```
Daft/src/python/daft/execution/backends/
├── __init__.py
├── duckdb_translator.py      # SQL 转译器
├── duckdb_executor.py        # 执行器
└── duckdb_types.py           # 类型映射

Daft/tests/
└── test_duckdb_translator.py  # 单元测试
```

---

## 5. MVP 实施步骤

| 阶段 | 内容 | 状态 |
|------|------|------|
| 1 | 基础 Source → SQL | ⏳ |
| 2 | Filter 表达式支持 | ⏳ |
| 3 | Project 操作 | ⏳ |
| 4 | Aggregate 操作 | ⏳ |
| 5 | 类型映射完善 | ⏳ |
| 6 | 端到端测试 | ⏳ |

---

## 6. 风险与挑战

| 风险 | 级别 | 缓解措施 |
|------|------|----------|
| 表达式复杂性 | 🟡 中 | MVP 限制支持基础表达式 |
| 多模态类型 | 🟡 中 | 特殊处理 Image/Embedding |
| 分布式场景 | 🔴 高 | MVP 仅支持单机 |
| 性能开销 | 🟢 低 | 转译仅在计划阶段 |

---

## 7. 与 DuckDB Extension 集成

### 7.1 AI 算子调用

```python
# Daft 侧
df.filter(ai_filter("image", "cat"))

# 转译为 SQL
SELECT * FROM table WHERE ai_filter(image, 'cat', 'clip') > 0.8
```

### 7.2 大对象传输

- Daft Image 类型 → 序列化为 BLOB → DuckDB
- 结果：DOUBLE 分数数组

---

**状态**: 🟢 设计完成，等待实现
