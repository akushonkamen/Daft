# Daft × DuckDB 多模态 AI 平台演示

## 项目概述

将 DuckDB 的多模态 AI 能力嵌入 Daft 分布式 DataFrame 框架，实现：
- 🚀 **Daft DataFrame API** - 直观的数据操作接口
- 🦆 **DuckDB SQL** - 强大的关系型查询能力
- 🤖 **多模态 AI** - 图像语义理解、过滤、聚合

---

## M0-M4 里程碑完成情况

### ✅ M0: 环境验证与架构调研
- Daft Python 绑定编译成功（v0.3.0-dev0）
- DuckDB AI Extension 集成验证
- 架构方案确定：LogicalPlan → SQL → DuckDB

### ✅ M1: Daft DuckDB Execution Backend（单机）
- SQL 转译层 MVP
- 支持基础操作：Filter, Project, Aggregate
- 类型系统映射：Daft ↔ DuckDB

### ✅ M2: LogicalPlan → SQL 转译层 MVP
- Visitor 模式转译器
- 支持复杂表达式和嵌套查询
- 多模态类型支持（Image, Embedding, Audio）

### ✅ M3: DuckDB 多模态 AI 算子
- `ai_filter()` 函数 - 语义相似度过滤
- Real HTTP API - CLIP 语义匹配
- VARCHAR (base64) API - 图像编码支持

### ✅ M4: Daft AI 算子 API
- `daft.functions.ai_filter()` - 高级 API
- DataFrame 方法集成
- 端到端工作流验证

---

## 快速开始

### 1. 环境准备

```bash
# 激活虚拟环境
cd Daft/
source .venv/bin/activate

# 验证 Daft 安装
python3 -c "import daft; print('Daft:', daft.__version__)"
```

### 2. 基础用法

```python
import daft
from daft.functions import ai_filter

# 读取图像数据
df = daft.read_parquet("images.parquet")

# 方法1: 使用字符串列名
cat_images = df.filter(ai_filter("image", "cat") > 0.8)

# 方法2: 使用列表达式
cat_images = df.filter(ai_filter(daft.col("image"), "cat", model="clip") > 0.8)

# 方法3: 添加相似度分数列
df = df.with_column("cat_score", ai_filter("image", "cat"))
df = df.filter(df["cat_score"] > 0.8)

# 显示结果
cat_images.show()
```

### 3. 完整工作流

```python
import daft
from daft.functions import ai_filter

# 1. 读取数据
df = daft.read_parquet("images.parquet")

# 2. 计算多个类别的相似度分数
df = df.with_column("cat_score", ai_filter("image", "cat"))
df = df.with_column("dog_score", ai_filter("image", "dog"))
df = df.with_column("bird_score", ai_filter("image", "bird"))

# 3. 过滤高分图像
cats = df.filter(df["cat_score"] > 0.8)
dogs = df.filter(df["dog_score"] > 0.8)
birds = df.filter(df["bird_score"] > 0.8)

# 4. 排序并显示
cats = cats.sort(df["cat_score"], desc=True)
cats.select("image_path", "cat_score").show()
```

---

## API 参考

### `ai_filter(image, prompt, model="clip")`

计算图像与文本提示的语义相似度分数（0.0-1.0）。

**参数**：
- `image` (str | Expression) - 图像列名或列表达式
- `prompt` (str) - 文本提示（如 "cat", "dog", "sunset"）
- `model` (str) - 嵌入模型，默认 "clip"，可选 "openclip", "sam"

**返回**：
- Expression - Float64 类型，相似度分数

**示例**：
```python
# 基础用法
score_expr = ai_filter("image", "cat")

# 指定模型
score_expr = ai_filter(daft.col("image"), "cat", model="clip")

# 在过滤中使用
filtered = df.filter(ai_filter("image", "cat") > 0.8)

# 添加分数列
df = df.with_column("score", ai_filter("image", "cat"))
```

---

## SQL 转译示例

### Daft 表达式 → DuckDB SQL

| Daft 表达式 | DuckDB SQL |
|------------|------------|
| `ai_filter("image", "cat")` | `ai_filter(image, 'cat', 'clip')` |
| `ai_filter(col("img"), "dog", model="clip")` | `ai_filter(img, 'dog', 'clip')` |
| `ai_filter("img", "cat", model="openclip")` | `ai_filter(img, 'cat', 'openclip')` |

### 完整查询示例

```python
# Daft 代码
df = daft.read_parquet("images.parquet")
filtered = df.filter(ai_filter("image", "cat") > 0.8)
result = filtered.select("id", "image_path")
```

```sql
-- 转译后的 SQL
SELECT id, image_path
FROM read_parquet('images.parquet')
WHERE ai_filter(image, 'cat', 'clip') > 0.8;
```

---

## 性能特性

### Subprocess 开销
- 简单查询：~16.5ms
- AI filter 查询：~17.2ms
- **结论**：适合 MVP 验证，生产环境可优化

### 优化建议
1. **批量查询** - 减少 subprocess 调用次数
2. **连接池** - 复用 DuckDB 连接
3. **并行执行** - 利用 Daft 分布式能力

---

## 测试覆盖

### 单元测试
- ✅ API 导入测试
- ✅ 表达式创建测试
- ✅ SQL 转译测试
- ✅ 多模型支持测试

### E2E 测试
- ✅ 完整工作流测试
- ✅ API 变体测试
- ✅ 文档示例测试

**总计**：10/10 测试通过

---

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                    Daft DataFrame API                       │
│  df.filter(ai_filter("image", "cat") > 0.8)               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   LogicalPlan Builder                       │
│  - Source: ParquetScan                                     │
│  - Filter: ai_filter expression                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    SQL Translator                           │
│  ai_filter(image, 'cat', 'clip') > 0.8                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  DuckDB CLI Executor                        │
│  subprocess: ["./duckdb", "-unsigned", "-c", sql]          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   DuckDB + AI Extension                     │
│  - Parse SQL                                               │
│  - Execute ai_filter() (HTTP call to CLIP API)            │
│  - Return results                                          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Results Parsing                          │
│  Parse CLI output → Python dicts → Daft DataFrame          │
└─────────────────────────────────────────────────────────────┘
```

---

## 文件结构

```
Daft/
├── daft/
│   ├── functions/
│   │   ├── ai/
│   │   │   └── __init__.py          # ai_filter() 函数实现
│   │   └── __init__.py              # 导出 ai_filter
│   └── execution/
│       └── backends/
│           ├── duckdb_translator.py # SQL 转译器
│           ├── duckdb_cli_executor.py # CLI 执行器
│           └── duckdb_types.py      # 类型映射
├── tests/
│   ├── test_ai_filter_api.py       # API 单元测试
│   ├── test_ai_filter_e2e.py       # E2E 集成测试
│   └── test_end_to_end_daft_duckdb.py # 完整架构测试
├── CHANGES.md                       # 变更记录
├── Discussion.md                    # 讨论记录
└── DEMO.md                          # 本文档
```

---

## 下一步 (M5)

### CI/CD 全流程
- [ ] GitHub Actions 配置
- [ ] 自动化测试
- [ ] 性能基准测试

### 生产优化
- [ ] 批量查询优化
- [ ] 连接池实现
- [ ] 分布式执行

### 功能扩展
- [ ] `ai_aggregation()` - AI 聚合函数
- [ ] `ai_transform()` - AI 转换函数
- [ ] 更多模型支持

---

## 贡献者

- **Tech Lead** - 项目协调与架构设计
- **daft-engineer** - Daft 框架侧实现
- **duckdb-engineer** - DuckDB Extension 开发

---

## 许可证

本项目遵循 Daft 和 DuckDB 的开源许可证。

---

**文档版本**: v1.0
**最后更新**: 2026-03-01
**项目状态**: M0-M4 完成 ✅
