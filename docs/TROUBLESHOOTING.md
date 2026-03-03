# 故障排查指南

本文档提供 Daft-DuckDB 集成常见问题的解决方案。

## 目录

- [安装问题](#安装问题)
- [编译问题](#编译问题)
- [运行时问题](#运行时问题)
- [性能问题](#性能问题)
- [AI 扩展问题](#ai-扩展问题)
- [分布式问题](#分布式问题)

## 安装问题

### Python 版本不兼容

**错误信息**：`ERROR: This package requires Python 3.10 or later`

**解决方案**：

```bash
# 检查 Python 版本
python --version

# 使用 pyenv 安装正确版本
pyenv install 3.11
pyenv local 3.11

# 或使用 conda
conda create -n daft python=3.11
conda activate daft
```

### 依赖安装失败

**错误信息**：`Could not find a version that satisfies the requirement...`

**解决方案**：

```bash
# 更新 pip
pip install --upgrade pip

# 使用国内镜像
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple daft

# 或使用 conda
conda install -c conda-forge daft
```

### 子模块未初始化

**错误信息**：`fatal: not a git repository`

**解决方案**：

```bash
# 初始化子模块
git submodule update --init --recursive

# 如果子模块指针有问题
git submodule sync --recursive
git submodule update --init --recursive
```

## 编译问题

### CMake 未找到

**错误信息**：`CMake Error: Could not find CMAKE_ROOT`

**解决方案**：

```bash
# Ubuntu/Debian
apt-get install cmake

# macOS
brew install cmake

# 验证安装
cmake --version
```

### DuckDB 编译失败

**错误信息**：`make: *** No rule to make target 'all'`

**解决方案**：

```bash
cd duckdb
mkdir -p build
cd build
cmake ..
make -j$(nproc)

# 清理后重试
make clean
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### AI Extension 编译失败

**错误信息**：`undefined reference to 'duckdb::...'`

**解决方案**：

```bash
# 确保 DuckDB 已正确编译
cd duckdb
make -j$(nproc)

# 清理 extension build
cd build/extension/ai
rm -rf CMakeFiles cmake_install.cmake Makefile

# 重新编译
cd ../../
make ai.duckdb_extension
```

## 运行时问题

### 扩展加载失败

**错误信息**：`Extension Load Error: ...ai.duckdb_extension`

**解决方案**：

1. **检查扩展路径**：

```python
import os
from pathlib import Path

# 验证扩展文件存在
ext_path = Path("/path/to/ai.duckdb_extension")
if not ext_path.exists():
    print(f"Extension not found at {ext_path}")
    # 搜索可能的扩展位置
    for p in Path(".").rglob("ai.duckdb_extension"):
        print(f"Found extension at: {p}")
```

2. **检查 DuckDB 版本**：

```bash
# 确保版本匹配（1.4.4）
cd duckdb/build
./duckdb --version
```

3. **使用绝对路径加载**：

```python
import duckdb

con = duckdb.connect()
ext_path = "/absolute/path/to/ai.duckdb_extension"
con.execute(f"LOAD '{ext_path}'")
```

### Ray 初始化失败

**错误信息**：`RuntimeError: Ray is not available`

**解决方案**：

```bash
# 安装 Ray
pip install "ray[default]"

# 或使用 conda
conda install -c conda-forge ray
```

```python
# 检查 Ray 状态
import ray
if ray.is_initialized():
    print("Ray is initialized")
else:
    ray.init(ignore_reinit_error=True)
```

### 内存不足错误

**错误信息**：`MemoryError: std::bad_alloc`

**解决方案**：

1. **增加 DuckDB 内存限制**：

```python
import duckdb

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET max_memory='16GB'")
```

2. **增加 Ray 内存**：

```python
ray.init(
    memory=16_000_000_000,  # 16GB
    object_store_memory=4_000_000_000  # 4GB
)
```

3. **分批处理数据**：

```python
# 分批读取和处理
for batch in df.iter_batches(batch_size=1000):
    process_batch(batch)
```

## 性能问题

### 查询缓慢

**症状**：查询执行时间过长

**诊断步骤**：

1. **检查是否使用了索引**：

```sql
-- 查看表结构
DESCRIBE table_name;

-- 创建索引
CREATE INDEX idx_column ON table_name(column);
```

2. **检查执行计划**：

```python
import duckdb

con = duckdb.connect()
con.execute("EXPLAIN SELECT * FROM table_name WHERE column = 'value'")
print(con.fetchdf())
```

3. **检查配置**：

```python
# 增加线程数
con.execute("SET threads=8")

# 启用向量化
con.execute("SET enable_object_cache=true")
```

### AI Filter 性能差

**症状**：AI filter 调用延迟高

**解决方案**：

1. **使用批处理**：

```python
# 不推荐：逐个调用
for item in data:
    score = ai_filter(item, "prompt", "model")

# 推荐：批量调用
scores = ai_filter_batch(data, "prompt", "model")
```

2. **调整批大小**：

```python
# 找到最佳批大小（10-50）
for batch_size in [10, 20, 50, 100]:
    test_performance(batch_size)
```

3. **启用缓存**：

```python
# 缓存重复查询
con.execute("SET enable_object_cache=true")
con.execute("SET enable_http_cache=true")
```

### 并发性能下降

**症状**：并发执行时性能反而下降

**解决方案**：

1. **调整并发数**：

```python
# 找到最佳并发数
import ray

ray.init(num_cpus=4)  # 根据 CPU 核心数调整
```

2. **检查资源争用**：

```bash
# 查看资源使用
htop
```

3. **使用对象存储减少数据传输**：

```python
@ray.remote
def process_data(data_ref):
    # 使用对象存储引用
    result = data_ref.map(filter_func)
    return result
```

## AI 扩展问题

### API 超时

**错误信息**：`Timeout error calling AI API`

**解决方案**：

1. **增加超时时间**：

```cpp
// 在扩展代码中
int timeout_seconds = 60;  // 增加到 60 秒
```

2. **使用降级策略**：

```python
# 扩展会自动返回降级分数
# 配置降级分数
degradation_score = 0.5  # 默认值
```

3. **重试机制**：

```python
# 扩展内置指数退避重试
# 可以调整重试次数
max_retries = 3
```

### 无效的模型名称

**错误信息**：`Invalid model: unknown_model`

**解决方案**：

```python
# 使用支持的模型
valid_models = ["clip", "blip", "llm"]  # 检查扩展文档

# 如果模型不支持，会返回降级分数
# 不会抛出错误
```

### 降级分数异常

**症状**：总是返回固定的降级分数

**解决方案**：

1. **检查 API 密钥**：

```bash
# 设置 API 密钥
export OPENAI_API_KEY="your-key"
export ANTHROPIC_API_KEY="your-key"
```

2. **检查网络连接**：

```bash
# 测试 API 连接
curl -I https://api.openai.com
```

3. **查看日志**：

```python
from monitoring.logger import get_logger

logger = get_logger("ai_extension")
# 查看详细错误信息
```

## 分布式问题

### Worker 连接失败

**错误信息**：`RuntimeError: Failed to connect to Ray cluster`

**解决方案**：

1. **检查防火墙**：

```bash
# 开放 Ray 端口
ufw allow 6379/tcp
ufw allow 8265/tcp
```

2. **检查 head 节点地址**：

```python
# 确保使用正确的地址
ray.init(address="ray://head-node:10001")

# 或使用环境变量
export RAY_HEAD_ADDRESS="head-node:6379"
```

3. **重启集群**：

```bash
# 在 head 节点
ray stop
ray start --head --port=6379

# 在 worker 节点
ray stop
ray start --address=head-node:6379
```

### 数据倾斜

**症状**：某些 worker 处理时间明显更长

**解决方案**：

1. **重新分区数据**：

```python
# 增加分区数
df = df.repartition(16)
```

2. **使用随机分区**：

```python
# 随机打乱数据
df = df.shuffle(seed=42)
```

3. **监控数据分布**：

```python
# 检查分区大小
for partition in df.get_partitions():
    print(f"Partition size: {partition.count_rows()}")
```

### 任务卡死

**症状**：任务无限等待，无输出

**解决方案**：

1. **设置超时**：

```python
@ray.remote(timeout=300)  # 5 分钟超时
def long_running_task():
    ...
```

2. **检查死锁**：

```python
# 避免在远程任务中等待其他远程任务
# 使用 ray.wait 代替
```

3. **查看 Ray Dashboard**：

```
访问 http://head-node:8265 查看任务状态
```

## 调试技巧

### 启用详细日志

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("duckdb").setLevel(logging.DEBUG)
logging.getLogger("ray").setLevel(logging.DEBUG)
```

### 使用 Python 调试器

```python
import pdb

def my_function():
    # 设置断点
    pdb.set_trace()
    # 代码继续...
```

### 性能分析

```python
# 使用 cProfile
import cProfile

cProfile.run('my_function()', 'profile_stats')

# 查看结果
import pstats
p = pstats.Stats('profile_stats')
p.sort_stats('cumtime').print_stats(10)
```

### Ray 内存分析

```bash
# 使用 Ray 内存监控
ray memory

# 查看对象引用
ray.list_named_actors()
```

## 获取帮助

如果问题仍未解决：

1. **查看日志**：`/var/log/daft/`
2. **查看测试结果**：`integration_tests/`
3. **查看性能指标**：`monitoring/metrics.py`
4. **提交 Issue**：在项目仓库提交详细的问题报告

## 相关资源

- [DuckDB 文档](https://duckdb.org/docs/)
- [Ray 故障排查](https://docs.ray.io/en/latest/ray-troubleshooting.html)
- [Daft 文档](https://docs.daft.ai/)
