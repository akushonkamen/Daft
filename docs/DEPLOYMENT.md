# 部署指南

本文档介绍如何部署 Daft-DuckDB 多模态分析平台。

## 目录

- [环境要求](#环境要求)
- [本地开发环境](#本地开发环境)
- [生产环境部署](#生产环境部署)
- [Docker 部署](#docker-部署)
- [Kubernetes 部署](#kubernetes-部署)
- [性能优化](#性能优化)

## 环境要求

### 最低要求

| 组件      | 最低版本 |
|-----------|----------|
| Python    | 3.10    |
| DuckDB   | 1.4.4   |
| Ray      | 2.5.0   |
| 操作系统 | Linux/macOS |

### 推荐配置

| 组件      | 推荐配置                    |
|-----------|-----------------------------|
| CPU       | 4 核心以上                  |
| 内存      | 8GB 以上                    |
| 存储      | SSD，100GB 以上             |
| 网络      | 1Gbps（分布式场景）         |

## 本地开发环境

### 1. 克隆仓库

```bash
git clone <repository-url>
cd daft-duckdb-multimodal
```

### 2. 初始化子模块

```bash
git submodule update --init --recursive
```

### 3. 编译 DuckDB

```bash
cd duckdb
mkdir -p build
cd build
cmake ..
make -j$(nproc)
```

### 4. 编译 AI Extension

```bash
cd duckdb/build
make ai.duckdb_extension
```

### 5. 安装 Python 依赖

```bash
cd Daft
pip install -e ".[ray,aws,s3]"
pip install -r integration_tests/requirements.txt
```

### 6. 验证安装

```bash
# 运行单元测试
pytest tests/

# 运行集成测试
cd integration_tests
python test_e2e_ai_pipeline.py
```

## 生产环境部署

### 1. 系统配置

#### 内核参数优化

```bash
# /etc/sysctl.conf
vm.max_map_count=262144
fs.file-max=2097152
net.core.somaxconn=1024
```

#### 资源限制

```bash
# /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536
* soft nproc 4096
* hard nproc 4096
```

### 2. Python 环境

#### 使用 Pyenv

```bash
pyenv install 3.11
pyenv global 3.11
```

#### 使用 Conda

```bash
conda create -n daft-duckdb python=3.11
conda activate daft-duckdb
```

### 3. 依赖安装

```bash
pip install --upgrade pip
pip install -e ".[ray,aws,s3,monitoring]"
```

### 4. 配置文件

创建 `/etc/daft/config.toml`：

```toml
[duckdb]
extension_path = "/opt/duckdb/extensions/ai.duckdb_extension"
memory_limit = "4GB"
threads = 4

[ray]
dashboard_host = "0.0.0.0"
dashboard_port = 8265
num_cpus = 4

[monitoring]
log_level = "INFO"
metrics_export = "json"
metrics_path = "/var/log/daft/metrics"
```

### 5. 服务启动

#### 使用 Supervisor

```ini
# /etc/supervisor/conf.d/daft-duckdb.conf
[program:daft-duckdb]
command=/opt/daft-duckdb/venv/bin/python -m daft_service
directory=/opt/daft-duckdb
user=daft
autostart=true
autorestart=true
stderr_logfile=/var/log/daft/err.log
stdout_logfile=/var/log/daft/out.log
```

#### 使用 systemd

```ini
# /etc/systemd/system/daft-duckdb.service
[Unit]
Description=Daft DuckDB Service
After=network.target

[Service]
Type=simple
User=daft
WorkingDirectory=/opt/daft-duckdb
Environment="PATH=/opt/daft-duckdb/venv/bin"
ExecStart=/opt/daft-duckdb/venv/bin/python -m daft_service
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Docker 部署

### Dockerfile

```dockerfile
FROM python:3.11-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制并安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY . .

# 安装 Daft
RUN pip install -e .

# 暴露端口
EXPOSE 8265

# 启动命令
CMD ["python", "-m", "daft_service"]
```

### Docker Compose

```yaml
version: '3.8'

services:
  daft-head:
    build: .
    ports:
      - "8265:8265"
    environment:
      - RAY_ROLE=head
      - DAFT_DUCKDB_EXTENSION=/app/extensions/ai.duckdb_extension
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    command: ray start --head --port=6379 --dashboard-host=0.0.0.0

  daft-worker:
    build: .
    depends_on:
      - daft-head
    environment:
      - RAY_ROLE=worker
      - RAY_HEAD_ADDRESS=daft-head:6379
    deploy:
      replicas: 2
    command: ray start --address=daft-head:6379

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    depends_on:
      - daft-head
```

## Kubernetes 部署

### Namespace

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: daft-duckdb
```

### ConfigMap

```yaml
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: daft-config
  namespace: daft-duckdb
data:
  DUCKDB_MEMORY_LIMIT: "4GB"
  DUCKDB_THREADS: "4"
  RAY_NUM_CPUS: "4"
  LOG_LEVEL: "INFO"
```

### Head Service

```yaml
# head-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: daft-head
  namespace: daft-duckdb
spec:
  ports:
  - port: 6379
    name: gcs
  - port: 8265
    name: dashboard
  selector:
    app: daft-head
```

### Head Deployment

```yaml
# head-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: daft-head
  namespace: daft-duckdb
spec:
  replicas: 1
  selector:
    matchLabels:
      app: daft-head
  template:
    metadata:
      labels:
        app: daft-head
    spec:
      containers:
      - name: daft-head
        image: daft-duckdb:latest
        ports:
        - containerPort: 6379
        - containerPort: 8265
        envFrom:
        - configMapRef:
            name: daft-config
        command: ["ray", "start", "--head", "--port=6379", "--dashboard-host=0.0.0.0"]
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
```

### Worker Deployment

```yaml
# worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: daft-worker
  namespace: daft-duckdb
spec:
  replicas: 3
  selector:
    matchLabels:
      app: daft-worker
  template:
    metadata:
      labels:
        app: daft-worker
    spec:
      containers:
      - name: daft-worker
        image: daft-duckdb:latest
        env:
        - name: RAY_HEAD_ADDRESS
          value: "daft-head:6379"
        envFrom:
        - configMapRef:
            name: daft-config
        command: ["ray", "start", "--address=$(RAY_HEAD_ADDRESS)"]
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
```

## 性能优化

### 1. DuckDB 配置优化

```python
import duckdb

con = duckdb.connect()

# 内存设置
con.execute("SET memory_limit='4GB'")
con.execute("SET max_memory='8GB'")

# 并行设置
con.execute("SET threads=4")

# Temp 目录
con.execute("SET temp_directory='/tmp/duckdb_temp'")
```

### 2. Ray 配置优化

```python
import ray

# 初始化配置
ray.init(
    num_cpus=4,
    num_gpus=0,
    memory=8_000_000_000,  # 8GB
    object_store_memory=2_000_000_000,  # 2GB
    _system_config={
        "max_task_retry": 3,
        "task_retry_delay_ms": 1000,
    }
)
```

### 3. AI 扩展优化

- 使用批处理 API 而非单次调用
- 调整批大小（推荐 10-50）
- 启用连接池
- 配置合理的超时时间

### 4. 数据本地化

```bash
# 将数据缓存到本地存储
rsync -avz s3://bucket/data/ /local/cache/data/
```

## 监控和日志

### 日志收集

```bash
# 收集日志到集中式存储
mkdir -p /var/log/daft
tail -f /var/log/daft/*.log | jq '.'
```

### 指标导出

```python
from monitoring.metrics import get_collector

metrics = get_collector()
# 定期导出指标
metrics.export_json("/var/log/daft/metrics.json")
```

## 故障排查

### 服务启动失败

1. 检查端口占用：`netstat -tulpn | grep 8265`
2. 检查日志文件：`tail -f /var/log/daft/err.log`
3. 验证配置文件语法

### 性能问题

1. 检查 CPU 使用：`top -p $(pgrep -f daft)`
2. 检查内存使用：`free -h`
3. 查看指标：`cat /var/log/daft/metrics.json`

### 扩展加载失败

1. 验证扩展路径
2. 检查 DuckDB 版本兼容性
3. 查看错误日志

## 参考资源

- [DuckDB 文档](https://duckdb.org/docs/)
- [Ray 文档](https://docs.ray.io/)
- [Daft 文档](https://docs.daft.ai/)
- [Kubernetes 文档](https://kubernetes.io/docs/)
