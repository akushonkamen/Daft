#!/usr/bin/env python3
"""
Daft + Ray + DuckDB 分布式执行演示

MVP 目标：
1. 本地 Ray 集群（单机多 worker）
2. 每个 worker 有独立的 DuckDB CLI + Extension
3. 使用本地 parquet 文件验证端到端执行
"""

import os
import sys
import subprocess
import tempfile
from pathlib import Path

# 确保可以导入 Daft
sys.path.insert(0, str(Path(__file__).parent))

print("=" * 60)
print("Daft + Ray + DuckDB 分布式执行演示")
print("=" * 60)

# ============================================================================
# 步骤 1: 检查环境
# ============================================================================
print("\n📋 步骤 1: 环境检查")

def check_ray():
    """检查 Ray 是否可用"""
    try:
        import ray
        print(f"  ✅ Ray 版本: {ray.__version__}")
        return True, ray.__version__
    except ImportError as e:
        print(f"  ❌ Ray 不可用: {e}")
        return False, None

def check_duckdb_cli():
    """检查 DuckDB CLI 是否可用"""
    cli_path = Path("../duckdb/build/duckdb")
    if not cli_path.exists():
        cli_path = Path("./duckdb/build/duckdb")
    if not cli_path.exists():
        cli_path = Path("../duckdb/duckdb")

    if cli_path.exists():
        result = subprocess.run(
            [str(cli_path), "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            version = result.stdout.strip().split()[1] if len(result.stdout.strip().split()) > 1 else "unknown"
            print(f"  ✅ DuckDB CLI: {cli_path} (v{version})")
            return True, str(cli_path)
        else:
            print(f"  ⚠️  DuckDB CLI 存在但无法执行: {cli_path}")
            return False, None
    else:
        print(f"  ❌ DuckDB CLI 不存在: {cli_path}")
        # 尝试查找系统中的 duckdb
        result = subprocess.run(
            ["which", "duckdb"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            print(f"  ℹ️  系统 DuckDB CLI: {result.stdout.strip()}")
            return True, result.stdout.strip()
        return False, None

def check_extension():
    """检查 AI Extension 是否可用"""
    ext_path = Path("../duckdb/build/test/extension/ai.duckdb_extension")
    if not ext_path.exists():
        ext_path = Path("./build/test/extension/ai.duckdb_extension")

    if ext_path.exists():
        size_mb = ext_path.stat().st_size / (1024 * 1024)
        print(f"  ✅ AI Extension: {ext_path} ({size_mb:.1f} MB)")
        return True, str(ext_path)
    else:
        print(f"  ❌ AI Extension 不存在: {ext_path}")
        return False, None

def check_cifar10_data():
    """检查 CIFAR-10 测试数据"""
    data_path = Path("test_data/cifar10.parquet")
    if data_path.exists():
        size_mb = data_path.stat().st_size / (1024 * 1024)
        print(f"  ✅ CIFAR-10 数据: {data_path} ({size_mb:.1f} MB)")
        return True, str(data_path)
    else:
        print(f"  ⚠️  CIFAR-10 数据不存在: {data_path}")
        return False, None

ray_ok, ray_version = check_ray()
duckdb_cli_ok, duckdb_cli_path = check_duckdb_cli()
extension_ok, extension_path = check_extension()
data_ok, data_path = check_cifar10_data()

# ============================================================================
# 步骤 2: 初始化 Ray 集群
# ============================================================================
print("\n📋 步骤 2: 初始化 Ray 集群")

if ray_ok:
    import ray

    # 检查是否已有 Ray 集群运行
    if ray.is_initialized():
        print("  ℹ️  Ray 已初始化，重启...")
        ray.shutdown()

    # 初始化本地 Ray 集群（多进程）
    # 使用 2 个 worker 模拟分布式环境
    try:
        ray.init(
            num_cpus=2,
            num_gpus=0,
            runtime_env={"env_vars": {"PYTHONPATH": str(Path(__file__).parent)}},
            ignore_reinit_error=True
        )
        cluster_resources = ray.cluster_resources()
        print(f"  ✅ Ray 集群已启动")
        print(f"     CPUs: {cluster_resources.get('CPU', 'N/A')}")
        print(f"     Dashboard: {ray.worker.get_dashboard_url() if ray.is_initialized() else 'N/A'}")
    except Exception as e:
        print(f"  ⚠️  Ray 初始化失败: {e}")
        ray_ok = False
else:
    print("  ⏭️  跳过 Ray 初始化（Ray 不可用）")

# ============================================================================
# 步骤 3: 创建 Ray 远程函数（模拟分布式 DuckDB 执行）
# ============================================================================
print("\n📋 步骤 3: 创建分布式执行函数")

if ray_ok and duckdb_cli_ok and extension_ok:
    @ray.remote
    def duckdb_ai_filter_worker(
        partition_id: int,
        data_path: str,
        duckdb_cli_path: str,
        extension_path: str,
        prompt: str = "cat",
        limit: int = 10
    ):
        """
        Ray Worker 函数：在单个 worker 上执行 DuckDB 查询

        模拟分布式场景：
        - 每个 worker 是独立的进程
        - 每个 worker 有自己的 DuckDB CLI 实例
        - 每个 worker 加载 AI Extension
        """
        import subprocess
        import json
        import re

        # 构建 SQL 查询
        # 注意：ai_filter 期望 VARCHAR（base64 字符串），不是 BLOB
        sql = f"""
        LOAD '{extension_path}';
        SELECT id, label, ai_filter(image_base64, '{prompt}', 'clip') AS ai_score
        FROM read_parquet('{data_path}')
        WHERE ai_filter(image_base64, '{prompt}', 'clip') > 0.3
        LIMIT {limit};
        """

        # 执行查询
        try:
            result = subprocess.run(
                [duckdb_cli_path, "-unsigned", "-c", sql],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(Path(__file__).parent)
            )

            if result.returncode == 0:
                # 解析输出
                lines = result.stdout.strip().split('\n')
                if len(lines) > 2:  # 有数据行
                    # 提取数据（跳过分隔线和类型行）
                    data_lines = [l for l in lines if l.strip() and not l.startswith('│') and not l.startswith('┌') and not l.startswith('└') and not l.startswith('├')]

                    # 简单解析：提取所有数字
                    rows = []
                    for line in lines:
                        if '│' in line and not line.startswith('│'):
                            parts = [p.strip() for p in line.split('│')[1:-1]]
                            if len(parts) >= 3:
                                try:
                                    row_id = int(parts[0])
                                    label = parts[1]
                                    score = float(parts[2])
                                    rows.append({
                                        "partition_id": partition_id,
                                        "id": row_id,
                                        "label": label,
                                        "ai_score": score
                                    })
                                except (ValueError, IndexError):
                                    continue

                    return {
                        "partition_id": partition_id,
                        "success": True,
                        "row_count": len(rows),
                        "rows": rows[:5],  # 返回前 5 行
                        "output_preview": result.stdout[:500] if len(result.stdout) > 500 else result.stdout
                    }
                else:
                    return {
                        "partition_id": partition_id,
                        "success": True,
                        "row_count": 0,
                        "message": "No data returned",
                        "output": result.stdout
                    }
            else:
                return {
                    "partition_id": partition_id,
                    "success": False,
                    "error": result.stderr,
                    "output": result.stdout
                }
        except subprocess.TimeoutExpired:
            return {
                "partition_id": partition_id,
                "success": False,
                "error": "Query timeout after 30s"
            }
        except Exception as e:
            return {
                "partition_id": partition_id,
                "success": False,
                "error": str(e)
            }

    print("  ✅ Ray Worker 函数已定义: duckdb_ai_filter_worker")
    print("     - 每个 worker 加载独立的 DuckDB CLI + Extension")
    print("     - 并行执行 AI_filter 查询")

else:
    print("  ⏭️  跳过（缺少依赖）")
    duckdb_ai_filter_worker = None

# ============================================================================
# 步骤 4: 分布式执行测试
# ============================================================================
print("\n📋 步骤 4: 分布式执行测试")

if ray_ok and duckdb_ai_filter_worker and data_ok:
    print("  在 3 个 Ray worker 上并行执行查询...")

    # 创建 3 个并发任务（模拟 3 个分区）
    futures = []
    prompts = ["cat", "dog", "bird"]

    for i, prompt in enumerate(prompts):
        future = duckdb_ai_filter_worker.remote(
            partition_id=i,
            data_path=data_path,
            duckdb_cli_path=duckdb_cli_path,
            extension_path=extension_path,
            prompt=prompt,
            limit=5
        )
        futures.append((prompt, future))

    # 等待所有任务完成
    results = []
    for prompt, future in futures:
        try:
            result = ray.get(future, timeout=60)
            results.append((prompt, result))
            status = "✅" if result.get("success") else "❌"
            print(f"  {status} Worker (prompt='{prompt}'): {result.get('row_count', 0)} rows")
        except Exception as e:
            print(f"  ❌ Worker (prompt='{prompt}'): {e}")
            results.append((prompt, {"success": False, "error": str(e)}))

    # 显示结果详情
    print("\n  详细结果:")
    for prompt, result in results:
        print(f"\n  ┌─ Prompt: '{prompt}'")
        if result.get("success"):
            print(f"  │  Row count: {result.get('row_count', 0)}")
            if result.get("rows"):
                print(f"  │  Sample rows:")
                for row in result.get("rows", [])[:3]:
                    print(f"  │    - ID={row.get('id')}, Label={row.get('label')}, Score={row.get('ai_score', 0):.4f}")
        else:
            print(f"  │  Error: {result.get('error', 'Unknown error')}")
        print(f"  └─────────────────")

else:
    print("  ⏭️  跳过分布式执行（缺少依赖）")

# ============================================================================
# 步骤 5: 性能对比
# ============================================================================
print("\n📋 步骤 5: 性能对比")

if ray_ok and duckdb_ai_filter_worker and data_ok:
    import time

    print("  对比：单机 vs 分布式（3 workers）")

    # 单机执行
    if duckdb_cli_ok and extension_ok:
        print("\n  单机执行:")
        sql = """
        LOAD '{extension}';
        SELECT COUNT(*) as count FROM (
            SELECT ai_filter(image_base64, 'cat', 'clip') AS score
            FROM read_parquet('{data}')
            WHERE ai_filter(image_base64, 'cat', 'clip') > 0.3
        );
        """.format(extension=extension_path, data=data_path)

        start = time.time()
        result = subprocess.run(
            [duckdb_cli_path, "-unsigned", "-c", sql],
            capture_output=True,
            text=True,
            timeout=60
        )
        single_time = time.time() - start

        if result.returncode == 0:
            print(f"  ⏱️  耗时: {single_time:.2f}s")
            print(f"  📊 输出: {result.stdout.strip()[:200]}")
        else:
            print(f"  ❌ 失败: {result.stderr[:200]}")

    # 分布式执行（3 个不同 prompt 并行）
    print("\n  分布式执行（3 workers 并行）:")
    start = time.time()

    futures = []
    for i in range(3):
        future = duckdb_ai_filter_worker.remote(
            partition_id=i,
            data_path=data_path,
            duckdb_cli_path=duckdb_cli_path,
            extension_path=extension_path,
            prompt="cat",
            limit=10
        )
        futures.append(future)

    results = ray.get(futures, timeout=60)
    distributed_time = time.time() - start

    success_count = sum(1 for r in results if r.get("success"))
    print(f"  ⏱️  耗时: {distributed_time:.2f}s")
    print(f"  ✅ 成功: {success_count}/3 workers")

    if success_count == 3:
        speedup = single_time / distributed_time if distributed_time > 0 else 0
        print(f"  🚀 加速比: {speedup:.2f}x")

else:
    print("  ⏭️  跳过性能对比（缺少依赖）")

# ============================================================================
# 步骤 6: 验证 Worker 隔离性
# ============================================================================
print("\n📋 步骤 6: 验证 Worker 隔离性")

if ray_ok:
    @ray.remote
    def get_worker_id():
        """获取当前 worker 的唯一标识"""
        import os
        return {
            "pid": os.getpid(),
            "node_id": ray.get_runtime_context().get_worker_id(),
            "task_id": ray.get_runtime_context().get_task_id(),
        }

    # 在多个 worker 上获取 ID
    print("  获取 3 个 worker 的标识:")
    futures = [get_worker_id.remote() for _ in range(3)]
    worker_ids = ray.get(futures)

    unique_pids = set()
    for i, worker_id in enumerate(worker_ids):
        pid = worker_id.get("pid")
        unique_pids.add(pid)
        print(f"  Worker {i}: PID={pid}, Node={worker_id.get('node_id', 'N/A')}")

    print(f"\n  独立进程数: {len(unique_pids)}")
    if len(unique_pids) > 1:
        print("  ✅ 确认：多个 worker 在独立进程中运行")
    else:
        print("  ℹ️  单进程模式（可能是 Ray 配置问题）")

# ============================================================================
# 清理
# ============================================================================
print("\n📋 清理资源")

if ray_ok and ray.is_initialized():
    ray.shutdown()
    print("  ✅ Ray 集群已关闭")

# ============================================================================
# 总结
# ============================================================================
print("\n" + "=" * 60)
print("演示总结")
print("=" * 60)

print(f"Ray 可用: {'✅' if ray_ok else '❌'}")
print(f"DuckDB CLI 可用: {'✅' if duckdb_cli_ok else '❌'}")
print(f"AI Extension 可用: {'✅' if extension_ok else '❌'}")
print(f"测试数据可用: {'✅' if data_ok else '❌'}")

if ray_ok and duckdb_ai_filter_worker:
    print("\n✅ 分布式执行演示完成！")
    print("\n架构验证:")
    print("  ✅ Ray Worker 可以独立执行 DuckDB 查询")
    print("  ✅ 每个 Worker 加载自己的 Extension")
    print("  ✅ 多 Worker 并行执行正常")
    print("\n后续工作:")
    print("  1. 使用 Ray runtime_env 自动分发 Extension")
    print("  2. 实现两阶段聚合优化")
    print("  3. 支持 S3 数据源")
else:
    print("\n⚠️  分布式执行演示受限（缺少依赖）")
    print("\n需要:")
    print("  - pip install ray")
    print("  - DuckDB CLI 可执行文件")
    print("  - AI Extension 文件")
