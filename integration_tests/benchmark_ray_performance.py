#!/usr/bin/env python3
"""
Ray + DuckDB 性能基准测试（CI 友好版，带监控和日志）

特点：
- 使用 mock 数据，不依赖外部 AI API
- 快速执行，适合 CI 环境
- 测量 Ray 单进程 vs 多进程性能差异
- 结构化 JSON 日志输出
- 性能指标收集和报告
"""

import os
import sys
import time
import logging
import subprocess
from pathlib import Path

# 添加 monitoring 模块路径
sys.path.insert(0, str(Path(__file__).parent.parent))
from monitoring import get_logger, get_collector

print("=" * 60)
print("Ray + DuckDB 性能基准测试（带监控）")
print("=" * 60)

# ============================================================================
# 配置
# ============================================================================
# CI 模式：快速测试模式（小数据集）
CI_MODE = os.getenv("CI", "false").lower() == "true"

# 日志配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = get_logger("benchmark", level=LOG_LEVEL)

# DuckDB 路径
DUCKDB_CLI = Path(__file__).parent.parent.parent / "duckdb/build/duckdb"
AI_EXTENSION = Path(__file__).parent.parent.parent / "duckdb/build/test/extension/ai.duckdb_extension"

# 测试配置
TEST_SIZES = [10, 50] if CI_MODE else [10, 50, 100]
RAY_CPUS = 1 if CI_MODE else 2

# 指标收集器
metrics = get_collector()

logger.info("Benchmark initialized",
           ci_mode=CI_MODE,
           test_sizes=TEST_SIZES,
           ray_cpus=RAY_CPUS)

# ============================================================================
# 环境检查
# ============================================================================
print("\n📋 环境检查")

def check_ray():
    try:
        import ray
        print(f"  ✅ Ray {ray.__version__}")
        return True
    except ImportError:
        print("  ⚠️  Ray 不可用，跳过 Ray 测试")
        return False

def check_duckdb():
    if DUCKDB_CLI.exists():
        print(f"  ✅ DuckDB CLI")
        return True
    else:
        print("  ❌ DuckDB CLI 不存在")
        return False

ray_ok = check_ray()
duckdb_ok = check_duckdb()

if not duckdb_ok:
    logger.error("Environment check failed")
    print("\n❌ 环境检查失败")
    sys.exit(1)

logger.info("Environment check passed",
           ray_available=ray_ok,
           duckdb_available=duckdb_ok)

# ============================================================================
# DuckDB CLI 直接执行基准
# ============================================================================
print("\n📊 基准 1: DuckDB CLI 直接执行")

def benchmark_duckdb_cli(rows: int) -> float:
    """测量 DuckDB CLI 执行时间"""
    sql = f"SELECT ai_filter_batch('test', 'cat', 'clip') FROM range({rows});"

    cmd = [
        str(DUCKDB_CLI),
        "-unsigned",
        "-c", f"LOAD '{AI_EXTENSION}';",
        "-c", sql
    ]

    logger.info("Starting DuckDB CLI benchmark", rows=rows)

    timer_id = metrics.start_timer("duckdb_cli_query")

    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    elapsed = time.time() - start

    metrics.stop_timer(timer_id, "duckdb_cli_query", rows=rows)

    if result.returncode != 0:
        logger.error("DuckDB CLI execution failed",
                    error=result.stderr.strip()[:100],
                    rows=rows)
        metrics.increment_counter("duckdb_cli_errors", rows=rows)
        return -1

    metrics.increment_counter("duckdb_cli_success", rows=rows)
    logger.info("DuckDB CLI benchmark completed",
               rows=rows,
               elapsed_sec=elapsed)

    return elapsed

# 执行基准测试
print("  行数  |  时间 (s)  |  行/秒   ")
print("-------|------------|---------")

baseline_results = {}
for size in TEST_SIZES:
    elapsed = benchmark_duckdb_cli(size)
    if elapsed > 0:
        rps = size / elapsed
        print(f"  {size:4d}  |  {elapsed:.4f}    |  {rps:7.1f}")
        baseline_results[size] = elapsed

        metrics.record_metric("duckdb_cli_rps", rps, "rows/s", size=size)
    else:
        print(f"  {size:4d}  |  FAILED    |    -")

# ============================================================================
# Ray 分布式执行基准
# ============================================================================
if ray_ok:
    print("\n📊 基准 2: Ray 分布式执行")

    import ray

    # 初始化 Ray
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=RAY_CPUS, ignore_reinit_error=True, logging_level=logging.ERROR)

    @ray.remote
    def run_duckdb_query(rows: int) -> dict:
        """Ray worker: 执行 DuckDB 查询"""
        sql = f"SELECT ai_filter_batch('test', 'cat', 'clip') FROM range({rows});"
        cmd = [
            str(DUCKDB_CLI),
            "-unsigned",
            "-c", f"LOAD '{AI_EXTENSION}';",
            "-c", sql
        ]

        start = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        elapsed = time.time() - start

        return {
            "rows": rows,
            "elapsed": elapsed,
            "success": result.returncode == 0
        }

    def benchmark_ray_distributed(total_rows: int, batch_size: int) -> float:
        """测量 Ray 分布式执行时间"""
        batches = (total_rows + batch_size - 1) // batch_size
        futures = [
            run_duckdb_query.remote(min(batch_size, total_rows - i * batch_size))
            for i in range(batches)
        ]

        logger.info("Starting Ray distributed benchmark",
                   total_rows=total_rows,
                   batches=batches,
                   batch_size=batch_size)

        timer_id = metrics.start_timer("ray_distributed_total")

        start = time.time()
        results = ray.get(futures)
        elapsed = time.time() - start

        metrics.stop_timer(timer_id, "ray_distributed_total", total_rows=total_rows)

        success_count = sum(1 for r in results if r["success"])
        if success_count != len(futures):
            logger.warning("Some Ray tasks failed",
                           success=success_count,
                           total=len(futures))
            metrics.increment_counter("ray_errors", delta=len(futures)-success_count)

        metrics.increment_counter("ray_success", success_count)

        logger.info("Ray distributed benchmark completed",
                   total_rows=total_rows,
                   batches=batches,
                   success_count=success_count,
                   elapsed_sec=elapsed)

        return elapsed

    print(f"  配置: {RAY_CPUS} CPU, 批处理")
    print("  总行数 | 批次 |  时间 (s)  |  行/秒   ")
    print("--------|------|------------|---------")

    for size in TEST_SIZES:
        batch_size = max(10, size // 2)
        elapsed = benchmark_ray_distributed(size, batch_size)
        rps = size / elapsed
        print(f"   {size:4d}  |  {batch_size:3d} |  {elapsed:.4f}    |  {rps:7.1f}")

        metrics.record_metric("ray_distributed_rps", rps, "rows/s", size=size)

    ray.shutdown()

# ============================================================================
# 性能总结和报告
# ============================================================================
print("\n📈 性能总结")

if baseline_results:
    print("\n  DuckDB CLI 单机性能:")
    for size, elapsed in baseline_results.items():
        rps = size / elapsed
        print(f"    {size:4d} 行: {rps:7.1f} 行/秒")

# 导出指标报告
print("\n💾 导出指标报告...")
try:
    metrics.export_json("benchmark_metrics.json")
    logger.info("Metrics exported", file="benchmark_metrics.json")
    print("  ✅ 指标已保存到: benchmark_metrics.json")
except Exception as e:
    logger.error("Failed to export metrics", error=str(e))
    print(f"  ⚠️  导出失败: {e}")

# 打印指标摘要
metrics.print_summary()

print("\n" + "=" * 60)
print("✅ 基准测试完成（带监控和日志）")
print("=" * 60)

# CI 环境输出简洁结果
if CI_MODE:
    if baseline_results:
        avg_rps = sum(size/elapsed for size, elapsed in baseline_results.items()) / len(baseline_results)
        print(f"\n:: CI 输出: 平均 {avg_rps:.1f} 行/秒")

        logger.info("CI test completed", avg_rows_per_sec=avg_rps)
