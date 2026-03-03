#!/usr/bin/env python3
"""
Performance Benchmark for ai_similarity Function.

This script benchmarks the ai_similarity UDF performance across:
1. Different data sizes (10, 100, 1K, 10K, 100K rows)
2. Different vector dimensions (128, 384, 768, 1536)
3. Different similarity models (cosine, dot, euclidean)
4. Different operations (filter, join, aggregate)

Requirements:
- DuckDB with ai_similarity Extension
"""

import json
import subprocess
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DUCKDB_CLI = PROJECT_ROOT / "duckdb" / "build" / "duckdb"
EXTENSION = PROJECT_ROOT / "duckdb" / "build" / "test" / "extension" / "ai.duckdb_extension"

# Benchmark configuration
DATA_SIZES = [10, 100, 1000, 10000]  # rows
VECTOR_DIMENSIONS = [128, 384, 768]  # typical embedding sizes
MODELS = ["cosine", "dot", "euclidean"]


def run_duckdb_query(sql: str) -> tuple[bool, str, float]:
    """Run a DuckDB query and return success status, output, and execution time."""
    try:
        start_time = time.time()
        result = subprocess.run(
            [str(DUCKDB_CLI), "-unsigned"],
            input=f"LOAD '{EXTENSION}';\n{sql}",
            capture_output=True,
            text=True,
            timeout=60
        )
        elapsed = time.time() - start_time

        if result.returncode == 0:
            return True, result.stdout, elapsed
        else:
            return False, result.stderr, elapsed
    except subprocess.TimeoutExpired:
        return False, "Query timeout (>60s)", 60.0
    except Exception as e:
        return False, str(e), 0.0


def benchmark_filter(data_size: int, vector_dim: int, model: str) -> Dict[str, Any]:
    """Benchmark ai_similarity in WHERE clause filtering."""
    print(f"  Testing: filter | size={data_size} | dim={vector_dim} | model={model}")

    # Generate test data
    vec1 = "[" + ", ".join(["0.1"] * vector_dim) + "]"
    vec2 = "[" + ", ".join(["0.2"] * vector_dim) + "]"

    sql = f"""
    CREATE TEMP TABLE bench_{data_size}_{vector_dim} AS
    SELECT * FROM (
        SELECT UNNEST(GENERATE_SERIES(1, {data_size})) AS id,
               '{vec1}'::FLOAT[] AS vec1,
               '{vec2}'::FLOAT[] AS vec2
    );

    SELECT COUNT(*) AS count
    FROM bench_{data_size}_{vector_dim}
    WHERE ai_similarity(vec1, vec2, '{model}') > 0.5;
    """

    success, output, elapsed = run_duckdb_query(sql)

    return {
        "operation": "filter",
        "data_size": data_size,
        "vector_dim": vector_dim,
        "model": model,
        "success": success,
        "time_seconds": elapsed,
        "rows_per_second": data_size / elapsed if elapsed > 0 else 0,
        "output": output[:200] if success else output
    }


def benchmark_join(data_size: int, vector_dim: int, model: str) -> Dict[str, Any]:
    """Benchmark ai_similarity in JOIN operations."""
    print(f"  Testing: join | size={data_size} | dim={vector_dim} | model={model}")

    vec1 = "[" + ", ".join(["0.1"] * vector_dim) + "]"
    vec2 = "[" + ", ".join(["0.2"] * vector_dim) + "]"

    sql = f"""
    CREATE TEMP TABLE left_{data_size}_{vector_dim} AS
    SELECT * FROM (
        SELECT UNNEST(GENERATE_SERIES(1, {data_size})) AS id,
               '{vec1}'::FLOAT[] AS embedding
    );

    CREATE TEMP TABLE right_{data_size}_{vector_dim} AS
    SELECT * FROM (
        SELECT UNNEST(GENERATE_SERIES(1, {data_size})) AS id,
               '{vec2}'::FLOAT[] AS embedding
    );

    SELECT COUNT(*) AS match_count
    FROM left_{data_size}_{vector_dim} l
    JOIN right_{data_size}_{vector_dim} r
    ON ai_similarity(l.embedding, r.embedding, '{model}') > 0.5;
    """

    success, output, elapsed = run_duckdb_query(sql)

    return {
        "operation": "join",
        "data_size": data_size,
        "vector_dim": vector_dim,
        "model": model,
        "success": success,
        "time_seconds": elapsed,
        "rows_per_second": data_size / elapsed if elapsed > 0 else 0,
        "output": output[:200] if success else output
    }


def benchmark_aggregate(data_size: int, vector_dim: int, model: str) -> Dict[str, Any]:
    """Benchmark ai_similarity in aggregation."""
    print(f"  Testing: aggregate | size={data_size} | dim={vector_dim} | model={model}")

    vec1 = "[" + ", ".join(["0.1"] * vector_dim) + "]"
    vec2 = "[" + ", ".join(["0.2"] * vector_dim) + "]"

    sql = f"""
    CREATE TEMP TABLE agg_{data_size}_{vector_dim} AS
    SELECT * FROM (
        SELECT UNNEST(GENERATE_SERIES(1, {data_size})) AS id,
               '{vec1}'::FLOAT[] AS vec1,
               '{vec2}'::FLOAT[] AS vec2
    );

    SELECT
        AVG(ai_similarity(vec1, vec2, '{model}')) AS avg_sim,
        MIN(ai_similarity(vec1, vec2, '{model}')) AS min_sim,
        MAX(ai_similarity(vec1, vec2, '{model}')) AS max_sim
    FROM agg_{data_size}_{vector_dim};
    """

    success, output, elapsed = run_duckdb_query(sql)

    return {
        "operation": "aggregate",
        "data_size": data_size,
        "vector_dim": vector_dim,
        "model": model,
        "success": success,
        "time_seconds": elapsed,
        "rows_per_second": data_size / elapsed if elapsed > 0 else 0,
        "output": output[:200] if success else output
    }


def run_benchmarks() -> List[Dict[str, Any]]:
    """Run all benchmark combinations."""
    print("=" * 70)
    print("ai_similarity Performance Benchmark")
    print("=" * 70)
    print()

    results = []

    # Run benchmarks for each combination
    for size in DATA_SIZES:
        for dim in VECTOR_DIMENSIONS:
            for model in MODELS:
                # Filter benchmark
                result = benchmark_filter(size, dim, model)
                results.append(result)
                time.sleep(0.5)  # Brief pause between queries

                # Join benchmark (only for smaller sizes due to O(n*m) complexity)
                if size <= 1000:
                    result = benchmark_join(size, dim, model)
                    results.append(result)
                    time.sleep(0.5)

                # Aggregate benchmark
                result = benchmark_aggregate(size, dim, model)
                results.append(result)
                time.sleep(0.5)

    return results


def analyze_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze benchmark results and generate summary statistics."""
    print("\n" + "=" * 70)
    print("Benchmark Analysis")
    print("=" * 70)

    analysis = {
        "timestamp": datetime.now().isoformat(),
        "total_tests": len(results),
        "successful": sum(1 for r in results if r["success"]),
        "failed": sum(1 for r in results if not r["success"]),
        "by_operation": {},
        "by_model": {},
        "performance_summary": {}
    }

    # Group by operation
    for op in ["filter", "join", "aggregate"]:
        op_results = [r for r in results if r["operation"] == op and r["success"]]
        if op_results:
            times = [r["time_seconds"] for r in op_results]
            throughputs = [r["rows_per_second"] for r in op_results]

            analysis["by_operation"][op] = {
                "count": len(op_results),
                "avg_time": sum(times) / len(times),
                "min_time": min(times),
                "max_time": max(times),
                "avg_throughput": sum(throughputs) / len(throughputs)
            }

    # Group by model
    for model in MODELS:
        model_results = [r for r in results if r["model"] == model and r["success"]]
        if model_results:
            times = [r["time_seconds"] for r in model_results]
            analysis["by_model"][model] = {
                "avg_time": sum(times) / len(times),
                "min_time": min(times),
                "max_time": max(times)
            }

    # Performance summary by data size
    for size in DATA_SIZES:
        size_results = [r for r in results if r["data_size"] == size and r["success"] and r["operation"] == "filter"]
        if size_results:
            throughputs = [r["rows_per_second"] for r in size_results]
            analysis["performance_summary"][f"{size}_rows"] = {
                "avg_throughput": sum(throughputs) / len(throughputs),
                "max_throughput": max(throughputs)
            }

    return analysis


def print_summary(analysis: Dict[str, Any]):
    """Print benchmark summary."""
    print(f"\nTotal Tests: {analysis['total_tests']}")
    print(f"Successful: {analysis['successful']}")
    print(f"Failed: {analysis['failed']}")

    print("\n--- Performance by Operation ---")
    for op, stats in analysis["by_operation"].items():
        print(f"\n{op.upper()}:")
        print(f"  Tests: {stats['count']}")
        print(f"  Avg Time: {stats['avg_time']:.4f}s")
        print(f"  Avg Throughput: {stats['avg_throughput']:.0f} rows/s")

    print("\n--- Performance by Model ---")
    for model, stats in analysis["by_model"].items():
        print(f"\n{model.upper()}:")
        print(f"  Avg Time: {stats['avg_time']:.4f}s")

    print("\n--- Scalability (Filter Operation) ---")
    for size_key, stats in analysis["performance_summary"].items():
        print(f"{size_key}: {stats['avg_throughput']:.0f} rows/s")


def save_results(results: List[Dict[str, Any]], analysis: Dict[str, Any]):
    """Save benchmark results to JSON file."""
    output_file = Path(__file__).parent / "benchmark_ai_similarity_results.json"

    output = {
        "analysis": analysis,
        "raw_results": results
    }

    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Results saved to: {output_file}")


def main():
    """Run the complete benchmark suite."""
    print("Starting ai_similarity performance benchmarks...")
    print(f"DuckDB CLI: {DUCKDB_CLI}")
    print(f"Extension: {EXTENSION}")
    print()

    # Check prerequisites
    if not DUCKDB_CLI.exists():
        print(f"❌ DuckDB CLI not found: {DUCKDB_CLI}")
        return 1

    if not EXTENSION.exists():
        print(f"❌ Extension not found: {EXTENSION}")
        return 1

    # Run benchmarks
    results = run_benchmarks()

    # Analyze results
    analysis = analyze_results(results)

    # Print summary
    print_summary(analysis)

    # Save results
    save_results(results, analysis)

    print("\n" + "=" * 70)
    print("✅ Benchmark Complete!")
    print("=" * 70)

    return 0 if analysis["failed"] == 0 else 1


if __name__ == "__main__":
    exit(main())
