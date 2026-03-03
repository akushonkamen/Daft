#!/usr/bin/env python3
"""
Performance Regression Test - TASK-TEST-001

Tests for detecting performance regressions:
1. Baseline performance comparison
2. Throughput measurement over time
3. Latency percentiles tracking
4. Memory usage monitoring
5. Scaling behavior verification

CI-friendly: Stores baseline data for comparison in subsequent runs.
"""

import os
import sys
import time
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, List
from statistics import mean, median, stdev

# Add project paths
sys.path.insert(0, str(Path(__file__).parent.parent))

# Test configuration
CI_MODE = os.getenv("CI", "false").lower() == "true"
DUCKDB_CLI = Path(__file__).parent.parent.parent / "duckdb/build/duckdb"
AI_EXTENSION = Path(__file__).parent.parent.parent / "duckdb/build/test/extension/ai.duckdb_extension"

# Baseline file for regression detection
BASELINE_FILE = Path(__file__).parent / "baseline_performance.json"
CURRENT_RESULTS_FILE = Path(__file__).parent / "current_performance.json"
REGRESSION_THRESHOLD = 0.20  # 20% degradation threshold

# Test configurations
TEST_SIZES = [10, 50] if CI_MODE else [10, 50, 100, 200]
ITERATIONS = 3 if CI_MODE else 5

# ============================================================================
# Test Utilities
# ============================================================================

def run_duckdb_query(sql: str) -> Dict[str, Any]:
    """Execute DuckDB query and measure performance."""
    cmd = [
        str(DUCKDB_CLI),
        "-unsigned",
        "-c", f"LOAD '{AI_EXTENSION}';",
        "-c", sql
    ]

    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    elapsed = time.time() - start

    return {
        "success": result.returncode == 0,
        "elapsed": elapsed,
        "stdout": result.stdout,
        "stderr": result.stderr
    }


def load_baseline() -> Dict[str, Any]:
    """Load baseline performance data."""
    if BASELINE_FILE.exists():
        with open(BASELINE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_baseline(data: Dict[str, Any]):
    """Save current performance as new baseline."""
    with open(BASELINE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def save_current_results(data: Dict[str, Any]):
    """Save current test results."""
    with open(CURRENT_RESULTS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def calculate_stats(values: List[float]) -> Dict[str, float]:
    """Calculate statistics for a list of values."""
    if len(values) < 2:
        return {
            "mean": values[0] if values else 0,
            "median": values[0] if values else 0,
            "min": values[0] if values else 0,
            "max": values[0] if values else 0,
            "stdev": 0
        }
    return {
        "mean": mean(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
        "stdev": stdev(values) if len(values) > 1 else 0
    }


def compare_with_baseline(current: float, baseline: float, metric: str) -> Dict[str, Any]:
    """Compare current performance with baseline."""
    if baseline == 0:
        return {"regression": False, "improvement": False, "ratio": 0}

    ratio = current / baseline
    regression = ratio > (1 + REGRESSION_THRESHOLD)
    improvement = ratio < (1 - REGRESSION_THRESHOLD)

    return {
        "regression": regression,
        "improvement": improvement,
        "ratio": ratio,
        "percent_change": (ratio - 1) * 100
    }


# ============================================================================
# Performance Tests
# ============================================================================

def test_1_single_query_latency():
    """Test 1: Measure single query latency."""
    print("\n📋 Test 1: Single Query Latency")
    print("-" * 70)

    latencies = []
    for i in range(ITERATIONS):
        result = run_duckdb_query("SELECT ai_filter_batch('test', 'cat', 'clip') AS score;")
        if result["success"]:
            latencies.append(result["elapsed"])

    stats = calculate_stats(latencies)
    print(f"  Mean: {stats['mean']*1000:.1f}ms")
    print(f"  Median: {stats['median']*1000:.1f}ms")
    print(f"  Min: {stats['min']*1000:.1f}ms")
    print(f"  Max: {stats['max']*1000:.1f}ms")
    print(f"  StdDev: {stats['stdev']*1000:.1f}ms")

    return {"single_query_latency": stats}


def test_2_throughput_by_batch_size():
    """Test 2: Measure throughput at different batch sizes."""
    print("\n📋 Test 2: Throughput by Batch Size")
    print("-" * 70)

    throughput_results = {}
    print(f"  {'Size':<6} | {'Time (s)':<10} | {'Rows/s':<10}")
    print("-" * 70)

    for size in TEST_SIZES:
        times = []
        for _ in range(max(2, ITERATIONS // 2)):  # Fewer iterations for larger sizes
            result = run_duckdb_query(f"SELECT ai_filter_batch('test', 'cat', 'clip') FROM range({size});")
            if result["success"]:
                times.append(result["elapsed"])

        if times:
            avg_time = mean(times)
            rps = size / avg_time
            print(f"  {size:<6} | {avg_time:<10.3f} | {rps:<10.1f}")
            throughput_results[size] = {
                "avg_time": avg_time,
                "throughput": rps,
                "iterations": len(times)
            }

    return {"throughput_by_batch_size": throughput_results}


def test_3_warm_up_effect():
    """Test 3: Measure warm-up effect on first queries."""
    print("\n📋 Test 3: Warm-up Effect")
    print("-" * 70)

    times = []
    for i in range(10):
        result = run_duckdb_query("SELECT ai_filter_batch('test', 'cat', 'clip') AS score;")
        if result["success"]:
            times.append(result["elapsed"])
            print(f"  Query {i+1}: {result['elapsed']*1000:.1f}ms")

    # Compare first 3 vs last 3
    if len(times) >= 6:
        first_3_avg = mean(times[:3])
        last_3_avg = mean(times[-3:])
        warm_up_factor = first_3_avg / last_3_avg if last_3_avg > 0 else 0

        print(f"\n  First 3 avg: {first_3_avg*1000:.1f}ms")
        print(f"  Last 3 avg: {last_3_avg*1000:.1f}ms")
        print(f"  Warm-up factor: {warm_up_factor:.2f}x")

        return {"warm_up_effect": {
            "first_3_avg_ms": first_3_avg * 1000,
            "last_3_avg_ms": last_3_avg * 1000,
            "warm_up_factor": warm_up_factor
        }}

    return {"warm_up_effect": {"note": "Not enough data points"}}


def test_4_memory_efficiency():
    """Test 4: Memory efficiency across batch sizes."""
    print("\n📋 Test 4: Memory Efficiency")
    print("-" * 70)

    # This is an indirect measure - we look at how time scales with size
    # If memory were leaking, time would increase super-linearly

    results = {}
    for size in [10, 50, 100]:
        result = run_duckdb_query(f"SELECT ai_filter_batch('test', 'cat', 'clip') FROM range({size});")
        if result["success"]:
            results[size] = result["elapsed"]

    if len(results) >= 2:
        # Check if scaling is reasonable (roughly linear)
        sizes = sorted(results.keys())
        times = [results[s] for s in sizes]

        # Calculate scaling factor
        if len(sizes) >= 2:
            scaling_factors = []
            for i in range(1, len(sizes)):
                size_ratio = sizes[i] / sizes[i-1]
                time_ratio = times[i] / times[i-1] if times[i-1] > 0 else 0
                scaling_factors.append(time_ratio / size_ratio if size_ratio > 0 else 0)

            avg_scaling = mean(scaling_factors) if scaling_factors else 0
            print(f"  Average scaling factor: {avg_scaling:.2f}")
            print("  (1.0 = linear, <1.0 = better than linear, >1.5 = concern)")

            if avg_scaling < 1.5:
                print("  ✅ Memory efficiency looks good")
                return {"memory_efficiency": {"status": "good", "scaling_factor": avg_scaling}}
            else:
                print("  ⚠️  Possible memory inefficiency")
                return {"memory_efficiency": {"status": "concern", "scaling_factor": avg_scaling}}

    return {"memory_efficiency": {"status": "unknown"}}


def test_5_concurrent_performance():
    """Test 5: Concurrent query performance."""
    print("\n📋 Test 5: Concurrent Query Performance")
    print("-" * 70)

    import concurrent.futures

    def run_query(idx: int) -> float:
        result = run_duckdb_query("SELECT ai_filter_batch('test', 'concurrent', 'clip') AS score;")
        return result["elapsed"]

    # Test with different concurrency levels
    concurrency_levels = [1, 2, 4] if CI_MODE else [1, 2, 4, 8]

    for concurrency in concurrency_levels:
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_query, i) for i in range(concurrency)]
            times = [f.result() for f in concurrent.futures.as_completed(futures)]
        total = time.time() - start

        avg_time = mean(times) if times else 0
        print(f"  Concurrency {concurrency}: {total:.3f}s total, {avg_time*1000:.1f}ms avg")

    return {"concurrent_performance": {"tested": concurrency_levels}}


# ============================================================================
# Regression Detection
# ============================================================================

def detect_regressions(current_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Detect performance regressions by comparing with baseline."""
    baseline = load_baseline()

    if not baseline:
        print("\n📋 No baseline found - saving current results as baseline")
        save_baseline(current_results)
        return []

    print("\n📋 Regression Detection")
    print("-" * 70)

    regressions = []

    # Compare throughput
    if "throughput_by_batch_size" in current_results and "throughput_by_batch_size" in baseline:
        print("  Throughput comparison:")
        for size in current_results["throughput_by_batch_size"]:
            if size in baseline["throughput_by_batch_size"]:
                current_rps = current_results["throughput_by_batch_size"][size]["throughput"]
                baseline_rps = baseline["throughput_by_batch_size"][size]["throughput"]

                comparison = compare_with_baseline(current_rps, baseline_rps, f"throughput_{size}")

                status = "✅"
                if comparison["regression"]:
                    status = "❌ REGRESSION"
                    regressions.append({
                        "metric": f"throughput_{size}",
                        "current": current_rps,
                        "baseline": baseline_rps,
                        "percent_change": comparison["percent_change"]
                    })
                elif comparison["improvement"]:
                    status = "⭐ IMPROVEMENT"

                print(f"    {size} rows: {current_rps:.1f} vs {baseline_rps:.1f} rows/s ({comparison['percent_change']:+.1f}%) {status}")

    # Compare single query latency
    if "single_query_latency" in current_results and "single_query_latency" in baseline:
        current_mean = current_results["single_query_latency"]["mean"]
        baseline_mean = baseline["single_query_latency"]["mean"]

        comparison = compare_with_baseline(current_mean, baseline_mean, "latency")
        # For latency, regression means it got worse (higher time)
        latency_regression = comparison["ratio"] > (1 + REGRESSION_THRESHOLD)

        status = "✅"
        if latency_regression:
            status = "❌ REGRESSION"
            regressions.append({
                "metric": "single_query_latency",
                "current": current_mean,
                "baseline": baseline_mean,
                "percent_change": comparison["percent_change"]
            })
        elif comparison["improvement"]:
            status = "⭐ IMPROVEMENT"

        print(f"\n  Single query latency: {current_mean*1000:.1f}ms vs {baseline_mean*1000:.1f}ms ({comparison['percent_change']:+.1f}%) {status}")

    return regressions


# ============================================================================
# Test Runner
# ============================================================================

def main():
    """Run all performance regression tests."""
    print("=" * 70)
    print("Performance Regression Tests - TASK-TEST-001")
    print("=" * 70)
    print(f"CI Mode: {CI_MODE}")
    print(f"DuckDB CLI: {DUCKDB_CLI}")
    print(f"AI Extension: {AI_EXTENSION}")
    print(f"Regression threshold: {REGRESSION_THRESHOLD*100}%")

    # Environment check
    if not DUCKDB_CLI.exists():
        print("\n❌ DuckDB CLI not found")
        return False

    if not AI_EXTENSION.exists():
        print("\n❌ AI Extension not found")
        return False

    # Run tests
    test_results = {}
    tests = [
        ("Single Query Latency", test_1_single_query_latency),
        ("Throughput by Batch Size", test_2_throughput_by_batch_size),
        ("Warm-up Effect", test_3_warm_up_effect),
        ("Memory Efficiency", test_4_memory_efficiency),
        ("Concurrent Performance", test_5_concurrent_performance),
    ]

    print("\n" + "=" * 70)
    print("Running Performance Tests")
    print("=" * 70)

    all_passed = True
    for name, test_func in tests:
        try:
            result = test_func()
            test_results.update(result)
        except Exception as e:
            print(f"  ❌ Test '{name}' raised exception: {e}")
            all_passed = False

    # Detect regressions
    regressions = detect_regressions(test_results)

    # Save current results
    save_current_results(test_results)

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

    if regressions:
        print(f"  ⚠️  Found {len(regressions)} potential regression(s):")
        for r in regressions:
            print(f"    - {r['metric']}: {r['percent_change']:+.1f}% change")
        print("  ⚠️  Please review performance changes")
        # Don't fail the test - just warn about regressions
    else:
        print("  ✅ No regressions detected")
        if load_baseline():
            print("  ✅ Performance stable or improved")
        else:
            print("  ℹ️  Baseline established for future comparisons")

    # Export results with timestamp
    timestamped_results = {
        "timestamp": time.time(),
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "ci_mode": CI_MODE,
        "results": test_results,
        "regressions": regressions
    }

    results_file = Path(__file__).parent / f"performance_results_{int(time.time())}.json"
    with open(results_file, "w") as f:
        json.dump(timestamped_results, f, indent=2)
    print(f"📝 Results saved to: {results_file}")

    return len(regressions) == 0  # Pass if no regressions


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
