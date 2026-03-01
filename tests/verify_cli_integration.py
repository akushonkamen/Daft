#!/usr/bin/env python3
"""
DuckDB CLI 集成验证脚本

验证 AI 扩展在 CLI 下的完整功能。

Run: python tests/verify_cli_integration.py
"""

import subprocess
from pathlib import Path

# Configuration
DUCKDB_CLI = Path(__file__).parent.parent.parent / "duckdb" / "build" / "duckdb"
AI_EXTENSION = Path(__file__).parent.parent.parent / "duckdb" / "build" / "test" / "extension" / "ai.duckdb_extension"


def run_duckdb(sql):
    """Run SQL via DuckDB CLI and return output."""
    cmd = [
        str(DUCKDB_CLI),
        "-unsigned",
        "-c", f"LOAD '{AI_EXTENSION}';",
        "-c", sql
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout, result.stderr, result.returncode


def test_basic_call():
    """Test 1: Basic ai_filter() call."""
    print("Test 1: Basic ai_filter() call", end=" ... ")

    sql = "SELECT ai_filter() AS score;"
    stdout, stderr, code = run_duckdb(sql)

    if code == 0 and "score" in stdout.lower():
        print("✅ PASSED")
        print(f"  Output:\n{stdout}")
        return True
    else:
        print("❌ FAILED")
        print(f"  Error: {stderr}")
        return False


def test_multiple_calls():
    """Test 2: Multiple ai_filter() calls in one query."""
    print("\nTest 2: Multiple ai_filter() calls", end=" ... ")

    sql = "SELECT ai_filter() AS s1, ai_filter() AS s2, ai_filter() AS s3;"
    stdout, stderr, code = run_duckdb(sql)

    if code == 0 and "s1" in stdout.lower() and "s2" in stdout.lower():
        print("✅ PASSED")
        print(f"  Output:\n{stdout}")
        return True
    else:
        print("❌ FAILED")
        print(f"  Error: {stderr}")
        return False


def test_where_clause():
    """Test 3: ai_filter() in WHERE clause."""
    print("\nTest 3: ai_filter() in WHERE clause", end=" ... ")

    sql = """
    SELECT * FROM (
        SELECT 1 AS id, 'item_a'::VARCHAR AS name
        UNION ALL
        SELECT 2 AS id, 'item_b'::VARCHAR AS name
    ) t
    WHERE ai_filter() > 0.0
    ORDER BY id;
    """
    stdout, stderr, code = run_duckdb(sql)

    if code == 0 and "item_a" in stdout:
        print("✅ PASSED")
        print(f"  Output:\n{stdout}")
        return True
    else:
        print("❌ FAILED")
        print(f"  Error: {stderr}")
        return False


def test_aggregation():
    """Test 4: ai_filter() with aggregation functions."""
    print("\nTest 4: ai_filter() with aggregations", end=" ... ")

    sql = """
    SELECT
        COUNT(*) AS count,
        MIN(ai_filter()) AS min_score,
        AVG(ai_filter()) AS avg_score
    FROM range(5);
    """
    stdout, stderr, code = run_duckdb(sql)

    if code == 0 and "count" in stdout.lower() and "min_score" in stdout.lower():
        print("✅ PASSED")
        print(f"  Output:\n{stdout}")
        return True
    else:
        print("❌ FAILED")
        print(f"  Error: {stderr}")
        return False


def test_project():
    """Test 5: ai_filter() in SELECT projection."""
    print("\nTest 5: ai_filter() in SELECT projection", end=" ... ")

    sql = """
    SELECT
        id,
        name,
        ai_filter() AS ai_score
    FROM (
        SELECT 1 AS id, 'product_a'::VARCHAR AS name
        UNION ALL
        SELECT 2 AS id, 'product_b'::VARCHAR AS name
    ) t
    ORDER BY id;
    """
    stdout, stderr, code = run_duckdb(sql)

    if code == 0 and "product_a" in stdout and "ai_score" in stdout.lower():
        print("✅ PASSED")
        print(f"  Output:\n{stdout}")
        return True
    else:
        print("❌ FAILED")
        print(f"  Error: {stderr}")
        return False


def main():
    """Run all verification tests."""
    print("=" * 70)
    print("DuckDB CLI Integration Verification")
    print("=" * 70)

    # Check prerequisites
    if not DUCKDB_CLI.exists():
        print(f"\n❌ DuckDB CLI not found at: {DUCKDB_CLI}")
        return False

    if not AI_EXTENSION.exists():
        print(f"\n❌ AI extension not found at: {AI_EXTENSION}")
        return False

    print(f"\n✅ DuckDB CLI: {DUCKDB_CLI}")
    print(f"✅ AI Extension: {AI_EXTENSION}")
    print(f"   Size: {AI_EXTENSION.stat().st_size / (1024*1024):.1f} MB")

    # Get DuckDB version
    version_sql = "SELECT version();"
    stdout, _, _ = run_duckdb(version_sql)
    print(f"✅ DuckDB Version: {stdout.split()[1] if len(stdout.split()) > 1 else 'Unknown'}")

    # Run tests
    print("\n" + "=" * 70)
    print("Running Tests")
    print("=" * 70)

    tests = [
        test_basic_call,
        test_multiple_calls,
        test_where_clause,
        test_aggregation,
        test_project,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ FAILED with exception: {e}")
            failed += 1

    # Summary
    print("\n" + "=" * 70)
    print(f"Test Summary: {passed} passed, {failed} failed")
    print("=" * 70)

    if failed == 0:
        print("\n🎉 All tests passed! CLI integration is working correctly.")
        print("\nNext steps:")
        print("  1. Update Discussion.md with verification results")
        print("  2. Commit changes")
        print("  3. Notify team-lead to request sync")

    return failed == 0


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
