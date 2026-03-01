#!/usr/bin/env python3
"""
Quick integration test runner for Daft-DuckDB AI pipeline.

This script provides a quick way to verify the end-to-end integration
without requiring the full pytest infrastructure.

Usage:
    python tests/run_integration_tests.py
"""

import sys
from pathlib import Path

# Add Daft to path
daft_root = Path(__file__).parent.parent
sys.path.insert(0, str(daft_root))

try:
    import duckdb
except ImportError:
    print("❌ DuckDB not installed. Install with: pip install duckdb")
    sys.exit(1)

# Configuration
EXTENSION_PATH = daft_root.parent / "duckdb" / "build" / "test" / "extension" / "ai.duckdb_extension"


def test_extension_exists():
    """Test 1: Check if extension file exists."""
    print("Test 1: Extension File Check")
    print(f"  Looking for: {EXTENSION_PATH}")

    if EXTENSION_PATH.exists():
        size = EXTENSION_PATH.stat().st_size
        print(f"  ✅ Extension found ({size:,} bytes)")
        return True
    else:
        print(f"  ❌ Extension not found at {EXTENSION_PATH}")
        return False


def test_extension_loads():
    """Test 2: Check if extension loads in DuckDB."""
    print("\nTest 2: Extension Loading")

    if not EXTENSION_PATH.exists():
        print("  ⏭️  Skipping (extension file not found)")
        return None

    try:
        con = duckdb.connect(":memory:")

        # Check for version compatibility
        import duckdb as duckdb_module
        duckdb_version = duckdb_module.__version__
        print(f"  ℹ️  Python duckdb version: {duckdb_version}")

        # The extension was built for DuckDB v0.0.1
        # Python bindings use v1.4.4 - this causes a version mismatch
        if duckdb_version != "0.0.1":
            print(f"  ⚠️  Version mismatch detected!")
            print(f"      Extension built for: v0.0.1")
            print(f"      Python duckdb version: v{duckdb_version}")
            print(f"  ❌ Extension cannot be loaded due to version mismatch")
            print(f"  💡 Solution: duckdb-engineer needs to rebuild extension for v{duckdb_version}")
            return False

        con.execute(f"LOAD '{EXTENSION_PATH}'")
        print("  ✅ Extension loaded successfully")
        con.close()
        return True
    except Exception as e:
        error_msg = str(e)
        if "version" in error_msg.lower():
            print(f"  ⚠️  Version mismatch error: {error_msg[:100]}...")
            print(f"  💡 This is expected - extension needs to be rebuilt for Python duckdb version")
            return False
        else:
            print(f"  ❌ Failed to load extension: {e}")
            return False


def test_ai_filter_function():
    """Test 3: Check if ai_filter function exists."""
    print("\nTest 3: AI Filter Function")

    if not EXTENSION_PATH.exists():
        print("  ⏭️  Skipping (extension file not found)")
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"LOAD '{EXTENSION_PATH}'")

        # Try to call ai_filter
        result = con.execute("SELECT ai_filter('test_blob'::BLOB, 'cat', 'clip') AS score").fetchone()

        if result and result[0] is not None:
            print(f"  ✅ ai_filter function works (returned score: {result[0]})")
            con.close()
            return True
        else:
            print("  ❌ ai_filter returned NULL")
            con.close()
            return False

    except Exception as e:
        print(f"  ❌ Failed to call ai_filter: {e}")
        return False


def test_type_mapping():
    """Test 4: Type mapping functionality."""
    print("\nTest 4: Type Mapping")

    try:
        from daft.datatype import DataType
        from daft.execution.backends.duckdb_types import daft_type_to_duckdb_sql

        # Test a few types
        tests = [
            (DataType.int64(), "BIGINT"),
            (DataType.float64(), "DOUBLE"),
            (DataType.string(), "VARCHAR"),
            (DataType.bool(), "BOOLEAN"),
        ]

        all_passed = True
        for dtype, expected in tests:
            result = daft_type_to_duckdb_sql(dtype)
            if result == expected:
                print(f"  ✅ {dtype} → {result}")
            else:
                print(f"  ❌ {dtype} → {result} (expected {expected})")
                all_passed = False

        return all_passed

    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return False


def test_translator_creation():
    """Test 5: SQL Translator creation."""
    print("\nTest 5: SQL Translator")

    try:
        from daft.execution.backends.duckdb_translator import SQLTranslator

        translator = SQLTranslator()
        print("  ✅ SQLTranslator created")

        # Test literal formatting
        tests = [
            ("hello", "'hello'"),
            (True, "TRUE"),
            (False, "FALSE"),
            (42, "42"),
            (None, "NULL"),
        ]

        all_passed = True
        for value, expected in tests:
            result = translator._format_literal(value)
            if result == expected:
                print(f"  ✅ Format {value} → {result}")
            else:
                print(f"  ❌ Format {value} → {result} (expected {expected})")
                all_passed = False

        return all_passed

    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return False


def test_executor_creation():
    """Test 6: DuckDB Executor creation."""
    print("\nTest 6: DuckDB Executor")

    try:
        from daft.execution.backends.duckdb_executor import DuckDBExecutor

        if EXTENSION_PATH.exists():
            executor = DuckDBExecutor(
                extension_path=str(EXTENSION_PATH),
                database_path=":memory:",
            )
            print("  ✅ DuckDBExecutor created with extension")
        else:
            executor = DuckDBExecutor(database_path=":memory:")
            print("  ✅ DuckDBExecutor created (without extension)")

        # Test basic query
        result = executor.execute_sql("SELECT 1 AS num")
        if result and result[0]["num"] == 1:
            print("  ✅ Basic query execution works")
        else:
            print("  ❌ Basic query failed")
            executor.close()
            return False

        executor.close()
        return True

    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return False


def test_end_to_end():
    """Test 7: End-to-end pipeline test."""
    print("\nTest 7: End-to-End Pipeline")

    try:
        from daft.execution.backends.duckdb_executor import DuckDBExecutor

        if not EXTENSION_PATH.exists():
            print("  ⏭️  Skipping (extension file not found)")
            return None

        executor = DuckDBExecutor(
            extension_path=str(EXTENSION_PATH),
            database_path=":memory:",
        )

        # Create test table
        executor.execute_sql(
            "CREATE TABLE test_products AS "
            "SELECT * FROM ("
            "SELECT 1 AS id, 'Widget A'::VARCHAR AS name, 10.0::FLOAT AS price "
            "UNION ALL "
            "SELECT 2 AS id, 'Widget B'::VARCHAR AS name, 20.0::FLOAT AS price "
            "UNION ALL "
            "SELECT 3 AS id, 'Widget C'::VARCHAR AS name, 30.0::FLOAT AS price "
            ") t"
        )
        print("  ✅ Test table created")

        # Test filter query
        result = executor.execute_sql(
            "SELECT * FROM test_products WHERE price > 15.0 ORDER BY price"
        )
        if len(result) == 2:
            print(f"  ✅ Filter query works (returned {len(result)} rows)")
        else:
            print(f"  ❌ Filter query failed (expected 2 rows, got {len(result)})")
            executor.close()
            return False

        # Test aggregation query
        result = executor.execute_sql(
            "SELECT COUNT(*) AS count, AVG(price) AS avg_price FROM test_products"
        )
        if result[0]["count"] == 3 and abs(result[0]["avg_price"] - 20.0) < 0.01:
            print(f"  ✅ Aggregation query works")
        else:
            print(f"  ❌ Aggregation query failed")
            executor.close()
            return False

        executor.close()
        return True

    except Exception as e:
        print(f"  ❌ Failed: {e}")
        return False


def main():
    """Run all integration tests."""
    print("=" * 70)
    print("Daft-DuckDB Integration Test Suite")
    print("=" * 70)

    # Run all tests
    tests = [
        test_extension_exists,
        test_extension_loads,
        test_ai_filter_function,
        test_type_mapping,
        test_translator_creation,
        test_executor_creation,
        test_end_to_end,
    ]

    results = []
    for test_func in tests:
        try:
            result = test_func()
            results.append((test_func.__name__, result))
        except Exception as e:
            print(f"\n❌ Test {test_func.__name__} crashed: {e}")
            results.append((test_func.__name__, False))

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

    passed = sum(1 for _, result in results if result is True)
    failed = sum(1 for _, result in results if result is False)
    skipped = sum(1 for _, result in results if result is None)

    for test_name, result in results:
        status = "✅ PASSED" if result is True else ("❌ FAILED" if result is False else "⏭️  SKIPPED")
        print(f"  {test_name}: {status}")

    print(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped")
    print("=" * 70)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
