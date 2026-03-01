#!/usr/bin/env python3
"""
DuckDB CLI Integration Tests for Daft-DuckDB AI Pipeline.

This module tests the complete pipeline using DuckDB CLI via subprocess,
which solves the version compatibility issue between the AI extension
(built for v0.0.1) and Python duckdb library (v1.4.4).

Run with: python tests/test_duckdb_cli_integration.py
"""

import sys
from pathlib import Path

# Add CLI executor to path
cli_executor_path = Path(__file__).parent.parent / "daft" / "execution" / "backends"
sys.path.insert(0, str(cli_executor_path))

try:
    import pytest
except ImportError:
    pytest = None

# Test configuration
DUCKDB_CLI_PATH = Path(__file__).parent.parent.parent / "duckdb" / "build" / "duckdb"
AI_EXTENSION_PATH = Path(__file__).parent.parent.parent / "duckdb" / "build" / "test" / "extension" / "ai.duckdb_extension"

# Import CLI executor directly
import duckdb_cli_executor


class TestDuckDBCLIExecutor:
    """Test DuckDB CLI Executor functionality."""

    def test_cli_exists(self):
        """Test DuckDB CLI exists."""
        assert DUCKDB_CLI_PATH.exists(), f"DuckDB CLI not found at {DUCKDB_CLI_PATH}"

    def test_extension_exists(self):
        """Test AI extension exists."""
        assert AI_EXTENSION_PATH.exists(), f"AI extension not found at {AI_EXTENSION_PATH}"

    def test_executor_creation(self):
        """Test executor can be initialized."""
        executor = duckdb_cli_executor.DuckDBCLIExecutor(
            cli_path=str(DUCKDB_CLI_PATH),
            extension_path=str(AI_EXTENSION_PATH),
        )
        assert executor is not None

    def test_get_version(self):
        """Test getting DuckDB version."""
        executor = duckdb_cli_executor.DuckDBCLIExecutor(
            cli_path=str(DUCKDB_CLI_PATH),
            extension_path=str(AI_EXTENSION_PATH),
        )

        version = executor.get_duckdb_version()
        # CLI version is v1.4.4 (may vary)
        assert "v1.4" in version or "v0.0.1" in version or "1.4.4" in version
        print(f"DuckDB CLI version: {version}")

    def test_basic_query(self):
        """Test basic SQL query execution."""
        executor = duckdb_cli_executor.DuckDBCLIExecutor(
            cli_path=str(DUCKDB_CLI_PATH),
            extension_path=str(AI_EXTENSION_PATH),
        )

        result = executor.execute_sql("SELECT 1 AS num, 'test' AS str")
        assert len(result) == 1
        assert result[0]["num"] == "1"  # CLI returns strings
        assert result[0]["str"] == "test"

    def test_ai_filter_function(self):
        """Test AI filter function call."""
        executor = duckdb_cli_executor.DuckDBCLIExecutor(
            cli_path=str(DUCKDB_CLI_PATH),
            extension_path=str(AI_EXTENSION_PATH),
        )

        # Test ai_filter function
        result = executor.execute_sql(
            "SELECT ai_filter('test_blob'::BLOB, 'cat', 'clip') AS score"
        )

        assert len(result) == 1
        assert "score" in result[0]

        # Score should be a number (as string from CLI)
        score_str = result[0]["score"]
        score = float(score_str)
        assert 0.0 <= score <= 1.0
        print(f"AI filter score: {score}")

    def test_filter_query(self):
        """Test filter query with ai_filter."""
        executor = duckdb_cli_executor.DuckDBCLIExecutor(
            cli_path=str(DUCKDB_CLI_PATH),
            extension_path=str(AI_EXTENSION_PATH),
        )

        # Create test data and filter
        result = executor.execute_sql("""
            SELECT * FROM (
                SELECT 1 AS id, 'image_a'::VARCHAR AS name
                UNION ALL
                SELECT 2 AS id, 'image_b'::VARCHAR AS name
                UNION ALL
                SELECT 3 AS id, 'image_c'::VARCHAR AS name
            ) t
            WHERE ai_filter('test'::BLOB, 'cat', 'clip') > 0.0
        """)

        # Should return some rows (all match since condition is > 0.0)
        assert len(result) >= 1
        print(f"Filter returned {len(result)} rows")

    def test_aggregation_query(self):
        """Test aggregation query."""
        executor = duckdb_cli_executor.DuckDBCLIExecutor(
            cli_path=str(DUCKDB_CLI_PATH),
            extension_path=str(AI_EXTENSION_PATH),
        )

        result = executor.execute_sql("""
            SELECT
                COUNT(*) AS count,
                AVG(ai_filter('test'::BLOB, 'cat', 'clip')) AS avg_score
            FROM range(10)
        """)

        assert len(result) == 1
        assert result[0]["count"] == "10"
        # avg_score should be present
        assert "avg_score" in result[0]

    def test_context_manager(self):
        """Test executor as context manager."""
        with duckdb_cli_executor.DuckDBCLIExecutor(
            cli_path=str(DUCKDB_CLI_PATH),
            extension_path=str(AI_EXTENSION_PATH),
        ) as executor:
            result = executor.execute_sql("SELECT 1 AS num")
            assert len(result) == 1


class TestEndToEndCLI:
    """Test end-to-end pipeline using CLI executor."""

    def test_simple_pipeline(self):
        """Test simple data pipeline."""
        executor = duckdb_cli_executor.DuckDBCLIExecutor(
            cli_path=str(DUCKDB_CLI_PATH),
            extension_path=str(AI_EXTENSION_PATH),
        )

        # Simulate a simple Daft-like pipeline
        # 1. Source (simulated with VALUES)
        # 2. Filter with ai_filter
        # 3. Project (SELECT)
        result = executor.execute_sql("""
            SELECT
                id,
                name,
                score
            FROM (
                SELECT
                    id,
                    name,
                    ai_filter('data'::BLOB, 'cat', 'clip') AS score
                FROM (
                    SELECT 1 AS id, 'Widget A'::VARCHAR AS name
                    UNION ALL
                    SELECT 2 AS id, 'Widget B'::VARCHAR AS name
                    UNION ALL
                    SELECT 3 AS id, 'Widget C'::VARCHAR AS name
                ) t
            ) t2
            WHERE score > 0.0
            ORDER BY score DESC
        """)

        assert len(result) == 3
        print(f"Pipeline returned {len(result)} rows")


def run_cli_integration_tests():
    """Run CLI integration tests manually without pytest."""
    print("=" * 70)
    print("DuckDB CLI Integration Tests")
    print("=" * 70)

    # Check prerequisites
    if not DUCKDB_CLI_PATH.exists():
        print(f"\n❌ DuckDB CLI not found at: {DUCKDB_CLI_PATH}")
        return False

    if not AI_EXTENSION_PATH.exists():
        print(f"\n❌ AI extension not found at: {AI_EXTENSION_PATH}")
        return False

    print(f"\n✅ DuckDB CLI found: {DUCKDB_CLI_PATH}")
    print(f"✅ AI extension found: {AI_EXTENSION_PATH}")

    # Run tests
    tests_passed = 0
    tests_failed = 0

    test_classes = [
        TestDuckDBCLIExecutor,
        TestEndToEndCLI,
    ]

    for test_class in test_classes:
        print(f"\n{'=' * 70}")
        print(f"Testing: {test_class.__name__}")
        print("=" * 70)

        test_instance = test_class()

        test_methods = [
            method for method in dir(test_instance)
            if method.startswith("test_") and callable(getattr(test_instance, method))
        ]

        for method_name in test_methods:
            method = getattr(test_instance, method_name)
            try:
                print(f"  Running {method_name}...", end=" ")
                method()
                print("✅ PASSED")
                tests_passed += 1
            except Exception as e:
                print(f"❌ FAILED: {e}")
                tests_failed += 1

    print(f"\n{'=' * 70}")
    print(f"Test Results: {tests_passed} passed, {tests_failed} failed")
    print("=" * 70)

    return tests_failed == 0


if __name__ == "__main__":
    success = run_cli_integration_tests()
    sys.exit(0 if success else 1)
