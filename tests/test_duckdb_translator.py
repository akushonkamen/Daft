"""
Tests for DuckDB SQL Translator.

This module contains tests for the SQL translation layer.
"""

import pytest

from daft.datatype import DataType
from daft.execution.backends.duckdb_types import daft_type_to_duckdb_sql, get_duckdb_type_mapping
from daft.execution.backends.duckdb_translator import SQLTranslator, translate_to_sql


class TestDuckDBTypes:
    """Test Daft to DuckDB type mapping."""

    def test_integer_types(self):
        """Test integer type mappings."""
        assert daft_type_to_duckdb_sql(DataType.int8()) == "TINYINT"
        assert daft_type_to_duckdb_sql(DataType.int16()) == "SMALLINT"
        assert daft_type_to_duckdb_sql(DataType.int32()) == "INTEGER"
        assert daft_type_to_duckdb_sql(DataType.int64()) == "BIGINT"

    def test_unsigned_integer_types(self):
        """Test unsigned integer type mappings."""
        assert daft_type_to_duckdb_sql(DataType.uint8()) == "UTINYINT"
        assert daft_type_to_duckdb_sql(DataType.uint16()) == "USMALLINT"
        assert daft_type_to_duckdb_sql(DataType.uint32()) == "UINTEGER"
        assert daft_type_to_duckdb_sql(DataType.uint64()) == "UBIGINT"

    def test_float_types(self):
        """Test float type mappings."""
        assert daft_type_to_duckdb_sql(DataType.float32()) == "FLOAT"
        assert daft_type_to_duckdb_sql(DataType.float64()) == "DOUBLE"

    def test_basic_types(self):
        """Test basic type mappings."""
        assert daft_type_to_duckdb_sql(DataType.bool()) == "BOOLEAN"
        assert daft_type_to_duckdb_sql(DataType.string()) == "VARCHAR"
        assert daft_type_to_duckdb_sql(DataType.binary()) == "BLOB"
        assert daft_type_to_duckdb_sql(DataType.null() if hasattr(DataType, 'null') else DataType.null_type()) == "NULL"

    def test_temporal_types(self):
        """Test temporal type mappings."""
        assert daft_type_to_duckdb_sql(DataType.date()) == "DATE"
        assert daft_type_to_duckdb_sql(DataType.timestamp()) == "TIMESTAMP"

    def test_multimodal_types(self):
        """Test multimodal type mappings."""
        # Image type
        if hasattr(DataType, 'image'):
            assert daft_type_to_duckdb_sql(DataType.image()) == "VARCHAR"

        # Embedding type
        if hasattr(DataType, 'embedding'):
            assert daft_type_to_duckdb_sql(DataType.embedding()) == "FLOAT[]"

        # Audio type
        if hasattr(DataType, 'audio'):
            assert daft_type_to_duckdb_sql(DataType.audio()) == "VARCHAR"

    def test_get_type_mapping(self):
        """Test getting the complete type mapping."""
        mapping = get_duckdb_type_mapping()
        assert isinstance(mapping, dict)
        assert len(mapping) > 0
        assert "Int64Type" in mapping
        assert mapping["Int64Type"] == "BIGINT"


class TestSQLTranslator:
    """Test SQL Translator."""

    def test_translator_initialization(self):
        """Test translator can be initialized."""
        translator = SQLTranslator()
        assert translator is not None
        assert translator.extension_path is None

    def test_translator_with_extension_path(self):
        """Test translator with extension path."""
        extension_path = "/path/to/extension.duckdb_extension"
        translator = SQLTranslator(extension_path=extension_path)
        assert translator.extension_path == extension_path

    def test_format_literal_null(self):
        """Test formatting NULL literal."""
        translator = SQLTranslator()
        assert translator._format_literal(None) == "NULL"

    def test_format_literal_string(self):
        """Test formatting string literals."""
        translator = SQLTranslator()
        assert translator._format_literal("hello") == "'hello'"
        assert translator._format_literal("it's") == "'it''s'"  # Escaped quote

    def test_format_literal_boolean(self):
        """Test formatting boolean literals."""
        translator = SQLTranslator()
        assert translator._format_literal(True) == "TRUE"
        assert translator._format_literal(False) == "FALSE"

    def test_format_literal_number(self):
        """Test formatting numeric literals."""
        translator = SQLTranslator()
        assert translator._format_literal(42) == "42"
        assert translator._format_literal(3.14) == "3.14"

    def test_format_literal_list(self):
        """Test formatting list literals."""
        translator = SQLTranslator()
        result = translator._format_literal([1, 2, 3])
        # Should be formatted as DuckDB array
        assert "[" in result
        assert "]" in result

    def test_get_operator_symbol(self):
        """Test getting operator symbols."""
        from daft.expressions import col

        translator = SQLTranslator()

        # Note: This test assumes we can create expressions
        # In a real environment with compiled bindings, we would test actual expressions
        # For now, we test with mock data


class TestTranslateToSQL:
    """Test convenience function."""

    def test_translate_to_sql_basic(self):
        """Test basic SQL translation."""
        # This is a placeholder - full test would require actual LogicalPlanBuilder
        # which needs compiled Rust bindings
        pass


class TestDuckDBExecutor:
    """Test DuckDB Executor."""

    @pytest.mark.skipif(
        not __import__("sys").modules.get("duckdb"),
        reason="DuckDB not installed"
    )
    def test_executor_initialization(self):
        """Test executor can be initialized."""
        from daft.execution.backends.duckdb_executor import DuckDBExecutor

        executor = DuckDBExecutor(database_path=":memory:")
        assert executor is not None
        executor.close()

    @pytest.mark.skipif(
        not __import__("sys").modules.get("duckdb"),
        reason="DuckDB not installed"
    )
    def test_executor_context_manager(self):
        """Test executor as context manager."""
        from daft.execution.backends.duckdb_executor import DuckDBExecutor

        with DuckDBExecutor(database_path=":memory:") as executor:
            assert executor is not None
            assert executor.conn is not None


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
