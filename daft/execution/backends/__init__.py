"""
Execution backends for Daft.

This module contains various execution backends, including DuckDB integration.
"""

from daft.execution.backends.duckdb_executor import DuckDBExecutor
from daft.execution.backends.duckdb_translator import SQLTranslator
from daft.execution.backends.duckdb_types import daft_type_to_duckdb_sql

__all__ = [
    "DuckDBExecutor",
    "SQLTranslator",
    "daft_type_to_duckdb_sql",
]
