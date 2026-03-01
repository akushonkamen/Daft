"""
DuckDB Executor for running SQL queries generated from Daft LogicalPlan.

This module handles the execution of SQL queries in DuckDB and integration
with the DuckDB AI extension.
"""

import os
from typing import Any, Optional

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


class DuckDBExecutor:
    """
    Executes SQL queries using DuckDB.

    This executor handles:
    - Loading the DuckDB AI extension
    - Registering in-memory data
    - Executing SQL queries
    - Converting results back to Daft-compatible format

    Example:
        >>> executor = DuckDBExecutor(extension_path="/path/to/ai_extension")
        >>> executor.register_table("my_table", data)
        >>> result = executor.execute_sql("SELECT * FROM my_table WHERE ai_filter(image, 'cat') > 0.8")
    """

    def __init__(
        self,
        extension_path: Optional[str] = None,
        extension_name: str = "ai",
        database_path: Optional[str] = ":memory:",
    ):
        """
        Initialize the DuckDB Executor.

        Args:
            extension_path: Path to the DuckDB AI extension (.duckdb_extension file)
            extension_name: Name of the extension (default: "ai")
            database_path: Path to DuckDB database (default: :memory: for in-memory)
        """
        if not DUCKDB_AVAILABLE:
            raise ImportError(
                "DuckDB is not installed. Install it with: pip install duckdb"
            )

        self.extension_path = extension_path
        self.extension_name = extension_name
        self.database_path = database_path

        # Initialize DuckDB connection
        self._conn = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to DuckDB and load extensions."""
        self._conn = duckdb.connect(self.database_path)

        # Load AI extension if path is provided
        if self.extension_path:
            self._load_extension()

    def _load_extension(self) -> None:
        """Load the DuckDB AI extension."""
        if not self.extension_path:
            return

        try:
            # Load the extension
            self._conn.execute(f"LOAD {self.extension_path}")
        except Exception as e:
            raise RuntimeError(
                f"Failed to load DuckDB extension from {self.extension_path}: {e}"
            ) from e

    def execute_sql(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return results.

        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries

        Returns:
            List of dictionaries representing rows

        Example:
            >>> result = executor.execute_sql("SELECT * FROM t WHERE x > $min_val", params={"min_val": 5})
        """
        if self._conn is None:
            self._connect()

        try:
            if params:
                # Execute with parameters
                result = self._conn.execute(sql, params)
            else:
                # Execute without parameters
                result = self._conn.execute(sql)

            # Fetch all results as list of dictionaries
            # Using arrow() for better performance with large datasets
            table = result.arrow()
            rows = []

            # Convert to list of dicts
            for i in range(table.num_rows):
                row = {}
                for col_name in table.column_names:
                    row[col_name] = table[col_name][i].as_py()
                rows.append(row)

            return rows

        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL query: {e}\nQuery: {sql}") from e

    def execute_sql_to_arrow(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """
        Execute a SQL query and return results as PyArrow Table.

        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries

        Returns:
            PyArrow Table

        Example:
            >>> import pyarrow as pa
            >>> table = executor.execute_sql_to_arrow("SELECT * FROM t")
        """
        if self._conn is None:
            self._connect()

        try:
            if params:
                result = self._conn.execute(sql, params)
            else:
                result = self._conn.execute(sql)

            return result.arrow()

        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL query: {e}\nQuery: {sql}") from e

    def register_table(
        self,
        table_name: str,
        data: Any,
    ) -> None:
        """
        Register a data source as a DuckDB table.

        Args:
            table_name: Name for the registered table
            data: Data source (can be:
                - Pandas DataFrame
                - PyArrow Table
                - Polars DataFrame
                - Path to Parquet/CSV file
            )

        Example:
            >>> import pandas as pd
            >>> df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
            >>> executor.register_table("my_table", df)
        """
        if self._conn is None:
            self._connect()

        try:
            # DuckDB can auto-detect and register various data formats
            self._conn.register(table_name, data)
        except Exception as e:
            raise RuntimeError(
                f"Failed to register table '{table_name}': {e}"
            ) from e

    def register_parquet(
        self,
        table_name: str,
        parquet_path: str,
    ) -> None:
        """
        Register a Parquet file as a DuckDB view.

        Args:
            table_name: Name for the view
            parquet_path: Path to Parquet file

        Example:
            >>> executor.register_parquet("my_data", "/path/to/data.parquet")
        """
        if self._conn is None:
            self._connect()

        try:
            sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
            self._conn.execute(sql)
        except Exception as e:
            raise RuntimeError(
                f"Failed to register parquet file '{parquet_path}' as '{table_name}': {e}"
            ) from e

    def get_table_schema(self, table_name: str) -> dict[str, str]:
        """
        Get the schema of a registered table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary mapping column names to SQL types

        Example:
            >>> schema = executor.get_table_schema("my_table")
            >>> print(schema)  # {'x': 'INTEGER', 'y': 'VARCHAR'}
        """
        if self._conn is None:
            self._connect()

        try:
            sql = f"DESCRIBE {table_name}"
            result = self._conn.execute(sql).fetchall()

            return {row[0]: row[1] for row in result}
        except Exception as e:
            raise RuntimeError(
                f"Failed to get schema for table '{table_name}': {e}"
            ) from e

    def execute_from_plan(
        self,
        sql: str,
        data_map: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a SQL query with optional data registration.

        This is a convenience method that:
        1. Registers provided data sources
        2. Executes the SQL query
        3. Returns results

        Args:
            sql: SQL query string
            data_map: Optional mapping of table names to data sources

        Returns:
            List of dictionaries representing rows

        Example:
            >>> data = {"images": df_images, "metadata": df_metadata}
            >>> result = executor.execute_from_plan(
            ...     "SELECT i.*, m.category FROM images i JOIN metadata m ON i.id = m.id",
            ...     data_map=data
            ... )
        """
        if data_map:
            for table_name, data in data_map.items():
                self.register_table(table_name, data)

        return self.execute_sql(sql)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    @property
    def conn(self) -> Any:
        """
        Get the underlying DuckDB connection for advanced usage.

        Returns:
            DuckDB connection object
        """
        if self._conn is None:
            self._connect()
        return self._conn


def create_executor(
    extension_path: Optional[str] = None,
    extension_name: str = "ai",
    database_path: Optional[str] = ":memory:",
) -> DuckDBExecutor:
    """
    Convenience function to create a DuckDB Executor.

    Args:
        extension_path: Path to DuckDB AI extension
        extension_name: Name of the extension
        database_path: Path to DuckDB database

    Returns:
        DuckDBExecutor instance

    Example:
        >>> executor = create_executor(extension_path="/path/to/ai.duckdb_extension")
    """
    return DuckDBExecutor(
        extension_path=extension_path,
        extension_name=extension_name,
        database_path=database_path,
    )
