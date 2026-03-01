"""
DuckDB CLI Executor for running SQL queries using subprocess.

This module provides an alternative executor that uses the DuckDB CLI
via subprocess instead of the Python API. This solves version compatibility
issues when the AI extension was built for a different DuckDB version.
"""

import json
import subprocess
from pathlib import Path
from typing import Any, Optional


class DuckDBCLIExecutor:
    """
    Executes SQL queries using DuckDB CLI via subprocess.

    This executor handles:
    - Loading the DuckDB AI extension (matching CLI version)
    - Executing SQL queries
    - Converting results back to Daft-compatible format

    Example:
        >>> executor = DuckDBCLIExecutor(
        ...     cli_path="./duckdb",
        ...     extension_path="/path/to/ai_extension.duckdb_extension"
        ... )
        >>> result = executor.execute_sql("SELECT * FROM my_table WHERE ai_filter(image, 'cat') > 0.8")
    """

    def __init__(
        self,
        cli_path: str,
        extension_path: Optional[str] = None,
        database_path: Optional[str] = ":memory:",
    ):
        """
        Initialize the DuckDB CLI Executor.

        Args:
            cli_path: Path to DuckDB CLI executable
            extension_path: Path to the DuckDB AI extension (.duckdb_extension file)
            database_path: Path to DuckDB database (default: :memory: for in-memory)
        """
        self.cli_path = Path(cli_path)
        self.extension_path = extension_path
        self.database_path = database_path

        # Verify CLI exists
        if not self.cli_path.exists():
            raise FileNotFoundError(
                f"DuckDB CLI not found at {self.cli_path}. "
                "Please ensure the path is correct."
            )

    def execute_sql(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return results.

        Args:
            sql: SQL query string
            params: Optional parameters (not currently supported in CLI mode)

        Returns:
            List of dictionaries representing rows

        Example:
            >>> result = executor.execute_sql("SELECT * FROM t WHERE x > 5")
        """
        if params:
            raise NotImplementedError(
                "Parameterized queries are not supported in CLI mode. "
                "Use string interpolation instead."
            )

        # Build the command
        cmd = self._build_command(sql)

        # Execute the command
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to execute SQL query via CLI:\n"
                f"Command: {' '.join(cmd)}\n"
                f"Error: {e.stderr}"
            ) from e

        # Parse the output
        return self._parse_output(result.stdout)

    def _build_command(self, sql: str) -> list[str]:
        """
        Build the subprocess command for executing SQL.

        Args:
            sql: SQL query string

        Returns:
            List of command arguments
        """
        cmd = [str(self.cli_path), "-unsigned"]

        # Add extension loading if provided
        if self.extension_path:
            cmd.extend(["-c", f"LOAD '{self.extension_path}';"])

        # Add the SQL query
        cmd.extend(["-c", sql])

        return cmd

    def _parse_output(self, output: str) -> list[dict[str, Any]]:
        """
        Parse DuckDB CLI output into a list of dictionaries.

        Args:
            output: Raw output from DuckDB CLI

        Returns:
            List of dictionaries representing rows
        """
        # Split output into lines
        lines = output.strip().split("\n")

        if len(lines) < 3:
            # No results or error
            return []

        # Extract column names (second line, after border)
        # Format: │ col1 │ col2 │ col3 │
        header_line = lines[1].strip()
        columns = [col.strip() for col in header_line.split("│") if col.strip()]

        # Extract data rows (skip header, separator, and footer)
        data_lines = lines[2:-1]  # Skip separator and footer border

        results = []
        for line in data_lines:
            if not line.strip() or "├" in line or "└" in line:
                # Skip separator lines
                continue

            # Extract values: │ val1 │ val2 │ val3 │
            values = [val.strip() for val in line.split("│") if val.strip()]

            if len(values) == len(columns):
                row = dict(zip(columns, values))
                results.append(row)

        return results

    def execute_sql_to_json(
        self,
        sql: str,
    ) -> str:
        """
        Execute a SQL query and return results as JSON string.

        This is useful for output that needs to be parsed later.

        Args:
            sql: SQL query string

        Returns:
            JSON string representation of results

        Example:
            >>> json_str = executor.execute_sql_to_json("SELECT * FROM t")
        """
        # Modify SQL to output JSON
        json_sql = f"COPY ({sql}) TO STDOUT (FORMAT JSON, ARRAY true);"
        cmd = self._build_command(json_sql)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to execute SQL query via CLI:\n"
                f"Command: {' '.join(cmd)}\n"
                f"Error: {e.stderr}"
            ) from e

    def execute_sql_to_arrow(
        self,
        sql: str,
    ) -> Any:
        """
        Execute a SQL query and return results as PyArrow Table.

        Args:
            sql: SQL query string

        Returns:
            PyArrow Table

        Example:
            >>> import pyarrow as pa
            >>> table = executor.execute_sql_to_arrow("SELECT * FROM t")
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "PyArrow is required for execute_sql_to_arrow. "
                "Install it with: pip install pyarrow"
            )

        # Get results as list of dicts
        results = self.execute_sql(sql)

        if not results:
            # Return empty table with no schema
            return pa.table({})

        # Convert to PyArrow Table
        return pa.table(results)

    def get_duckdb_version(self) -> str:
        """
        Get the DuckDB CLI version.

        Returns:
            Version string
        """
        cmd = [str(self.cli_path), "-c", "SELECT version();"]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        # Parse version from output
        return result.stdout.strip()

    def close(self) -> None:
        """
        Close the executor (no-op for CLI mode).

        This method exists for compatibility with the Python API executor.
        """
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def create_cli_executor(
    cli_path: str,
    extension_path: Optional[str] = None,
    database_path: Optional[str] = ":memory:",
) -> DuckDBCLIExecutor:
    """
    Convenience function to create a DuckDB CLI Executor.

    Args:
        cli_path: Path to DuckDB CLI executable
        extension_path: Path to DuckDB AI extension
        database_path: Path to DuckDB database

    Returns:
        DuckDBCLIExecutor instance

    Example:
        >>> executor = create_cli_executor(
        ...     cli_path="./duckdb",
        ...     extension_path="/path/to/ai.duckdb_extension"
        ... )
    """
    return DuckDBCLIExecutor(
        cli_path=cli_path,
        extension_path=extension_path,
        database_path=database_path,
    )
