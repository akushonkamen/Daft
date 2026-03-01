"""
Type mapping between Daft and DuckDB SQL types.

This module provides the mapping from Daft DataType to DuckDB SQL types.
"""

from typing import Dict

from daft.datatype import DataType


def daft_type_to_duckdb_sql(dtype: DataType) -> str:
    """
    Convert a Daft DataType to DuckDB SQL type string.

    Args:
        dtype: Daft DataType

    Returns:
        DuckDB SQL type string

    Examples:
        >>> daft_type_to_duckdb_sql(DataType.int64())
        'BIGINT'
        >>> daft_type_to_duckdb_sql(DataType.string())
        'VARCHAR'
    """
    # Get the type name for matching
    type_name = dtype.__class__.__name__

    # Handle basic types
    if type_name == "Int8Type":
        return "TINYINT"
    elif type_name == "Int16Type":
        return "SMALLINT"
    elif type_name == "Int32Type":
        return "INTEGER"
    elif type_name == "Int64Type":
        return "BIGINT"
    elif type_name == "UInt8Type":
        return "UTINYINT"
    elif type_name == "UInt16Type":
        return "USMALLINT"
    elif type_name == "UInt32Type":
        return "UINTEGER"
    elif type_name == "UInt64Type":
        return "UBIGINT"
    elif type_name == "Float32Type":
        return "FLOAT"
    elif type_name == "Float64Type":
        return "DOUBLE"
    elif type_name == "BooleanType":
        return "BOOLEAN"
    elif type_name == "Utf8Type" or type_name == "StringType":
        return "VARCHAR"
    elif type_name == "BinaryType":
        return "BLOB"
    elif type_name == "DateType":
        return "DATE"
    elif type_name == "TimestampType":
        return "TIMESTAMP"
    elif type_name == "ImageType":
        # Image stored as path (VARCHAR) or BLOB for raw data
        return "VARCHAR"
    elif type_name == "EmbeddingType":
        # Embedding as FLOAT array
        return "FLOAT[]"
    elif type_name == "AudioType":
        # Audio as path or BLOB
        return "VARCHAR"
    elif type_name == "NullType":
        return "NULL"
    else:
        # For complex types (List, Struct, etc.), use a generic approach
        # This can be expanded as needed
        return "VARCHAR"  # Default fallback


def get_duckdb_type_mapping() -> Dict[str, str]:
    """
    Returns the complete mapping of Daft type names to DuckDB SQL types.

    Returns:
        Dictionary mapping Daft type class names to DuckDB SQL types
    """
    return {
        # Integer types
        "Int8Type": "TINYINT",
        "Int16Type": "SMALLINT",
        "Int32Type": "INTEGER",
        "Int64Type": "BIGINT",
        "UInt8Type": "UTINYINT",
        "UInt16Type": "USMALLINT",
        "UInt32Type": "UINTEGER",
        "UInt64Type": "UBIGINT",

        # Float types
        "Float32Type": "FLOAT",
        "Float64Type": "DOUBLE",

        # Basic types
        "BooleanType": "BOOLEAN",
        "Utf8Type": "VARCHAR",
        "StringType": "VARCHAR",
        "BinaryType": "BLOB",

        # Temporal types
        "DateType": "DATE",
        "TimestampType": "TIMESTAMP",
        "TimeType": "TIME",

        # Multimodal types
        "ImageType": "VARCHAR",
        "EmbeddingType": "FLOAT[]",
        "AudioType": "VARCHAR",
        "VideoType": "VARCHAR",

        # Null type
        "NullType": "NULL",
    }
