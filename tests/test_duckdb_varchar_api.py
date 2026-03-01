#!/usr/bin/env python3
"""
DuckDB Integration Tests - VARCHAR (base64) API 版本

使用 VARCHAR 代替 BLOB，简化扩展实现并避免版本兼容性问题。

API: ai_filter(image_base64 VARCHAR, prompt VARCHAR, model VARCHAR) -> DOUBLE

Run with: python tests/test_duckdb_varchar_api.py
"""

import sys
from pathlib import Path

# Test configuration
DUCKDB_CLI_PATH = Path(__file__).parent.parent.parent / "duckdb" / "build" / "duckdb"
AI_EXTENSION_PATH = Path(__file__).parent.parent.parent / "duckdb" / "build" / "test" / "extension" / "ai.duckdb_extension"


def test_varchar_api():
    """Test VARCHAR (base64) API for ai_filter."""
    import subprocess

    print("=" * 70)
    print("Testing VARCHAR (base64) API")
    print("=" * 70)

    # Test SQL with VARCHAR instead of BLOB
    test_sql = """
        SELECT ai_filter('aGVsbG8gd29ybGQ='::VARCHAR, 'cat', 'clip') AS score
    """

    cmd = [
        str(DUCKDB_CLI_PATH),
        "-unsigned",
        "-c", f"LOAD '{AI_EXTENSION_PATH}'",
        "-c", test_sql
    ]

    print(f"\nCommand: {' '.join(cmd)}")
    print(f"\nTest SQL: {test_sql.strip()}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"\n✅ SUCCESS!")
        print(f"Output:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ FAILED")
        print(f"Error: {e.stderr}")
        return False


def test_base64_encoding():
    """Test base64 encoding example."""
    import base64

    print("\n" + "=" * 70)
    print("Base64 Encoding Example")
    print("=" * 70)

    # Example: encode a simple string
    test_data = b"hello world"
    base64_encoded = base64.b64encode(test_data).decode('utf-8')

    print(f"\nOriginal: {test_data}")
    print(f"Base64: {base64_encoded}")
    print(f"\nSQL Usage: ai_filter('{base64_encoded}'::VARCHAR, 'cat', 'clip')")


def test_image_to_base64_concept():
    """Demonstrate image → base64 concept."""
    print("\n" + "=" * 70)
    print("Image → Base64 Concept (Daft Integration)")
    print("=" * 70)

    print("""
# Daft 侧处理流程：

## 方案 1：Daft DataFrame 列转换
df = daft.read_parquet("images.parquet")
# 假设 df 有 "image" 列（Image 类型，存储为 URL/path）

# 转为 base64（Daft 内置或 UDF）
df = df.with_column(
    "image_base64",
    col("image").url_encode()  # 伪代码，需实现
)

# 转译为 SQL
SELECT * FROM t WHERE ai_filter(image_base64, 'cat', 'clip') > 0.8


## 方案 2：预编码（存储时编码）
# 数据准备阶段已将图像转为 base64 字符串
# Daft 直接使用 VARCHAR 列

df = daft.read_parquet("images_encoded.parquet")
# image_base64 列已经是 base64 字符串

# 直接转译
SELECT * FROM t WHERE ai_filter(image_base64, 'cat', 'clip') > 0.8


## 方案 3：SQL 端编码（如果 DuckDB 支持）
SELECT ai_filter(encode_base64(image_blob)::VARCHAR, 'cat', 'clip') FROM t
    """)


def prepare_varchar_tests():
    """Prepare test cases for VARCHAR API."""
    import base64

    print("\n" + "=" * 70)
    print("Test Cases for VARCHAR API")
    print("=" * 70)

    test_cases = [
        {
            "name": "Simple text base64",
            "data": b"hello",
            "base64": base64.b64encode(b"hello").decode('utf-8'),
        },
        {
            "name": "Empty string",
            "data": b"",
            "base64": base64.b64encode(b"").decode('utf-8'),
        },
        {
            "name": "Binary data",
            "data": b"\\x00\\x01\\x02\\x03",
            "base64": base64.b64encode(b"\\x00\\x01\\x02\\x03").decode('utf-8'),
        },
    ]

    for i, test in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test['name']}")
        print(f"  Data: {test['data']}")
        print(f"  Base64: {test['base64']}")
        print(f"  SQL: ai_filter('{test['base64']}'::VARCHAR, 'cat', 'clip')")


def main():
    """Run all VARCHAR API tests."""
    print("=" * 70)
    print("DuckDB VARCHAR (base64) API Test Suite")
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
    test_base64_encoding()
    test_image_to_base64_concept()
    prepare_varchar_tests()

    # Try to run the actual test
    print("\n" + "=" * 70)
    print("Testing Extension with VARCHAR API...")
    print("=" * 70)

    success = test_varchar_api()

    print("\n" + "=" * 70)
    if success:
        print("✅ All tests passed")
    else:
        print("❌ Some tests failed (extension may not be ready yet)")
    print("=" * 70)

    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
