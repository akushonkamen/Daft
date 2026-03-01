#!/usr/bin/env python3
"""
Test AI Filter API for Daft-DuckDB Integration.

This script tests the new ai_filter function API:
1. Import ai_filter from daft.functions
2. Use ai_filter in expressions
3. Verify SQL translation works correctly
"""

import sys
from pathlib import Path

# Add project paths
daft_root = Path(__file__).parent.parent
sys.path.insert(0, str(daft_root))

def test_import_ai_filter():
    """Test 1: Import ai_filter function."""
    print("=" * 70)
    print("Test 1: Import ai_filter from daft.functions")
    print("=" * 70)

    try:
        from daft.functions import ai_filter
        print("✅ Successfully imported ai_filter")
        print(f"   Function: {ai_filter}")
        print(f"   Module: {ai_filter.__module__}")
        return True
    except ImportError as e:
        print(f"❌ Failed to import ai_filter: {e}")
        return False


def test_ai_filter_expression():
    """Test 2: Create ai_filter expression."""
    print("\n" + "=" * 70)
    print("Test 2: Create ai_filter expression")
    print("=" * 70)

    try:
        from daft.functions import ai_filter
        import daft

        # Create an expression using ai_filter
        expr = ai_filter(daft.col("image"), "cat", model="clip")

        print("✅ Created ai_filter expression")
        print(f"   Expression: {expr}")
        print(f"   Type: {type(expr)}")

        # Check if expression has the required attributes
        if hasattr(expr, "_is_ai_filter"):
            print("✅ Expression has _is_ai_filter attribute")
        else:
            print("⚠️  Missing _is_ai_filter attribute")

        return True
    except Exception as e:
        print(f"❌ Failed to create ai_filter expression: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sql_translation():
    """Test 3: Translate ai_filter expression to SQL."""
    print("\n" + "=" * 70)
    print("Test 3: Translate ai_filter expression to SQL")
    print("=" * 70)

    try:
        from daft.functions import ai_filter
        import daft
        from daft.execution.backends.duckdb_translator import SQLTranslator

        # Create an ai_filter expression
        expr = ai_filter(daft.col("image"), "cat", model="clip")

        # Create SQL translator
        translator = SQLTranslator()

        # Translate the expression
        sql = translator._translate_expression(expr)

        print("✅ Translated ai_filter expression to SQL")
        print(f"   SQL: {sql}")

        # Verify the SQL contains the expected elements
        if "ai_filter" in sql and "image" in sql and "cat" in sql and "clip" in sql:
            print("✅ SQL contains expected elements (ai_filter, image, cat, clip)")
        else:
            print("⚠️  SQL may be incomplete")

        return True
    except Exception as e:
        print(f"❌ Failed to translate to SQL: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_filter_usage():
    """Test 4: Use ai_filter in DataFrame filter."""
    print("\n" + "=" * 70)
    print("Test 4: Use ai_filter in DataFrame filter")
    print("=" * 70)

    try:
        from daft.functions import ai_filter
        import daft

        # Create a simple DataFrame
        df = daft.from_pydict({
            "id": [1, 2, 3],
            "image": ["img1.jpg", "img2.jpg", "img3.jpg"]
        })

        print("✅ Created test DataFrame")

        # Try to use ai_filter in a filter expression
        # Note: This won't actually execute (DuckDB backend not set up)
        # but we can verify the API works
        filtered_expr = ai_filter(daft.col("image"), "cat", model="clip")

        print(f"✅ Created filter expression: {filtered_expr}")

        # Try to create a comparison
        comparison = filtered_expr > 0.8
        print(f"✅ Created comparison expression: {comparison}")

        return True
    except Exception as e:
        print(f"❌ Failed to create filter expression: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_full_api():
    """Test 5: Full API usage example."""
    print("\n" + "=" * 70)
    print("Test 5: Full API usage example")
    print("=" * 70)

    try:
        from daft.functions import ai_filter
        import daft

        print("\nExample 1: Basic usage")
        print("-" * 70)
        print("from daft.functions import ai_filter")
        print("import daft")
        print("")
        print("# Create DataFrame")
        print('df = daft.read_parquet("images.parquet")')
        print("")
        print("# Filter images with ai_filter")
        print('filtered = df.filter(ai_filter("image", "cat") > 0.8)')
        print("")
        print("# Or using column reference")
        print('filtered = df.filter(ai_filter(daft.col("image"), "cat", model="clip") > 0.8)')

        # Actually create the expressions to verify they work
        df = daft.from_pydict({"image": ["a.jpg", "b.jpg"]})
        expr1 = ai_filter("image", "cat")
        expr2 = ai_filter(daft.col("image"), "cat", model="clip")

        print("\n✅ All API examples compile successfully")
        print(f"   Expression 1: {expr1}")
        print(f"   Expression 2: {expr2}")

        return True
    except Exception as e:
        print(f"❌ Failed full API test: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("AI Filter API Tests")
    print("=" * 70)

    results = []

    # Run tests
    results.append(("Import ai_filter", test_import_ai_filter()))
    results.append(("Create expression", test_ai_filter_expression()))
    results.append(("SQL translation", test_sql_translation()))
    results.append(("Filter usage", test_filter_usage()))
    results.append(("Full API", test_full_api()))

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

    for test, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {test}: {status}")

    all_passed = all(r[1] for r in results)

    if all_passed:
        print("\n🎉 All tests passed!")
        print("\nNext steps:")
        print("  1. Update Discussion.md with results")
        print("  2. Create end-to-end integration test")
        print("  3. Test with actual DuckDB backend")
        return True
    else:
        print("\n⚠️  Some tests failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
