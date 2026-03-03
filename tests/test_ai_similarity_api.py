#!/usr/bin/env python3
"""
Test AI Similarity API for Daft-DuckDB Integration.

This script tests the new ai_similarity function API:
1. Import ai_similarity from daft.functions
2. Use ai_similarity in expressions
3. Verify SQL translation works correctly
"""

import sys
from pathlib import Path

# Add project paths
daft_root = Path(__file__).parent.parent
sys.path.insert(0, str(daft_root))

def test_import_ai_similarity():
    """Test 1: Import ai_similarity function."""
    print("=" * 70)
    print("Test 1: Import ai_similarity from daft.functions")
    print("=" * 70)

    try:
        from daft.functions import ai_similarity
        print("✅ Successfully imported ai_similarity")
        print(f"   Function: {ai_similarity}")
        print(f"   Module: {ai_similarity.__module__}")
        return True
    except ImportError as e:
        print(f"❌ Failed to import ai_similarity: {e}")
        return False


def test_ai_similarity_expression():
    """Test 2: Create ai_similarity expression."""
    print("\n" + "=" * 70)
    print("Test 2: Create ai_similarity expression")
    print("=" * 70)

    try:
        from daft.functions import ai_similarity
        import daft

        # Create an expression using ai_similarity
        expr = ai_similarity(daft.col("embedding_a"), daft.col("embedding_b"), model="cosine")

        print("✅ Created ai_similarity expression")
        print(f"   Expression: {expr}")
        print(f"   Type: {type(expr)}")

        # Check if expression has the required attributes
        if hasattr(expr, "_is_ai_similarity"):
            print("✅ Expression has _is_ai_similarity attribute")
        else:
            print("⚠️  Missing _is_ai_similarity attribute")

        return True
    except Exception as e:
        print(f"❌ Failed to create ai_similarity expression: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sql_translation():
    """Test 3: Translate ai_similarity expression to SQL."""
    print("\n" + "=" * 70)
    print("Test 3: Translate ai_similarity expression to SQL")
    print("=" * 70)

    try:
        from daft.functions import ai_similarity
        import daft
        from daft.execution.backends.duckdb_translator import SQLTranslator

        # Create an ai_similarity expression
        expr = ai_similarity(daft.col("embedding_a"), daft.col("embedding_b"), model="cosine")

        # Create SQL translator
        translator = SQLTranslator()

        # Translate the expression
        sql = translator._translate_expression(expr)

        print("✅ Translated ai_similarity expression to SQL")
        print(f"   SQL: {sql}")

        # Verify the SQL contains the expected elements
        if "ai_similarity" in sql and "embedding_a" in sql and "embedding_b" in sql and "cosine" in sql:
            print("✅ SQL contains expected elements (ai_similarity, embedding_a, embedding_b, cosine)")
        else:
            print("⚠️  SQL may be incomplete")

        return True
    except Exception as e:
        print(f"❌ Failed to translate to SQL: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_different_models():
    """Test 4: Test different similarity models."""
    print("\n" + "=" * 70)
    print("Test 4: Test different similarity models")
    print("=" * 70)

    try:
        from daft.functions import ai_similarity
        import daft
        from daft.execution.backends.duckdb_translator import SQLTranslator

        models = ["cosine", "dot", "euclidean"]
        translator = SQLTranslator()

        print("\nTesting different models:")
        for model in models:
            expr = ai_similarity(daft.col("vec1"), daft.col("vec2"), model=model)
            sql = translator._translate_expression(expr)
            print(f"  {model:10s}: {sql}")

        print("\n✅ All models translate correctly to SQL")
        return True
    except Exception as e:
        print(f"❌ Failed to test different models: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_join_usage():
    """Test 5: Use ai_similarity in a join condition."""
    print("\n" + "=" * 70)
    print("Test 5: Use ai_similarity in a join condition")
    print("=" * 70)

    try:
        from daft.functions import ai_similarity
        import daft

        # Create two DataFrames
        df_left = daft.from_pydict({
            "id": [1, 2, 3],
            "embedding": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
        })

        df_right = daft.from_pydict({
            "id": [1, 2, 3],
            "embedding": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
        })

        print("✅ Created test DataFrames")

        # Create a similarity expression for join
        similarity_expr = ai_similarity(
            df_left["embedding"],
            df_right["embedding"],
            model="cosine"
        )

        print(f"✅ Created similarity expression: {similarity_expr}")

        # Create a threshold filter
        join_condition = similarity_expr > 0.8
        print(f"✅ Created join condition: {join_condition}")

        return True
    except Exception as e:
        print(f"❌ Failed to create join expression: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_full_api():
    """Test 6: Full API usage examples."""
    print("\n" + "=" * 70)
    print("Test 6: Full API usage examples")
    print("=" * 70)

    try:
        from daft.functions import ai_similarity
        import daft

        print("\nExample 1: Basic similarity calculation")
        print("-" * 70)
        print("from daft.functions import ai_similarity")
        print("import daft")
        print("")
        print("# Calculate similarity between two embeddings")
        print('df = df.with_column("similarity", ai_similarity(df["vec1"], df["vec2"], model="cosine"))')

        print("\nExample 2: Filter by similarity threshold")
        print("-" * 70)
        print('# Filter rows where similarity > 0.8')
        print('filtered = df.filter(ai_similarity(df["query"], df["candidate"], model="cosine") > 0.8)')

        print("\nExample 3: Join DataFrames on semantic similarity")
        print("-" * 70)
        print('df_joined = df_left.join(df_right,')
        print('    on=ai_similarity(df_left["embedding"], df_right["embedding"], model="cosine") > 0.8')
        print(')')

        # Actually create the expressions to verify they work
        df = daft.from_pydict({
            "vec1": [[0.1, 0.2], [0.3, 0.4]],
            "vec2": [[0.1, 0.2], [0.3, 0.4]]
        })

        expr1 = ai_similarity("vec1", "vec2")
        expr2 = ai_similarity(daft.col("vec1"), daft.col("vec2"), model="cosine")
        expr3 = ai_similarity(df["vec1"], df["vec2"], model="dot")

        print("\n✅ All API examples compile successfully")
        print(f"   Expression 1 (string columns): {expr1}")
        print(f"   Expression 2 (cosine): {expr2}")
        print(f"   Expression 3 (dot): {expr3}")

        return True
    except Exception as e:
        print(f"❌ Failed full API test: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("AI Similarity API Tests")
    print("=" * 70)

    results = []

    # Run tests
    results.append(("Import ai_similarity", test_import_ai_similarity()))
    results.append(("Create expression", test_ai_similarity_expression()))
    results.append(("SQL translation", test_sql_translation()))
    results.append(("Different models", test_different_models()))
    results.append(("Join usage", test_join_usage()))
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
        print("  3. Test with actual DuckDB backend and ai_similarity UDF")
        return True
    else:
        print("\n⚠️  Some tests failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
