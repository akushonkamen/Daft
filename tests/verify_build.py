#!/usr/bin/env python3
"""
Verify Daft Python bindings compilation and run E2E test.

This script runs after `make build` completes.
"""

import sys
from pathlib import Path

def verify_daft_import():
    """Verify Daft can be imported."""
    print("=" * 70)
    print("Step 1: Verifying Daft Import")
    print("=" * 70)

    try:
        import daft
        print("✅ Successfully imported daft")
        print(f"   Version: {daft.__version__ if hasattr(daft, '__version__') else 'unknown'}")
        return True
    except ImportError as e:
        print(f"❌ Failed to import daft: {e}")
        return False

def verify_basic_operations():
    """Verify basic Daft operations work."""
    print("\n" + "=" * 70)
    print("Step 2: Verifying Basic Operations")
    print("=" * 70)

    try:
        import daft

        # Test 1: Create DataFrame from dict
        print("\nTest 1: Create DataFrame from dict", end=" ... ")
        df = daft.from_pydict({
            "x": [1, 2, 3],
            "y": [4, 5, 6]
        })
        assert df.shape() == (3, 2), f"Expected (3, 2), got {df.shape()}"
        print("✅ PASSED")

        # Test 2: Select columns
        print("Test 2: Select columns", end=" ... ")
        selected = df.select("x")
        assert selected.shape() == (3, 1)
        print("✅ PASSED")

        # Test 3: Filter
        print("Test 3: Filter rows", end=" ... ")
        filtered = df.filter(df["x"] > 1)
        assert filtered.shape() == (2, 2)
        print("✅ PASSED")

        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False

def verify_read_parquet():
    """Verify Parquet reading works."""
    print("\n" + "=" * 70)
    print("Step 3: Verifying Parquet Read")
    print("=" * 70)

    try:
        import daft

        parquet_path = Path(__file__).parent.parent / "test_data.parquet"
        if not parquet_path.exists():
            print(f"⏭️  Test parquet not found: {parquet_path}")
            return None

        print(f"\nReading: {parquet_path}", end=" ... ")
        df = daft.read_parquet(str(parquet_path))
        print(f"✅ Read {df.shape()} rows")

        # Show schema
        print(f"   Columns: {df.column_names}")

        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False

def main():
    """Main verification function."""
    print("=" * 70)
    print("Daft Python Bindings Verification")
    print("=" * 70)

    results = []

    # Run verification steps
    results.append(("Import Daft", verify_daft_import()))
    results.append(("Basic Operations", verify_basic_operations()))
    results.append(("Parquet Read", verify_read_parquet()))

    # Summary
    print("\n" + "=" * 70)
    print("Verification Summary")
    print("=" * 70)

    for test, passed in results:
        if passed is True:
            print(f"  ✅ {test}: PASSED")
        elif passed is None:
            print(f"  ⏭️  {test}: SKIPPED")
        else:
            print(f"  ❌ {test}: FAILED")

    all_passed = all(r[1] in [True, None] for r in results)

    if all_passed:
        print("\n🎉 All verifications passed!")
        print("\nNext steps:")
        print("  1. Update Discussion.md with build results")
        print("  2. Run full E2E test with real Daft API")
        print("  3. Mark TASK-14 as completed")
        return True
    else:
        print("\n⚠️  Some verifications failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
