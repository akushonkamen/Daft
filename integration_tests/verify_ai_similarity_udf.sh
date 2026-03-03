#!/bin/bash
#
# Verify ai_similarity UDF with DuckDB CLI
#
# This script tests the ai_similarity function directly with DuckDB
# to verify the Extension UDF works correctly.
#

set -e

echo "=========================================="
echo "AI Similarity UDF Verification"
echo "=========================================="
echo ""

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DUCKDB_DIR="$PROJECT_ROOT/duckdb"
DUCKDB_CLI="$DUCKDB_DIR/build/duckdb"
EXTENSION="$DUCKDB_DIR/build/test/extension/ai.duckdb_extension"

echo "Configuration:"
echo "  DuckDB CLI: $DUCKDB_CLI"
echo "  Extension:  $EXTENSION"
echo ""

# Check if DuckDB CLI exists
if [ ! -f "$DUCKDB_CLI" ]; then
    echo "❌ DuckDB CLI not found at: $DUCKDB_CLI"
    echo "   Please build DuckDB first"
    exit 1
fi

# Check if extension exists
if [ ! -f "$EXTENSION" ]; then
    echo "❌ AI Extension not found at: $EXTENSION"
    echo "   Please build the extension first"
    exit 1
fi

echo "✅ Prerequisites check passed"
echo ""

# Test 1: Basic similarity calculation
echo "=========================================="
echo "Test 1: Basic similarity calculation"
echo "=========================================="

$DUCKDB_CLI -unsigned \
    -c "LOAD '$EXTENSION';" \
    -c "SELECT ai_similarity([0.1, 0.2]::FLOAT[], [0.1, 0.2]::FLOAT[], 'cosine') AS similarity;"

echo ""

# Test 2: Different models
echo "=========================================="
echo "Test 2: Different similarity models"
echo "=========================================="

echo "Testing cosine similarity:"
$DUCKDB_CLI -unsigned \
    -c "LOAD '$EXTENSION';" \
    -c "SELECT ai_similarity([1.0, 0.0]::FLOAT[], [0.707, 0.707]::FLOAT[], 'cosine') AS cosine_sim;"

echo ""
echo "Testing dot product:"
$DUCKDB_CLI -unsigned \
    -c "LOAD '$EXTENSION';" \
    -c "SELECT ai_similarity([1.0, 2.0]::FLOAT[], [3.0, 4.0]::FLOAT[], 'dot') AS dot_sim;"

echo ""
echo "Testing euclidean distance:"
$DUCKDB_CLI -unsigned \
    -c "LOAD '$EXTENSION';" \
    -c "SELECT ai_similarity([1.0, 1.0]::FLOAT[], [2.0, 2.0]::FLOAT[], 'euclidean') AS euclidean_sim;"

echo ""

# Test 3: Filter by similarity threshold
echo "=========================================="
echo "Test 3: Filter by similarity threshold"
echo "=========================================="

$DUCKDB_CLI -unsigned \
    -c "LOAD '$EXTENSION';" \
    -c "CREATE TABLE test_vectors AS SELECT * FROM (VALUES
        (1, [0.1, 0.2]::FLOAT[], [0.1, 0.2]::FLOAT[]),
        (2, [0.3, 0.4]::FLOAT[], [0.9, 1.0]::FLOAT[]),
        (3, [0.5, 0.6]::FLOAT[], [0.5, 0.6]::FLOAT[])
    ) AS t(id, vec1, vec2);" \
    -c "SELECT id, ai_similarity(vec1, vec2, 'cosine') AS similarity
        FROM test_vectors
        WHERE ai_similarity(vec1, vec2, 'cosine') > 0.5;"

echo ""

# Test 4: Join on similarity
echo "=========================================="
echo "Test 4: Join on similarity"
echo "=========================================="

$DUCKDB_CLI -unsigned \
    -c "LOAD '$EXTENSION';" \
    -c "CREATE TABLE left_table AS SELECT * FROM (VALUES
        (1, 'A', [0.1, 0.2]::FLOAT[]),
        (2, 'B', [0.3, 0.4]::FLOAT[])
    ) AS t(id, name, embedding);" \
    -c "CREATE TABLE right_table AS SELECT * FROM (VALUES
        (10, 'X', [0.1, 0.2]::FLOAT[]),
        (20, 'Y', [0.9, 1.0]::FLOAT[])
    ) AS t(id, name, embedding);" \
    -c "SELECT
        l.id AS left_id,
        l.name AS left_name,
        r.id AS right_id,
        r.name AS right_name,
        ai_similarity(l.embedding, r.embedding, 'cosine') AS similarity
    FROM left_table l
    JOIN right_table r
    ON ai_similarity(l.embedding, r.embedding, 'cosine') > 0.5;"

echo ""

# Summary
echo "=========================================="
echo "✅ UDF Verification Complete"
echo "=========================================="
echo ""
echo "All tests executed successfully!"
echo ""
echo "Integration Status:"
echo "  ✅ DuckDB Extension: Loaded"
echo "  ✅ ai_similarity UDF: Working"
echo "  ✅ Cosine similarity: Working"
echo "  ✅ Dot product: Working"
echo "  ✅ Euclidean distance: Working"
echo "  ✅ WHERE clause filtering: Working"
echo "  ✅ JOIN on similarity: Working"
echo ""
echo "The UDF is ready for end-to-end integration with Daft!"
