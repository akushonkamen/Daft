#!/usr/bin/env python3
"""
AI Similarity API Demonstration for Daft-DuckDB Integration.

This script demonstrates the usage of the ai_similarity function.
Note: This cannot be executed directly due to Daft compilation requirements,
but it shows the complete API usage.

When the environment is properly set up (Daft compiled), you can run:
    python3 examples/ai_similarity_demo.py
"""

# Example 1: Basic similarity calculation
# ======================================
from daft.functions import ai_similarity
import daft

# Load data with embeddings
df = daft.read_parquet("embeddings.parquet")

# Calculate similarity between two embedding columns
df_with_similarity = df.with_column(
    "similarity_score",
    ai_similarity(df["embedding_query"], df["embedding_candidate"], model="cosine")
)

# Filter based on similarity threshold
df_filtered = df_with_similarity.filter(
    daft.col("similarity_score") > 0.8
)


# Example 2: Join DataFrames based on semantic similarity
# =========================================================
df_left = daft.read_parquet("documents_a.parquet")
df_right = daft.read_parquet("documents_b.parquet")

# Join where embeddings are similar above threshold
df_joined = df_left.join(
    df_right,
    on=ai_similarity(
        df_left["embedding"],
        df_right["embedding"],
        model="cosine"
    ) > 0.8,
    how="inner"
)


# Example 3: Using different similarity metrics
# ==============================================
# Cosine similarity (default)
expr_cosine = ai_similarity(
    daft.col("vec1"),
    daft.col("vec2"),
    model="cosine"
)

# Dot product similarity
expr_dot = ai_similarity(
    daft.col("vec1"),
    daft.col("vec2"),
    model="dot"
)

# Euclidean distance similarity
expr_euclidean = ai_similarity(
    daft.col("vec1"),
    daft.col("vec2"),
    model="euclidean"
)


# Example 4: SQL Translation
# ============================
# The above expressions translate to DuckDB SQL:
# ai_similarity(vec1, vec2, 'cosine')
# ai_similarity(vec1, vec2, 'dot')
# ai_similarity(vec1, vec2, 'euclidean')

# Full query example:
"""
SELECT
    id,
    ai_similarity(embedding_a, embedding_b, 'cosine') AS similarity
FROM embeddings
WHERE ai_similarity(embedding_a, embedding_b, 'cosine') > 0.8
"""


# Example 5: Using string column names
# =====================================
# You can use string column names directly
expr1 = ai_similarity("embedding_left", "embedding_right")

# Or with column references
expr2 = ai_similarity(daft.col("embedding_left"), daft.col("embedding_right"))


# Example 6: Chaining with other operations
# ===========================================
df = daft.read_parquet("data.parquet")

# Add similarity column, then filter, then select
result = (
    df
    .with_column(
        "similarity",
        ai_similarity(df["query_vec"], df["candidate_vec"])
    )
    .filter(daft.col("similarity") > 0.7)
    .select("id", "name", "similarity")
)


# Verification Checklist
# ======================
"""
✅ API Design: ai_similarity(left_vec, right_vec, model="cosine")
✅ Supported models: "cosine", "dot", "euclidean"
✅ Default model: "cosine"
✅ Return type: Expression (Float64)
✅ SQL translation: ai_similarity(left, right, model)
✅ Metadata marker pattern: _is_ai_similarity attribute
✅ Compatible with filter, join, select operations
"""

print("AI Similarity API Demo")
print("=" * 70)
print("\nThis demo shows the complete API usage for ai_similarity.")
print("\nKey features:")
print("  - Calculate similarity between two vector embeddings")
print("  - Support for multiple similarity metrics (cosine, dot, euclidean)")
print("  - Can be used in filter, join, and select operations")
print("  - Integrates with DuckDB SQL translation")
print("\nNote: Run this script in an environment with compiled Daft to execute.")
