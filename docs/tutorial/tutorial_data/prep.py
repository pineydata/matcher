"""Standardization for matching: one place to turn raw entity-resolution data into clean tables.

Used by 01 (iterative steps then this function) and by later notebooks via
load_entity_resolution_standardized(). Best practice: raw data → standardize → clean data → match.
"""

import polars as pl


# Canonical column order and names for entity-resolution tables
EXPECTED_SCHEMA = [
    "id", "email", "first_name", "last_name",
    "address", "city", "state", "zip_code",
]


def standardize_for_matching(
    left: pl.DataFrame,
    right: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Standardize two tables for matching: align schema, add email_clean and full_name.

    - Renames right columns if needed (e.g. email_address → email, postal_code → zip_code).
    - Selects both to EXPECTED_SCHEMA order.
    - Adds email_clean (lowercase + strip) and full_name (first + last, stripped).
    Nulls are left as-is; handle them before or after as needed.

    Returns:
        (left_clean, right_clean) with same schema and derived columns.
    """
    right = right.clone()
    if "email_address" in right.columns:
        right = right.rename({"email_address": "email", "postal_code": "zip_code"})

    left = left.select(EXPECTED_SCHEMA)
    right = right.select(EXPECTED_SCHEMA)

    email_clean = (
        pl.col("email").cast(pl.Utf8).str.to_lowercase().str.strip_chars().alias("email_clean")
    )
    left = left.with_columns(email_clean)
    right = right.with_columns(email_clean)

    full_name_expr = (
        (pl.col("first_name").fill_null("") + " " + pl.col("last_name").fill_null(""))
        .str.strip_chars()
        .alias("full_name")
    )
    left = left.with_columns(full_name_expr)
    right = right.with_columns(full_name_expr)

    return left, right
