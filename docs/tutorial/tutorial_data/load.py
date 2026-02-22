"""Load tutorial datasets on the fly (generated in memory). Returns Polars DataFrames and ground truth."""

import polars as pl

from tutorial_data.prep import standardize_for_matching
from tutorial_data._generate import (
    generate_blocking_evaluation_data,
    generate_deduplication_data,
    generate_entity_resolution_data,
    generate_evaluation_test_data,
    generate_fuzzy_evaluation_data,
)


def _ground_truth_30() -> pl.DataFrame:
    """Standard 30-pair ground truth (eval_left_1..30, eval_right_1..30)."""
    return pl.DataFrame({
        "left_id": [f"eval_left_{i+1}" for i in range(30)],
        "right_id": [f"eval_right_{i+1}" for i in range(30)],
    })


def load_evaluation() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load evaluation set (50×50, 30 perfect pairs). Returns (left, right, ground_truth)."""
    left, right = generate_evaluation_test_data()
    return left, right, _ground_truth_30()


def load_fuzzy_evaluation() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load fuzzy evaluation set (50×50, 30 pairs; 15 identical, 15 name variants). Returns (left, right, ground_truth)."""
    left, right = generate_fuzzy_evaluation_data()
    return left, right, _ground_truth_30()


def load_blocking_evaluation() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load blocking evaluation set (50×50, 30 pairs; 15 same zip, 15 split zip). Returns (left, right, ground_truth)."""
    left, right = generate_blocking_evaluation_data()
    return left, right, _ground_truth_30()


def load_entity_resolution() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load entity resolution set **raw** (500×500, 30 true pairs; column name mismatches, nulls, messy values).
    Use for 01 Preparation to walk through cleaning. Returns (left, right, ground_truth).
    Ground truth: 30 pairs only (exact_name + email_adds_value + fuzzy_name). Same first name + different
    last name at same address (address_zip) are filler—not in ground truth, so Run C zip cascade yields false positives."""
    left, right, _, _, _, _ = generate_entity_resolution_data()
    ground_truth = pl.DataFrame({
        "left_id": [f"left_{i+1}" for i in range(30)],
        "right_id": [f"right_{i+1}" for i in range(30)],
    })
    return left, right, ground_truth


def load_entity_resolution_standardized() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load entity resolution set **already standardized** (schema aligned, email_clean, full_name).
    Use from 02 onward: raw data → standardize (01) → clean data → match. Returns (left, right, ground_truth)."""
    left, right, ground_truth = load_entity_resolution()
    left, right = standardize_for_matching(left, right)
    return left, right, ground_truth


def load_deduplication() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load deduplication set (1000 rows, 50 duplicate pairs). Returns (df, ground_truth)."""
    df, duplicate_groups = generate_deduplication_data()
    ground_truth_data = []
    for group in duplicate_groups:
        ids = group
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                ground_truth_data.append({"left_id": ids[i], "right_id": ids[j]})
    ground_truth = pl.DataFrame(ground_truth_data)
    return df, ground_truth
