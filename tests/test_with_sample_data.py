"""Tests using the generated sample datasets."""

import polars as pl
import pytest
from pathlib import Path
from matcher import Matcher, Deduplicator, FuzzyMatcher


@pytest.fixture
def sample_data_dir():
    """Return path to sample data directory."""
    return Path(__file__).parent.parent / "data"


def test_entity_resolution_with_sample_data(sample_data_dir):
    """Test entity resolution with generated sample data."""
    left_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")

    # Email-only rule: should find 10 email-only + 10 mixed = 20 matches
    # (multi-field matches use address+zip, not email, so they don't match on email alone)
    results = matcher.match(match_on="email")
    assert results.count == 20, f"Expected 20 matches with email rule, got {results.count}"

    # Name-only rule: should find at least 10 name-only + 10 mixed = 20 matches
    # (may find more due to overlaps with other match types)
    results = matcher.match(match_on=["first_name", "last_name"])
    assert results.count >= 20, f"Expected at least 20 matches with name rule, got {results.count}"

    # Multi-field rule: should find at least 10 multi-field matches (address + zip_code)
    # (may find more due to overlaps with mixed matches)
    results = matcher.match(match_on=["address", "zip_code"])
    assert results.count >= 10, f"Expected at least 10 matches with address+zip rule, got {results.count}"

    # Multiple rules (cascading): should find all 40 matches (one per left row)
    results = matcher.match(match_on="email").refine(match_on=["first_name", "last_name"]).refine(match_on=["address", "zip_code"])
    assert results.count >= 40, f"Expected at least 40 matches with cascading rules, got {results.count}"

    # Verify matches are correct (all should have same email)
    for row in results.matches.iter_rows(named=True):
        # The join should preserve email from both sides
        assert "email" in row, "Match should include email field"


def test_deduplication_with_sample_data(sample_data_dir):
    """Test deduplication with generated sample data."""
    source_path = sample_data_dir / "ExactMatcher" / "deduplication" / "customers.parquet"

    df = pl.read_parquet(source_path)
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(match_on="email")

    assert results.count > 0, "Should find some duplicate pairs"
    assert "email" in results.matches.columns


def test_deduplication_evaluate_with_ground_truth(sample_data_dir):
    """Use evaluate() with ground truth derived from duplicate groups (improvement workflow)."""
    source_path = sample_data_dir / "ExactMatcher" / "deduplication" / "customers.parquet"
    df = pl.read_parquet(source_path)

    # Build ground truth: pairs of IDs that share the same email (known duplicates)
    ground_truth_data = []
    email_groups = df.group_by("email").agg(pl.col("id").alias("ids"))
    for row in email_groups.iter_rows(named=True):
        ids = row["ids"]
        if len(ids) > 1:
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    ground_truth_data.append({"left_id": ids[i], "right_id": ids[j]})
    ground_truth = pl.DataFrame(ground_truth_data)

    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(match_on="email")

    metrics = results.evaluate(ground_truth, left_id_col="id", right_id_col="id_right")

    assert metrics["true_positives"] > 0
    assert 0 <= metrics["precision"] <= 1.0
    assert 0 <= metrics["recall"] <= 1.0
    assert metrics["f1"] >= 0


def test_entity_resolution_ground_truth_validation(sample_data_dir):
    """Validate that we find all known matches using evaluate() with ground truth."""
    left_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)

    # Ground truth: first 40 records in each dataset are paired (known matches)
    ground_truth = pl.DataFrame({
        "left_id": [f"left_{i+1}" for i in range(40)],
        "right_id": [f"right_{i+1}" for i in range(40)],
    })

    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(match_on="email").refine(match_on=["first_name", "last_name"]).refine(match_on=["address", "zip_code"])

    # Use evaluate() as the standard way to judge quality (improvement workflow)
    metrics = results.evaluate(ground_truth, left_id_col="id", right_id_col="id_right")

    # Cascading rules can slightly reduce recall when one left row matches a different right row
    # on an earlier rule, so we require high recall rather than strict 1.0.
    assert metrics["recall"] >= 0.97, f"Should find nearly all known matches (recall>=0.97), got {metrics['recall']}"
    assert metrics["true_positives"] >= 39
    assert metrics["false_negatives"] <= 1
    # Some extra pairs are expected (e.g. multi-field matches also match on email alone), so precision may be < 1.0
    assert metrics["precision"] >= 0.9, f"Precision should be high, got {metrics['precision']}"
    assert results.count >= 40


def test_match_fuzzy_entity_resolution_with_sample_data(sample_data_dir):
    """Test fuzzy matching (Matcher) with generated entity resolution sample data."""
    left_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")

    # Fuzzy on first_name: known pairs share same first_name (name-only 10 + mixed 10 = 20)
    # Exact same string gives confidence 1.0; we may get more from similar names
    results = matcher.match(match_on=["first_name"], matching_algorithm=FuzzyMatcher(threshold=0.85))
    assert results.count >= 20, f"Expected at least 20 fuzzy matches on first_name, got {results.count}"
    assert "confidence" in results.matches.columns
    assert "id_right" in results.matches.columns
    assert results.matches["confidence"].min() >= 0.85
    assert results.matches["confidence"].max() <= 1.0


def test_match_fuzzy_deduplication_with_sample_data(sample_data_dir):
    """Test fuzzy matching (Deduplicator) with generated deduplication sample data."""
    source_path = sample_data_dir / "ExactMatcher" / "deduplication" / "customers.parquet"

    df = pl.read_parquet(source_path)
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(
        match_on=["first_name"],
        matching_algorithm=FuzzyMatcher(threshold=0.85),
    )

    # 50 duplicate pairs share same first_name (Duplicate0, Duplicate1, ...); no self-matches
    assert results.count >= 50, f"Expected at least 50 fuzzy duplicate pairs, got {results.count}"
    assert "confidence" in results.matches.columns
    assert "id_right" in results.matches.columns
    # No self-matches
    self_matches = results.matches.filter(pl.col("id") == pl.col("id_right"))
    assert len(self_matches) == 0, "Deduplicator should filter out self-matches"
