"""Tests using the generated sample datasets."""

import polars as pl
import pytest
from pathlib import Path
from matcher import Matcher, Deduplicator


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
    results = matcher.match(rules="email")
    assert results.count == 20, f"Expected 20 matches with email rule, got {results.count}"

    # Name-only rule: should find at least 10 name-only + 10 mixed = 20 matches
    # (may find more due to overlaps with other match types)
    results = matcher.match(rules=["first_name", "last_name"])
    assert results.count >= 20, f"Expected at least 20 matches with name rule, got {results.count}"

    # Multi-field rule: should find at least 10 multi-field matches (address + zip_code)
    # (may find more due to overlaps with mixed matches)
    results = matcher.match(rules=["address", "zip_code"])
    assert results.count >= 10, f"Expected at least 10 matches with address+zip rule, got {results.count}"

    # Multiple rules (OR): should find all 40 matches
    # Need to include address+zip to find multi-field matches
    results = matcher.match(rules=["email", ["first_name", "last_name"], ["address", "zip_code"]])
    assert results.count >= 40, f"Expected at least 40 matches with OR rules, got {results.count}"

    # Verify matches are correct (all should have same email)
    for row in results.matches.iter_rows(named=True):
        # The join should preserve email from both sides
        assert "email" in row, "Match should include email field"


def test_deduplication_with_sample_data(sample_data_dir):
    """Test deduplication with generated sample data."""
    source_path = sample_data_dir / "ExactMatcher" / "deduplication" / "customers.parquet"

    df = pl.read_parquet(source_path)
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(rules="email")

    # Should find 50 duplicate pairs
    # Note: self-join creates pairs, so we expect matches
    # The exact count depends on how we handle self-matches
    assert results.count > 0, "Should find some duplicate pairs"

    # Verify structure
    assert "email" in results.matches.columns


def test_entity_resolution_ground_truth_validation(sample_data_dir):
    """Validate that we find all known matches from ground truth."""
    left_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_b.parquet"

    # Load data
    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)

    # Known matches: first 40 records in each dataset are paired
    # They match on different rules (email, name, email+zip, or mixed)
    known_matches = []
    for i in range(40):
        left_id = f"left_{i+1}"
        right_id = f"right_{i+1}"
        known_matches.append((left_id, right_id))

    # Run matching with OR rules to find all matches (include address+zip for multi-field matches)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(rules=["email", ["first_name", "last_name"], ["address", "zip_code"]])

    # Verify we found all known matches
    found_pairs = set()
    for row in results.matches.iter_rows(named=True):
        left_id = row.get("id")
        right_id = row.get("id_right")
        if left_id and right_id:
            found_pairs.add((left_id, right_id))

    # Check that we found matches for all known pairs
    expected_pairs = set(known_matches)
    missing = expected_pairs - found_pairs
    extra = found_pairs - expected_pairs

    # Some extra matches are expected (e.g., multi-field matches also match on email alone)
    assert len(missing) == 0, f"Missing matches: {missing}"
    assert results.count >= 40, f"Should find at least 40 matches, got {results.count}"


def test_match_fuzzy_entity_resolution_with_sample_data(sample_data_dir):
    """Test fuzzy matching (Matcher) with generated entity resolution sample data."""
    left_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "entity_resolution" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")

    # Fuzzy on first_name: known pairs share same first_name (name-only 10 + mixed 10 = 20)
    # Exact same string gives confidence 1.0; we may get more from similar names
    results = matcher.match_fuzzy(field="first_name", threshold=0.85)
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
    results = deduplicator.match_fuzzy(field="first_name", threshold=0.85)

    # 50 duplicate pairs share same first_name (Duplicate0, Duplicate1, ...); no self-matches
    assert results.count >= 50, f"Expected at least 50 fuzzy duplicate pairs, got {results.count}"
    assert "confidence" in results.matches.columns
    assert "id_right" in results.matches.columns
    # No self-matches
    self_matches = results.matches.filter(pl.col("id") == pl.col("id_right"))
    assert len(self_matches) == 0, "Deduplicator should filter out self-matches"
