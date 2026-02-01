"""Tests for core matching functionality."""

import polars as pl
import pytest
from matcher import Matcher, MatchResults


def test_matcher_initialization_with_dataframes():
    """Test Matcher initialization with Polars DataFrames."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left_source=left, right_source=right)
    assert matcher.left.height == 2
    assert matcher.right.height == 2


def test_match_exact_entity_resolution():
    """Test exact matching for entity resolution."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left_source=left, right_source=right)
    results = matcher.match_exact(field="email")

    assert results.count == 1
    assert "email" in results.matches.columns


def test_match_exact_missing_field():
    """Test that missing field raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left_source=left, right_source=right)

    with pytest.raises(ValueError, match="Field 'name' not found"):
        matcher.match_exact(field="name")


def test_match_results_count():
    """Test MatchResults count property."""
    matches = pl.DataFrame({"id": [1, 2, 3]})
    results = MatchResults(matches)

    assert results.count == 3
