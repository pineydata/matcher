"""Tests for core matching functionality."""

import polars as pl
import pytest
from matcher import Matcher, MatchResults


def test_matcher_initialization_with_dataframes():
    """Test Matcher initialization with Polars DataFrames."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right)
    # DataFrames stay as DataFrames (in-memory only)
    assert isinstance(matcher.left, pl.DataFrame)
    assert isinstance(matcher.right, pl.DataFrame)
    # Verify data
    assert matcher.left.height == 2
    assert matcher.right.height == 2


def test_match_entity_resolution():
    """Test exact matching for entity resolution."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right)
    results = matcher.match(rules="email")

    assert results.count == 1
    assert "email" in results.matches.columns


def test_match_missing_field():
    """Test that missing field raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right)

    with pytest.raises(ValueError, match="Field\\(s\\) .* not found in left source"):
        matcher.match(rules="name")


def test_match_results_count():
    """Test MatchResults count property."""
    matches = pl.DataFrame({"id": [1, 2, 3]})
    results = MatchResults(matches)

    assert results.count == 3


def test_match_multi_field_entity_resolution():
    """Test exact matching with multiple fields for entity resolution."""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "zip_code": ["10001", "10002", "10001"]
    })
    right = pl.DataFrame({
        "id": [4, 5, 6],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "zip_code": ["10001", "10003", "10001"]  # Only first and third match on both fields
    })

    matcher = Matcher(left=left, right=right)
    results = matcher.match(rules=["email", "zip_code"])

    # Should find 2 matches: (a@test.com, 10001) and (c@test.com, 10001)
    # b@test.com doesn't match because zip codes differ (10002 vs 10003)
    assert results.count == 2
    assert "email" in results.matches.columns
    assert "zip_code" in results.matches.columns


def test_match_multi_field_deduplication():
    """Test exact matching with multiple fields for deduplication."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "email": ["a@test.com", "a@test.com", "b@test.com", "b@test.com"],
        "zip_code": ["10001", "10001", "10002", "10003"]  # Only first two match on both fields
    })

    matcher = Matcher(left=df)
    results = matcher.match(rules=["email", "zip_code"])

    # Should find 1 duplicate pair: records 1 and 2 match on both email and zip_code
    # Records 3 and 4 have same email but different zip_code, so they don't match
    assert results.count == 2  # Self-join creates both directions
    assert "email" in results.matches.columns
    assert "zip_code" in results.matches.columns


def test_match_multi_field_missing_field():
    """Test that missing field in multi-field matching raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"], "zip_code": ["10001", "10002"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})  # Missing zip_code

    matcher = Matcher(left=left, right=right)

    with pytest.raises(ValueError, match="Field\\(s\\) .* not found in right source"):
        matcher.match(rules=["email", "zip_code"])


def test_match_multi_field_empty_list():
    """Test that empty rules list raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right)

    with pytest.raises(ValueError, match="Rules must be"):
        matcher.match(rules=[])


def test_match_rules_entity_resolution():
    """Test rule-based matching for entity resolution (OR logic)."""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "first_name": ["Alice", "Bob", "Charlie"],
        "last_name": ["Smith", "Jones", "Brown"]
    })
    right = pl.DataFrame({
        "id": [4, 5, 6],
        "email": ["x@test.com", "b@test.com", "c@test.com"],
        "first_name": ["Alice", "Xavier", "Charlie"],
        "last_name": ["Smith", "Y", "Brown"]
    })

    matcher = Matcher(left=left, right=right)
    # Match if email OR (first_name AND last_name)
    results = matcher.match(rules=[
        ["email"],
        ["first_name", "last_name"]
    ])

    # Should find:
    # - id=2 matches id=5 via email (b@test.com)
    # - id=3 matches id=6 via email (c@test.com)
    # - id=1 matches id=4 via name (Alice Smith)
    assert results.count == 3
    assert "email" in results.matches.columns
    assert "first_name" in results.matches.columns


def test_match_rules_deduplication():
    """Test rule-based matching for deduplication (OR logic)."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com"],
        "first_name": ["Alice", "Bob", "Alice", "Eve"],
        "last_name": ["Smith", "Jones", "Smith", "Frank"]
    })

    matcher = Matcher(left=df)
    # Match if email OR (first_name AND last_name)
    results = matcher.match(rules=[
        ["email"],
        ["first_name", "last_name"]
    ])

    # Should find:
    # - id=1 and id=3 match via name (Alice Smith)
    # Note: email rule won't find duplicates since all emails are unique
    assert results.count >= 2  # Self-join creates both directions
    assert "email" in results.matches.columns




def test_match_rules_empty_list():
    """Test that empty rules list raises appropriate error."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"]})

    matcher = Matcher(left=left, right=right)

    with pytest.raises(ValueError, match="Rules must be"):
        matcher.match(rules=[])


def test_match_rules_invalid_rule():
    """Test that invalid rule format raises appropriate error."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"]})

    matcher = Matcher(left=left, right=right)

    with pytest.raises(ValueError, match="Each rule must contain at least one field"):
        matcher.match(rules=[[]])  # Empty rule

    # String rules are now valid
    results = matcher.match(rules=["email"])
    assert results.count == 1


def test_match_rules_string_single_field():
    """Test that single-field rules can be strings."""
    left = pl.DataFrame({
        "id": [1, 2],
        "email": ["a@test.com", "b@test.com"],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Smith", "Jones"]
    })
    right = pl.DataFrame({
        "id": [3, 4],
        "email": ["a@test.com", "x@test.com"],
        "first_name": ["Alice", "Charlie"],
        "last_name": ["Smith", "Brown"]
    })

    matcher = Matcher(left=left, right=right)
    # Mix of string and list rules
    results = matcher.match(rules=[
        "email",  # string for single field
        ["first_name", "last_name"]  # list for multiple fields
    ])

    # Should find matches from both rules
    assert results.count >= 1
