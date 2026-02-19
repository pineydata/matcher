"""Tests for core matching functionality."""

import polars as pl
import pytest
from matcher import Matcher, Deduplicator, MatchResults


def test_matcher_initialization_with_dataframes():
    """Test Matcher initialization with Polars DataFrames."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
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

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(rules="email")

    assert results.count == 1
    assert "email" in results.matches.columns


def test_match_missing_field():
    """Test that missing field raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

    with pytest.raises(ValueError, match="Field\\(s\\) .* not found in left source"):
        matcher.match(rules="name")


def test_match_results_count():
    """Test MatchResults count property."""
    matches = pl.DataFrame({"id": [1, 2, 3]})
    results = MatchResults(matches)

    assert results.count == 3


def test_export_for_review(tmp_path):
    """Test export_for_review writes CSV and preserves matches."""
    matches = pl.DataFrame({
        "id": [1, 2],
        "id_right": [3, 4],
        "confidence": [0.95, 0.88],
        "name": ["Alice", "Bob"],
        "name_right": ["A. Smith", "B. Jones"],
    })
    results = MatchResults(matches)
    path = tmp_path / "matches_for_review.csv"

    results.export_for_review(path)

    assert path.exists()
    back = pl.read_csv(path)
    assert back.shape == matches.shape
    assert back.columns == matches.columns
    # CSV round-trip may change dtypes; check key values
    assert back["id"].to_list() == [1, 2]
    assert back["name"].to_list() == ["Alice", "Bob"]


def test_sample_n():
    """Test sample(n) returns MatchResults with n rows (or all if n >= count)."""
    matches = pl.DataFrame({"id": [1, 2, 3, 4, 5], "x": [10, 20, 30, 40, 50]})
    results = MatchResults(matches)

    sampled = results.sample(n=3, seed=42)
    assert sampled.count == 3
    assert sampled.matches.height == 3

    all_rows = results.sample(n=10, seed=0)
    assert all_rows.count == 5


def test_sample_fraction():
    """Test sample(fraction) returns MatchResults with proportion of rows."""
    matches = pl.DataFrame({"id": list(range(100)), "x": list(range(100))})
    results = MatchResults(matches)

    sampled = results.sample(fraction=0.2, seed=123)
    assert sampled.count == 20


def test_sample_requires_n_or_fraction():
    """Test sample() raises if neither n nor fraction given."""
    results = MatchResults(pl.DataFrame({"id": [1, 2, 3]}))
    with pytest.raises(ValueError, match="Provide either n"):
        results.sample()
    with pytest.raises(ValueError, match="not both"):
        results.sample(n=2, fraction=0.5)


def test_sample_empty_matches():
    """Test sample on empty matches returns empty MatchResults."""
    results = MatchResults(pl.DataFrame({"id": [], "x": []}))
    sampled = results.sample(n=5, seed=0)
    assert sampled.count == 0


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

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
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

    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(rules=["email", "zip_code"])

    # Should find 1 duplicate pair: records 1 and 2 match on both email and zip_code
    # Records 3 and 4 have same email but different zip_code, so they don't match
    assert results.count == 2  # Self-join creates both directions
    assert "email" in results.matches.columns
    assert "zip_code" in results.matches.columns


def test_match_multi_field_missing_field():
    """Test that missing field in multi-field matching raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"], "zip_code": ["10001", "10002"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})  # Missing zip_code

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

    with pytest.raises(ValueError, match="Field\\(s\\) .* not found in right source"):
        matcher.match(rules=["email", "zip_code"])


def test_match_multi_field_empty_list():
    """Test that empty rules list raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

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

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
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

    deduplicator = Deduplicator(source=df, id_col="id")
    # Match if email OR (first_name AND last_name)
    results = deduplicator.match(rules=[
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

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

    with pytest.raises(ValueError, match="Rules must be"):
        matcher.match(rules=[])


def test_match_rules_invalid_rule():
    """Test that invalid rule format raises appropriate error."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"]})

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

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

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    # Mix of string and list rules
    results = matcher.match(rules=[
        "email",  # string for single field
        ["first_name", "last_name"]  # list for multiple fields
    ])

    # Should find matches from both rules
    assert results.count >= 1


# --- match_fuzzy (Phase 3) ---


def test_match_fuzzy_basic():
    """Fuzzy matching finds similar strings above threshold."""
    left = pl.DataFrame({
        "id": [1, 2],
        "name": ["John Smith", "Jane Doe"],
    })
    right = pl.DataFrame({
        "id": [10, 20],
        "name": ["John Smith", "J. Smith"],  # typo/variant
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match_fuzzy(field="name", threshold=0.85)
    assert results.count >= 1
    assert "confidence" in results.matches.columns
    assert "id" in results.matches.columns
    assert "id_right" in results.matches.columns
    # Exact match should be present
    exact = results.matches.filter(pl.col("name") == pl.col("name_right"))
    assert len(exact) >= 1
    # Confidence in [0, 1]
    assert results.matches["confidence"].min() >= 0.85
    assert results.matches["confidence"].max() <= 1.0


def test_match_fuzzy_typos():
    """Fuzzy matching matches names with typos (Jaro-Winkler)."""
    left = pl.DataFrame({"id": [1], "name": ["Alice Johnson"]})
    right = pl.DataFrame({"id": [2], "name": ["Alicia Johnson"]})  # typo
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match_fuzzy(field="name", threshold=0.80)
    assert results.count >= 1
    assert results.matches["confidence"][0] >= 0.80


def test_match_fuzzy_missing_field_left():
    """match_fuzzy raises when field is missing in left source."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    right = pl.DataFrame({"id": [2], "email": ["b@test.com"], "name": ["Bob"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="not found in left source"):
        matcher.match_fuzzy(field="name", threshold=0.85)


def test_match_fuzzy_missing_field_right():
    """match_fuzzy raises when field is missing in right source."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"], "name": ["Alice"]})
    right = pl.DataFrame({"id": [2], "email": ["b@test.com"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="not found in right source"):
        matcher.match_fuzzy(field="name", threshold=0.85)


def test_match_fuzzy_non_string_field_raises():
    """match_fuzzy raises when field is not a string (Utf8) column."""
    left = pl.DataFrame({"id": [1], "name": ["Alice"], "age": [30]})
    right = pl.DataFrame({"id": [2], "name": ["Bob"], "age": [25]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="must be a string \\(Utf8\\) column"):
        matcher.match_fuzzy(field="age", threshold=0.85)


def test_match_fuzzy_threshold_validation():
    """match_fuzzy raises when threshold not in [0, 1]."""
    left = pl.DataFrame({"id": [1], "name": ["a"]})
    right = pl.DataFrame({"id": [2], "name": ["b"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="threshold must be between 0 and 1"):
        matcher.match_fuzzy(field="name", threshold=1.5)
    with pytest.raises(ValueError, match="threshold must be between 0 and 1"):
        matcher.match_fuzzy(field="name", threshold=-0.1)


def test_match_fuzzy_high_threshold_fewer_matches():
    """Higher threshold yields fewer matches."""
    left = pl.DataFrame({"id": [1], "name": ["Alice"]})
    right = pl.DataFrame({"id": [2], "name": ["Alicia"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    low = matcher.match_fuzzy(field="name", threshold=0.5)
    high = matcher.match_fuzzy(field="name", threshold=0.99)
    assert low.count >= high.count


def test_match_fuzzy_nulls_excluded():
    """match_fuzzy excludes rows where the field is null (same as exact match)."""
    left = pl.DataFrame({"id": [1, 2], "name": ["Alice", None]})
    right = pl.DataFrame({"id": [3, 4], "name": ["Alicia", "Bob"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match_fuzzy(field="name", threshold=0.5)
    # Row with null on left should not produce matches; only (1,3) can match
    left_ids_in_results = results.matches["id"].to_list()
    assert 2 not in left_ids_in_results, "Left row with null name should be excluded from matching"


def test_match_fuzzy_empty_when_no_matches():
    """match_fuzzy returns empty MatchResults when no pairs above threshold."""
    left = pl.DataFrame({"id": [1], "name": ["xyzabc"]})
    right = pl.DataFrame({"id": [2], "name": ["qqq"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match_fuzzy(field="name", threshold=0.99)
    assert results.count == 0
    assert "confidence" in results.matches.columns


def test_deduplicator_match_fuzzy():
    """Deduplicator.match_fuzzy finds fuzzy duplicates and filters self-matches."""
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice Smith", "Alicia Smith", "Bob Jones"],  # 1 and 2 are fuzzy dupes
    })
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match_fuzzy(field="name", threshold=0.80)
    assert results.count >= 1
    assert "confidence" in results.matches.columns
    # No self-matches (id must not equal id_right)
    id_right = "id_right"
    assert id_right in results.matches.columns
    self_matches = results.matches.filter(pl.col("id") == pl.col(id_right))
    assert len(self_matches) == 0
