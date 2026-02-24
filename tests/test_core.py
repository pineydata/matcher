"""Tests for core matching functionality."""

import polars as pl
import pytest
from matcher import Matcher, Deduplicator, MatchResults, FuzzyMatcher


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
    results = matcher.match(on="email")

    assert results.count == 1
    assert "email" in results.matches.columns


def test_match_missing_field():
    """Test that missing field raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

    with pytest.raises(ValueError, match="Field\\(s\\) .* not found in left source"):
        matcher.match(on="name")


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


def test_sample_n_negative_raises():
    """Test sample(n=...) raises when n is negative."""
    results = MatchResults(pl.DataFrame({"id": [1, 2, 3]}))
    with pytest.raises(ValueError, match="n must be non-negative"):
        results.sample(n=-1)


def test_sample_fraction_out_of_range_raises():
    """Test sample(fraction=...) raises when fraction is not in (0, 1]."""
    results = MatchResults(pl.DataFrame({"id": [1, 2, 3]}))
    with pytest.raises(ValueError, match="fraction must be"):
        results.sample(fraction=0)
    with pytest.raises(ValueError, match="fraction must be"):
        results.sample(fraction=1.5)


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
    results = matcher.match(on=["email", "zip_code"])

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
    results = deduplicator.match(on=["email", "zip_code"])

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
        matcher.match(on=["email", "zip_code"])


def test_match_multi_field_empty_list():
    """Test that empty rules list raises appropriate error."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

    with pytest.raises(ValueError, match="on must"):
        matcher.match(on=[])


def test_match_rules_entity_resolution():
    """Cascade: match first rule then refine with second (email then name for unmatched)."""
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
    results = matcher.match(on="email").refine(on=["first_name", "last_name"])

    # Should find:
    # - id=2 matches id=5 via email (b@test.com)
    # - id=3 matches id=6 via email (c@test.com)
    # - id=1 matches id=4 via name (Alice Smith)
    assert results.count == 3
    assert "email" in results.matches.columns
    assert "first_name" in results.matches.columns


def test_match_rules_deduplication():
    """Cascade: match email then refine with name for unmatched (deduplication)."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com"],
        "first_name": ["Alice", "Bob", "Alice", "Eve"],
        "last_name": ["Smith", "Jones", "Smith", "Frank"]
    })

    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(on="email").refine(on=["first_name", "last_name"])

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

    with pytest.raises(ValueError, match="on must"):
        matcher.match(on=[])


def test_match_rules_multiple_rules_raises():
    """Passing multiple rules (list of lists) raises; use .refine() for cascade."""
    left = pl.DataFrame({"id": [1], "email": ["a@x.com"], "name": ["A"]})
    right = pl.DataFrame({"id": [2], "email": ["a@x.com"], "name": ["A"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="Only a single rule"):
        matcher.match(on=[["email"], ["name"]])


def test_match_rules_invalid_rule():
    """Test that invalid rule format raises appropriate error."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"]})

    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

    with pytest.raises(ValueError, match="on must contain at least one field"):
        matcher.match(on=[[]])  # Empty rule

    # String rules are now valid
    results = matcher.match(on=["email"])
    assert results.count == 1


def test_match_rules_string_single_field():
    """Test that single rule can be string or list; cascade via refine."""
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
    results = matcher.match(on="email").refine(on=["first_name", "last_name"])
    assert results.count >= 1


# --- Refine (cascading match) ---


def test_refine_entity_resolution_adds_name_matches():
    """Refine: match on email first, then on name for unmatched left; combined count and IDs."""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "email": ["a@test.com", "nomatch@test.com", "nomatch2@test.com"],
        "first_name": ["Alice", "Bob", "Charlie"],
        "last_name": ["Smith", "Jones", "Brown"],
    })
    right = pl.DataFrame({
        "id": [10, 20, 30],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "first_name": ["Alice", "Bob", "Xavier"],
        "last_name": ["Smith", "Jones", "Y"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on="email")
    # Only (1, 10) match on email
    assert results.count == 1
    assert results.matches["id"].to_list() == [1]

    refined = results.refine(on=["first_name", "last_name"])
    # Original (1, 10) + refined: (2, 20) on name Bob Jones. Left id=3 has no name match.
    assert refined.count == 2
    left_ids = refined.matches["id"].to_list()
    right_ids = refined.matches["id_right"].to_list()
    assert 1 in left_ids and 2 in left_ids
    assert 10 in right_ids and 20 in right_ids
    # Unmatched left (id=3) got no new match
    assert 3 not in left_ids


def test_refine_deduplication_no_self_matches():
    """Refine with Deduplicator: combined result has no self-matches (id == id_right)."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "email": ["a@test.com", "other@test.com", "other2@test.com", "b@test.com"],
        "first_name": ["Alice", "Bob", "Bob", "Bob"],
        "last_name": ["Smith", "Jones", "Jones", "Jones"],
    })
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(on="email")
    # (1,2) or (2,1) if same email - we have only a@ and b@ unique, so maybe 0 dupes on email
    # Set up so email gives no/some matches, then refine on name gives (2,3), (2,4), (3,4) etc.
    results = deduplicator.match(on="email")
    refined = results.refine(on=["first_name", "last_name"])
    id_right = "id_right"
    assert id_right in refined.matches.columns
    self_matches = refined.matches.filter(pl.col("id") == pl.col(id_right))
    assert len(self_matches) == 0


def test_refine_all_matched_returns_same():
    """Refine when no unmatched left records returns same matches unchanged."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "b@test.com"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on="email")
    assert results.count == 2

    refined = results.refine(on=["email"])
    assert refined.count == 2
    assert refined.matches.height == results.matches.height
    # Same set of (id, id_right) pairs
    pairs_orig = set(zip(results.matches["id"].to_list(), results.matches["id_right"].to_list()))
    pairs_refined = set(zip(refined.matches["id"].to_list(), refined.matches["id_right"].to_list()))
    assert pairs_refined == pairs_orig


def test_refine_with_blocking_key():
    """Refine with blocking_key runs the rule only within blocks (same blocking_key value)."""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "email": ["a@test.com", "nomatch@test.com", "nomatch2@test.com"],
        "first_name": ["Alice", "Bob", "Bob"],
        "last_name": ["Smith", "Jones", "Jones"],
        "zip_code": ["10001", "10001", "10002"],
    })
    right = pl.DataFrame({
        "id": [10, 20, 30],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "first_name": ["Alice", "Bob", "Xavier"],
        "last_name": ["Smith", "Jones", "Y"],
        "zip_code": ["10001", "10001", "10002"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on="email")
    # Only (1, 10) match on email. Unmatched: left 2 and 3.
    assert results.count == 1
    # Refine on name within zip: in 10001 block (left 2, right 10, 20) -> (2, 20) Bob Jones. In 10002 (left 3, right 30) -> no name match.
    refined = results.refine(on=["first_name", "last_name"], blocking_key="zip_code")
    assert refined.count == 2
    left_ids = refined.matches["id"].to_list()
    assert 1 in left_ids and 2 in left_ids
    assert 3 not in left_ids


def test_refine_with_blocking_key_list():
    """refine(blocking_key=[...]) uses multiple keys; matches only within same (zip, state) block."""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "email": ["a@test.com", "nomatch@test.com", "nomatch2@test.com"],
        "first_name": ["Alice", "Bob", "Bob"],
        "last_name": ["Smith", "Jones", "Jones"],
        "zip_code": ["10001", "10001", "10001"],
        "state": ["NY", "NY", "CA"],
    })
    right = pl.DataFrame({
        "id": [10, 20, 30],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "first_name": ["Alice", "Bob", "Bob"],
        "last_name": ["Smith", "Jones", "Jones"],
        "zip_code": ["10001", "10001", "10001"],
        "state": ["NY", "CA", "CA"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on="email")
    # Unmatched: left 2 (10001, NY), 3 (10001, CA). Refine by name within (zip_code, state).
    # Block (10001, NY): left 2, right 10 only -> no name match. Block (10001, CA): left 3, right 20, 30 -> (3,20) and (3,30) Bob Jones.
    refined = results.refine(on=["first_name", "last_name"], blocking_key=["zip_code", "state"])
    assert refined.count == 3  # (1,10), (3,20), (3,30)
    assert 1 in refined.matches["id"].to_list() and 3 in refined.matches["id"].to_list()
    assert 2 not in refined.matches["id"].to_list()


def test_refine_raises_when_no_matcher_available():
    """Refine raises when MatchResults has no stored source and no matcher passed."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"]})
    matches = pl.DataFrame({"id": [1], "id_right": [2], "email": ["a@test.com"]})
    results = MatchResults(matches, original_left=left)  # no source= stored

    with pytest.raises(ValueError, match="refine\\(\\) requires a matcher"):
        results.refine(on=["email"])


def test_refine_raises_when_original_left_missing():
    """Refine raises when MatchResults was created without original_left."""
    matches = pl.DataFrame({"id": [1], "id_right": [2], "email": ["a@test.com"]})
    results = MatchResults(matches, original_left=None)
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

    with pytest.raises(ValueError, match="original left data not available"):
        results.refine(on=["email"], matcher=matcher)


def test_refine_raises_when_matches_lack_id_structure():
    """Refine raises when matches DataFrame lacks expected left_id or right_id_right."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
    right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "b@test.com"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    # MatchResults with matches that have id but no id_right (malformed)
    matches_no_right = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    results = MatchResults(matches_no_right, original_left=left)

    with pytest.raises(ValueError, match="Expected.*id.*and.*id_right"):
        results.refine(on=["email"], matcher=matcher)


# --- Fuzzy matching (match with FuzzyMatcher) ---


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
    results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))
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
    results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.80))
    assert results.count >= 1
    assert results.matches["confidence"][0] >= 0.80


def test_match_fuzzy_missing_field_left():
    """match with FuzzyMatcher raises when field is missing in left source."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"]})
    right = pl.DataFrame({"id": [2], "email": ["b@test.com"], "name": ["Bob"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="not found in left source"):
        matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))


def test_match_fuzzy_missing_field_right():
    """match with FuzzyMatcher raises when field is missing in right source."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"], "name": ["Alice"]})
    right = pl.DataFrame({"id": [2], "email": ["b@test.com"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="not found in right source"):
        matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))


def test_match_fuzzy_non_string_field_raises():
    """match with FuzzyMatcher raises when field is not a string (Utf8) column."""
    left = pl.DataFrame({"id": [1], "name": ["Alice"], "age": [30]})
    right = pl.DataFrame({"id": [2], "name": ["Bob"], "age": [25]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="must be string.*Utf8"):
        matcher.match(on=["age"], matching_algorithm=FuzzyMatcher(threshold=0.85))


def test_match_fuzzy_threshold_validation():
    """FuzzyMatcher raises when threshold not in [0, 1]."""
    left = pl.DataFrame({"id": [1], "name": ["a"]})
    right = pl.DataFrame({"id": [2], "name": ["b"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="threshold must be between 0 and 1"):
        matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=1.5))
    with pytest.raises(ValueError, match="threshold must be between 0 and 1"):
        matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=-0.1))


def test_match_fuzzy_high_threshold_fewer_matches():
    """Higher threshold yields fewer matches."""
    left = pl.DataFrame({"id": [1], "name": ["Alice"]})
    right = pl.DataFrame({"id": [2], "name": ["Alicia"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    low = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.5))
    high = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.99))
    assert low.count >= high.count


def test_match_fuzzy_nulls_excluded():
    """Fuzzy matching excludes rows where the field is null (same as exact match)."""
    left = pl.DataFrame({"id": [1, 2], "name": ["Alice", None]})
    right = pl.DataFrame({"id": [3, 4], "name": ["Alicia", "Bob"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.5))
    # Row with null on left should not produce matches; only (1,3) can match
    left_ids_in_results = results.matches["id"].to_list()
    assert 2 not in left_ids_in_results, "Left row with null name should be excluded from matching"


def test_match_fuzzy_empty_when_no_matches():
    """match with FuzzyMatcher returns empty MatchResults when no pairs above threshold."""
    left = pl.DataFrame({"id": [1], "name": ["xyzabc"]})
    right = pl.DataFrame({"id": [2], "name": ["qqq"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.99))
    assert results.count == 0
    assert "confidence" in results.matches.columns


def test_deduplicator_match_fuzzy():
    """Deduplicator.match with FuzzyMatcher finds fuzzy duplicates and filters self-matches."""
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice Smith", "Alicia Smith", "Bob Jones"],  # 1 and 2 are fuzzy dupes
    })
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.80))
    assert results.count >= 1
    assert "confidence" in results.matches.columns
    # No self-matches (id must not equal id_right)
    id_right = "id_right"
    assert id_right in results.matches.columns
    self_matches = results.matches.filter(pl.col("id") == pl.col(id_right))
    assert len(self_matches) == 0


# --- Blocking (Phase 2) ---


def test_match_with_blocking_key_same_results():
    """With blocking_key, same matches found when blocks align with match keys."""
    left = pl.DataFrame({
        "id": [1, 2],
        "email": ["a@test.com", "b@test.com"],
        "zip_code": ["10001", "10002"],
    })
    right = pl.DataFrame({
        "id": [3, 4],
        "email": ["a@test.com", "c@test.com"],
        "zip_code": ["10001", "10002"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    without = matcher.match(on="email")
    with_blocking = matcher.match(on="email", blocking_key="zip_code")
    assert without.count == with_blocking.count == 1
    assert without.matches["id"].to_list() == with_blocking.matches["id"].to_list()
    assert without.matches["id_right"].to_list() == with_blocking.matches["id_right"].to_list()


def test_match_with_blocking_key_no_common_blocks():
    """When left and right share no blocking key value, no matches."""
    left = pl.DataFrame({
        "id": [1, 2],
        "email": ["a@test.com", "b@test.com"],
        "zip_code": ["10001", "10002"],
    })
    right = pl.DataFrame({
        "id": [3, 4],
        "email": ["a@test.com", "b@test.com"],
        "zip_code": ["20001", "20002"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on="email", blocking_key="zip_code")
    assert results.count == 0


def test_match_blocking_key_multiple_keys():
    """blocking_key as list restricts to same (zip_code, state) block."""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "zip_code": ["10001", "10001", "10002"],
        "state": ["NY", "CA", "NY"],
    })
    right = pl.DataFrame({
        "id": [4, 5, 6],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "zip_code": ["10001", "10001", "10002"],
        "state": ["NY", "NY", "NY"],  # (10001, CA) only on left; (10001, NY) in both
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    # Block on (zip_code, state): (10001, NY) and (10001, CA) and (10002, NY)
    # Same-email pairs: (1,4) in (10001,NY), (2,5) in (10001,NY) but 2 is CA/5 is NY so no; (3,6) in (10002,NY)
    results = matcher.match(on="email", blocking_key=["zip_code", "state"])
    assert results.count == 2  # (1,4) and (3,6); (2,5) different state within 10001
    ids_left = sorted(results.matches["id"].to_list())
    ids_right = sorted(results.matches["id_right"].to_list())
    assert ids_left == [1, 3]
    assert ids_right == [4, 6]


def test_match_blocking_key_empty_list_raises():
    """blocking_key=[] raises; must be non-empty when provided."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"], "zip_code": ["1"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"], "zip_code": ["1"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="blocking_key must be non-empty"):
        matcher.match(on="email", blocking_key=[])


def test_match_blocking_key_list_missing_column_raises():
    """When blocking_key is a list, missing column in right is reported."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"], "zip_code": ["1"], "state": ["NY"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"], "zip_code": ["1"]})  # no state
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="not found in right source"):
        matcher.match(on="email", blocking_key=["zip_code", "state"])


def test_match_blocking_key_missing_raises():
    """blocking_key column must exist in both left and right."""
    left = pl.DataFrame({"id": [1], "email": ["a@test.com"], "zip_code": ["1"]})
    right = pl.DataFrame({"id": [2], "email": ["a@test.com"]})  # no zip_code
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="not found in right source"):
        matcher.match(on="email", blocking_key="zip_code")


def test_match_blocking_key_nulls_form_one_block():
    """Nulls in blocking_key form one block; matches found within that block."""
    left = pl.DataFrame({
        "id": [1, 2],
        "email": ["a@test.com", "b@test.com"],
        "zip_code": [None, "10002"],
    })
    right = pl.DataFrame({
        "id": [3, 4],
        "email": ["a@test.com", "c@test.com"],
        "zip_code": [None, "10002"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on="email", blocking_key="zip_code")
    # Null block: (1,3) match on email. 10002 block: (2,4) no email match.
    assert results.count == 1
    assert results.matches["id"].to_list() == [1]
    assert results.matches["id_right"].to_list() == [3]


def test_match_blocking_key_per_rule():
    """Different blocking key per rule: rule 0 blocks by zip, rule 1 by state."""
    # Left 1 (Alice, 10001, NY), 2 (Bob, 10002, CA). Right 3 (Alice, 10002, NY), 4 (Carol, 10001, CA).
    # Same zip: (1,4) and (2,3) - no email/name match. Same state: (1,3) NY, (2,4) CA - (1,3) match on name.
    left = pl.DataFrame({
        "id": [1, 2],
        "email": ["a@x.com", "b@x.com"],
        "name": ["Alice", "Bob"],
        "zip_code": ["10001", "10002"],
        "state": ["NY", "CA"],
    })
    right = pl.DataFrame({
        "id": [3, 4],
        "email": ["a@x.com", "c@x.com"],
        "name": ["Alice", "Carol"],
        "zip_code": ["10002", "10001"],
        "state": ["NY", "CA"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    # Cascade: email within zip (no match in same zip), then name within state → (1,3) match
    per_rule = matcher.match(on="email", blocking_key="zip_code").refine(
        on=["name"], blocking_key="state"
    )
    assert per_rule.count == 1
    assert per_rule.matches["id"].to_list() == [1]
    assert per_rule.matches["id_right"].to_list() == [3]
    # Single blocking by zip for both: no pair in same zip has matching email or name
    by_zip = matcher.match(on="email", blocking_key="zip_code").refine(
        on=["name"], blocking_key="zip_code"
    )
    assert by_zip.count == 0


def test_match_blocking_key_list_length_must_match_rules():
    """Multiple rules (list of lists) raises; use .refine() for cascade."""
    left = pl.DataFrame({"id": [1], "email": ["a@x.com"], "name": ["A"], "zip_code": ["1"]})
    right = pl.DataFrame({"id": [2], "email": ["a@x.com"], "name": ["A"], "zip_code": ["1"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    with pytest.raises(ValueError, match="Only a single rule"):
        matcher.match(on=[["email"], ["name"]])  # multiple rules not allowed


def test_match_blocking_key_same_key_for_two_rules():
    """Cascade with same blocking key for both rules."""
    left = pl.DataFrame({
        "id": [1, 2],
        "email": ["a@x.com", "b@x.com"],
        "name": ["Alice", "Bob"],
        "zip_code": ["10001", "10002"],
    })
    right = pl.DataFrame({
        "id": [3, 4],
        "email": ["a@x.com", "c@x.com"],
        "name": ["Alice", "Bob"],
        "zip_code": ["10001", "10002"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on="email", blocking_key="zip_code").refine(
        on=["name"], blocking_key="zip_code"
    )
    assert results.count >= 1


def test_match_blocking_key_per_rule_none_for_one_rule():
    """First rule with blocking, refine with name (no blocking)."""
    left = pl.DataFrame({
        "id": [1, 2],
        "email": ["a@x.com", "b@x.com"],
        "name": ["Alice", "Bob"],
        "zip_code": ["10001", "10002"],
    })
    right = pl.DataFrame({
        "id": [3, 4],
        "email": ["a@x.com", "c@x.com"],
        "name": ["Alice", "Bob"],
        "zip_code": ["10002", "10001"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(on="email", blocking_key="zip_code").refine(on=["name"])
    assert results.count == 2
    ids = sorted(results.matches["id"].to_list())
    assert ids == [1, 2]
    ids_right = sorted(results.matches["id_right"].to_list())
    assert ids_right == [3, 4]


def test_match_fuzzy_with_blocking_key():
    """match_fuzzy with blocking_key finds matches only within blocks."""
    left = pl.DataFrame({
        "id": [1, 2],
        "name": ["Alice Smith", "Bob Jones"],
        "zip_code": ["10001", "10002"],
    })
    right = pl.DataFrame({
        "id": [3, 4, 5],
        "name": ["Alicia Smith", "Bob Jons", "Charlie Brown"],
        "zip_code": ["10001", "10002", "10002"],
    })
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    results = matcher.match(
        on=["name"],
        matching_algorithm=FuzzyMatcher(threshold=0.80),
        blocking_key="zip_code",
    )
    # (1,3) same block 10001; (2,4) and (2,5) same block 10002. No cross-block pairs.
    assert results.count >= 1
    assert "confidence" in results.matches.columns
    # All matches must share zip_code
    for row in results.matches.iter_rows(named=True):
        assert row["zip_code"] == row["zip_code_right"]


def test_match_fuzzy_blocking_same_as_no_blocking_when_one_block():
    """Fuzzy with one block gives same count as no blocking (sanity check)."""
    left = pl.DataFrame({"id": [1], "name": ["Alice"], "z": ["same"]})
    right = pl.DataFrame({"id": [2], "name": ["Alicia"], "z": ["same"]})
    matcher = Matcher(left=left, right=right, left_id="id", right_id="id")
    no_block = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.5))
    with_block = matcher.match(
        on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.5), blocking_key="z"
    )
    assert no_block.count == with_block.count


def test_deduplicator_match_with_blocking_key_list():
    """Deduplicator.match with blocking_key as list works and filters self-matches."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "email": ["a@test.com", "a@test.com", "b@test.com", "b@test.com", "c@test.com"],
        "zip_code": ["10001", "10001", "10002", "10002", "10002"],
        "state": ["NY", "NY", "NY", "NY", "NY"],
    })
    deduplicator = Deduplicator(source=df, id_col="id")
    # Block by (zip_code, state): (10001, NY) has 1,2 same email; (10002, NY) has 3,4 same email. Pairs (1,2),(2,1),(3,4),(4,3).
    results = deduplicator.match(on="email", blocking_key=["zip_code", "state"])
    assert results.count == 4  # (1,2), (2,1), (3,4), (4,3); no self-matches
    for row in results.matches.iter_rows(named=True):
        assert row["id"] != row["id_right"]


def test_deduplicator_match_with_blocking_key():
    """Deduplicator.match with blocking_key delegates and filters self-matches."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "email": ["a@test.com", "a@test.com", "b@test.com", "c@test.com"],
        "zip_code": ["10001", "10001", "10002", "10002"],
    })
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(on="email", blocking_key="zip_code")
    # Duplicates: (1,2) and (2,1) in block 10001; only one pair after self-filter
    assert results.count >= 1
    id_right = "id_right"
    self_matches = results.matches.filter(pl.col("id") == pl.col(id_right))
    assert len(self_matches) == 0


def test_deduplicator_match_fuzzy_with_blocking_key():
    """Deduplicator.match with FuzzyMatcher and blocking_key works and filters self-matches."""
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice Smith", "Alicia Smith", "Bob Jones"],
        "zip_code": ["10001", "10001", "10002"],
    })
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(
        on=["name"],
        matching_algorithm=FuzzyMatcher(threshold=0.80),
        blocking_key="zip_code",
    )
    assert results.count >= 1
    assert "confidence" in results.matches.columns
    self_matches = results.matches.filter(pl.col("id") == pl.col("id_right"))
    assert len(self_matches) == 0
