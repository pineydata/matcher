"""Tests for BatchedMatcher and batches()."""

import polars as pl
import pytest

from matcher import BatchedMatcher, batches, ExactMatcher, FuzzyMatcher


def test_batches_dataframe():
    """batches(DataFrame) yields slices."""
    df = pl.DataFrame({"a": range(10), "b": range(10, 20)})
    out = list(batches(df, batch_size=3))
    assert len(out) == 4
    assert out[0].height == 3
    assert out[1].height == 3
    assert out[2].height == 3
    assert out[3].height == 1


def test_batches_lazyframe():
    """batches(LazyFrame) yields collected chunks."""
    df = pl.DataFrame({"a": range(10), "b": range(10, 20)})
    lf = df.lazy()
    out = list(batches(lf, batch_size=3))
    assert len(out) >= 1
    total = sum(b.height for b in out)
    assert total == 10


def test_batches_bad_size():
    with pytest.raises(ValueError, match="batch_size"):
        list(batches(pl.DataFrame(), batch_size=0))


def test_batched_matcher_single_batch_each():
    """Match with one left batch and one right batch (same as eager)."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@x.com", "b@x.com"]})
    right = pl.DataFrame({"id": [10, 11], "email": ["a@x.com", "c@x.com"]})
    b = BatchedMatcher(
        iter([left]), iter([right]), left_id="id", right_id="id"
    )
    results = b.match(match_on="email")
    assert results.count == 1
    assert results.matches["id"].to_list() == [1]
    assert results.matches["id_right"].to_list() == [10]


def test_batched_matcher_multiple_batches():
    """Double loop over multiple batches finds all pairs (LazyFrame so right is re-iterated per left)."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@x.com", "b@x.com"]})
    right = pl.DataFrame({"id": [10, 11], "email": ["a@x.com", "b@x.com"]})
    # Use LazyFrame so each inner loop gets a fresh right iterator
    b = BatchedMatcher(
        left.lazy(), right.lazy(), left_id="id", right_id="id", batch_size=1
    )
    results = b.match(match_on="email")
    assert results.count == 2
    ids_left = sorted(results.matches["id"].to_list())
    ids_right = sorted(results.matches["id_right"].to_list())
    assert ids_left == [1, 2]
    assert ids_right == [10, 11]


def test_batched_matcher_lazyframe():
    """BatchedMatcher with LazyFrame sources (chunked via batches())."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@x.com", "b@x.com"]})
    right = pl.DataFrame({"id": [10, 11], "email": ["a@x.com", "c@x.com"]})
    lf_left = left.lazy()
    lf_right = right.lazy()
    b = BatchedMatcher(lf_left, lf_right, left_id="id", right_id="id", batch_size=10)
    results = b.match(match_on="email")
    assert results.count == 1


def test_batched_matcher_refine_requires_lazyframe():
    """refine() raises when sources are one-shot iterators."""
    left = pl.DataFrame({"id": [1, 2], "email": ["a@x.com", "b@x.com"], "name": ["A", "B"]})
    right = pl.DataFrame({"id": [10, 11], "email": ["a@x.com", "c@x.com"], "name": ["A", "C"]})
    b = BatchedMatcher(iter([left]), iter([right]), left_id="id", right_id="id")
    results = b.match(match_on="email")
    with pytest.raises(ValueError, match="re-iterable"):
        results.refine(match_on="name")


def test_batched_matcher_refine_lazyframe():
    """refine() with LazyFrame re-streams and applies second rule to unmatched."""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "email": ["a@x.com", "b@x.com", None],
        "name": ["Alice", "Bob", "Carol"],
    })
    right = pl.DataFrame({
        "id": [10, 11, 12],
        "email": ["a@x.com", "x@x.com", "y@x.com"],
        "name": ["Alice", "Bob", "Carol"],
    })
    lf_left = left.lazy()
    lf_right = right.lazy()
    b = BatchedMatcher(lf_left, lf_right, left_id="id", right_id="id", batch_size=10)
    results = b.match(match_on="email")
    assert results.count == 1  # only 1 matches on email
    refined = results.refine(match_on="name")
    # Unmatched on email: id 2, 3. Of those, 2 matches 11 on name "Bob", 3 matches 12 on "Carol"
    assert refined.count >= 2
    left_ids = refined.matches["id"].to_list()
    assert 1 in left_ids  # from email
    assert 2 in left_ids or 3 in left_ids  # from refine


def test_batched_matcher_empty_batches():
    """Empty batches are skipped."""
    left = pl.DataFrame({"id": [1], "email": ["a@x.com"]})
    right = pl.DataFrame({"id": [10], "email": ["a@x.com"]})
    empty = left.filter(pl.lit(False))
    b = BatchedMatcher(
        iter([empty, left]), iter([right]), left_id="id", right_id="id"
    )
    results = b.match(match_on="email")
    assert results.count == 1
