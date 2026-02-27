"""Batched matching for entity resolution when left/right do not fit in memory.

This module provides batches() and BatchedMatcher for processing chunked or
LazyFrame sources without loading full DataFrames. Same semantics as Matcher;
refine() is supported by re-streaming and filtering to unmatched left IDs.
Rule and block semantics (match_on, block_on) are defined in matcher.rules_and_blocking.

Preprocessing (normalize emails, derive blocking keys, etc.) should be done
*before* batching: build it into the LazyFrame plan or wrap the iterator so
each batch is preprocessed before BatchedMatcher consumes it. See
docs/BATCHED_MATCHER_DESIGN.md for patterns.

Usage:
    >>> import polars as pl
    >>> from matcher import BatchedMatcher, batches, ExactMatcher
    >>> left_lf = pl.scan_parquet("left.parquet").with_columns(pl.col("email").str.to_lowercase())
    >>> right_lf = pl.scan_parquet("right.parquet").with_columns(pl.col("email").str.to_lowercase())
    >>> batcher = BatchedMatcher(left_lf, right_lf, left_id="id", right_id="id")
    >>> results = batcher.match(match_on="email")
    >>> refined = results.refine(match_on=["first_name", "last_name"])
"""

from __future__ import annotations

import polars as pl
from polars import DataFrame, LazyFrame
from typing import Iterator, Union, Optional, TYPE_CHECKING

from matcher.algorithms import MatchingAlgorithm, ExactMatcher
from matcher.rules_and_blocking import (
    normalize_fields,
    paired_blocks_by_key,
)
from matcher.matcher import _add_provenance_columns

if TYPE_CHECKING:
    from matcher.results import MatchResults

_BATCH_INDEX_COL = "_matcher_batch_row_index"


def batches(
    source: Union[DataFrame, LazyFrame],
    batch_size: int = 50_000,
) -> Iterator[DataFrame]:
    """Turn a DataFrame or LazyFrame into an iterator of DataFrames (chunks).

    Use this to feed BatchedMatcher when data comes from scan_parquet/scan_csv
    or when you have a large in-memory DataFrame and want to process in chunks.

    Args:
        source: Polars DataFrame or LazyFrame. DataFrame is sliced in memory;
                LazyFrame is collected in chunks via row index (requires
                re-executing the plan per chunk).
        batch_size: Number of rows per chunk (default 50_000).

    Yields:
        DataFrames of up to batch_size rows. Last chunk may be smaller.

    Example:
        >>> for batch in batches(pl.scan_parquet("data.parquet")):
        ...     process(batch)
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    if isinstance(source, DataFrame):
        n = source.height
        for start in range(0, n, batch_size):
            yield source.slice(start, batch_size)
        return

    if isinstance(source, LazyFrame):
        offset = 0
        while True:
            # Chunk by row index: add index, filter range, collect, drop index
            lf = source.with_row_index(_BATCH_INDEX_COL)
            chunk_lf = lf.filter(
                pl.col(_BATCH_INDEX_COL).is_between(offset, offset + batch_size - 1)
            )
            chunk = chunk_lf.collect()
            if chunk.height == 0:
                break
            chunk = chunk.drop(_BATCH_INDEX_COL)
            yield chunk
            if chunk.height < batch_size:
                break
            offset += batch_size
        return

    raise TypeError(
        f"source must be a Polars DataFrame or LazyFrame, got {type(source).__name__}"
    )


def _validate_fields(
    left: DataFrame,
    right: DataFrame,
    rule_list: list[str],
    keys: Optional[list[str]],
    left_id: str,
    right_id: str,
) -> None:
    """Validate rule and blocking columns exist; require id columns."""
    if left_id not in left.columns:
        raise ValueError(
            f"Left batch MUST have '{left_id}' column. Found: {list(left.columns)}"
        )
    if right_id not in right.columns:
        raise ValueError(
            f"Right batch MUST have '{right_id}' column. Found: {list(right.columns)}"
        )
    for f in rule_list:
        if f not in left.columns:
            raise ValueError(f"Field '{f}' not in left. Available: {list(left.columns)}")
        if f not in right.columns:
            raise ValueError(f"Field '{f}' not in right. Available: {list(right.columns)}")
    if keys:
        for k in keys:
            if k not in left.columns:
                raise ValueError(
                    f"blocking_key '{k}' not in left. Available: {list(left.columns)}"
                )
            if k not in right.columns:
                raise ValueError(
                    f"blocking_key '{k}' not in right. Available: {list(right.columns)}"
                )


def _combine_batch_matches(all_matches: list[DataFrame], left_id: str, right_id: str) -> DataFrame:
    """Concatenate per-batch match DataFrames and deduplicate on (left_id, right_id_right)."""
    right_id_right = f"{right_id}_right"
    if not all_matches:
        return pl.DataFrame()
    combined = pl.concat(all_matches)
    if left_id in combined.columns and right_id_right in combined.columns:
        combined = combined.unique(subset=[left_id, right_id_right], keep="first")
    else:
        combined = combined.unique()
    return combined


class BatchedMatcher:
    """Entity resolution over chunked or batched sources (no full left/right in memory).

    Use when left and/or right do not fit in memory. Pass iterators of DataFrames
    or LazyFrames; we process in batches and return MatchResults. Refine is
    supported only when sources are re-iterable (LazyFrame). One-shot iterators
    are consumed by match(); pass a fresh iterator or LazyFrame for each match()
    if you need to run more than once.
    """

    def __init__(
        self,
        left_batches: Union[Iterator[DataFrame], LazyFrame],
        right_batches: Union[Iterator[DataFrame], LazyFrame],
        left_id: str,
        right_id: str,
        *,
        matching_algorithm: Optional[MatchingAlgorithm] = None,
        batch_size: int = 50_000,
    ):
        """Initialize with batch sources and ID column names.

        Args:
            left_batches: Iterator of left DataFrames, or a LazyFrame (we chunk it).
            right_batches: Iterator of right DataFrames, or a LazyFrame (we chunk it).
            left_id: Column name for left source ID (must exist in every left batch).
            right_id: Column name for right source ID (must exist in every right batch).
            matching_algorithm: Algorithm to use (default ExactMatcher).
            batch_size: Rows per chunk when left/right are LazyFrames (default 50_000).
        """
        self.left_id = left_id
        self.right_id = right_id
        self.batch_size = batch_size
        self.matching_algorithm = matching_algorithm or ExactMatcher()
        # Store sources for re-iteration (refine). Iterators are one-shot; LazyFrame is re-iterable.
        self._left_source: Union[Iterator[DataFrame], LazyFrame] = left_batches
        self._right_source: Union[Iterator[DataFrame], LazyFrame] = right_batches
        self._last_matched_left_ids: Optional[DataFrame] = None  # set after match/refine for next refine

    def _left_iter(self) -> Iterator[DataFrame]:
        if isinstance(self._left_source, LazyFrame):
            return batches(self._left_source, self.batch_size)
        return self._left_source

    def _right_iter(self) -> Iterator[DataFrame]:
        if isinstance(self._right_source, LazyFrame):
            return batches(self._right_source, self.batch_size)
        return self._right_source

    def _can_reiterate(self) -> bool:
        """True if we can re-iterate both sources (needed for refine)."""
        return isinstance(self._left_source, LazyFrame) and isinstance(
            self._right_source, LazyFrame
        )

    def match(
        self,
        match_on: Union[str, list[str]],
        block_on: Optional[Union[str, list[str]]] = None,
        matching_algorithm: Optional[MatchingAlgorithm] = None,
    ) -> "MatchResults":
        """Run matching on one rule over all (left_batch, right_batch) pairs.

        Same semantics as Matcher.match(): one rule, optional blocking per batch.
        Returns MatchResults. Refine is supported if sources are LazyFrames.
        """
        from matcher.results import MatchResults

        algo = matching_algorithm or self.matching_algorithm
        rule_list = normalize_fields(match_on, single_rule_only=True, param="match_on")
        keys = normalize_fields(block_on, allow_none=True, param="block_on")

        all_matches: list[DataFrame] = []
        right_id_right = f"{self.right_id}_right"
        first_left: Optional[DataFrame] = None
        first_right: Optional[DataFrame] = None

        for left_batch in self._left_iter():
            if left_batch.height == 0:
                continue
            if first_left is None:
                first_left = left_batch
            for right_batch in self._right_iter():
                if right_batch.height == 0:
                    continue
                if first_right is None:
                    first_right = right_batch
                _validate_fields(
                    left_batch, right_batch, rule_list, keys,
                    self.left_id, self.right_id,
                )
                if keys is None:
                    blocks = [(left_batch, right_batch)]
                else:
                    blocks = paired_blocks_by_key(left_batch, right_batch, keys)
                for left_block, right_block in blocks:
                    if left_block.height == 0 or right_block.height == 0:
                        continue
                    m = algo.match(
                        left_block, right_block, rule_list,
                        self.left_id, self.right_id,
                    )
                    if m.height > 0:
                        all_matches.append(m)

        if not all_matches:
            # Empty result with correct schema from algo
            if first_left is None:
                first_left = pl.DataFrame({self.left_id: []})
            if first_right is None:
                first_right = pl.DataFrame({self.right_id: []})
            empty_left = first_left.filter(pl.lit(False))
            empty_right = first_right.filter(pl.lit(False))
            empty_result = algo.match(
                empty_left, empty_right, rule_list,
                self.left_id, self.right_id,
            )
            empty_result = _add_provenance_columns(empty_result, match_on, algo)
            self._last_matched_left_ids = pl.DataFrame({self.left_id: []})
            return MatchResults(empty_result, original_left=None, source=self)

        combined = _combine_batch_matches(all_matches, self.left_id, self.right_id)
        combined = _add_provenance_columns(combined, match_on, algo)
        self._last_matched_left_ids = combined.select(self.left_id).unique()
        return MatchResults(combined, original_left=None, source=self)

    def refine(
        self,
        results: "MatchResults",
        match_on: Union[str, list[str]],
        block_on: Optional[Union[str, list[str]]] = None,
        matching_algorithm: Optional[MatchingAlgorithm] = None,
    ) -> "MatchResults":
        """Apply another rule to left rows that did not match yet (batched re-stream).

        Re-streams left and right; filters left to unmatched (left_id not in
        matched_left_ids); runs match for the new rule; unions pairs and
        builds full result. Requires LazyFrame sources (re-iterable).
        """
        from matcher.results import MatchResults
        from matcher.matcher import _add_provenance_columns
        from matcher.algorithms import is_score_on_column

        if not self._can_reiterate():
            raise ValueError(
                "refine() with BatchedMatcher requires re-iterable sources (LazyFrame). "
                "One-shot iterators cannot be re-streamed."
            )

        right_id_right = f"{self.right_id}_right"
        if right_id_right not in results.matches.columns:
            raise ValueError(
                f"refine() expects matches to have '{right_id_right}'. "
                "Cannot determine unmatched left."
            )
        matched_left_ids = results.matches.select(self.left_id).unique()
        algo = matching_algorithm or self.matching_algorithm
        rule_list = normalize_fields(match_on, single_rule_only=True, param="match_on")
        keys = normalize_fields(block_on, allow_none=True, param="block_on")

        # Re-stream: for each left_batch filter to unmatched, then for each right_batch run match
        new_matches_list: list[DataFrame] = []
        for left_batch in self._left_iter():
            if left_batch.height == 0:
                continue
            unmatched_left = left_batch.join(
                matched_left_ids, on=self.left_id, how="anti"
            )
            if unmatched_left.height == 0:
                continue
            for right_batch in self._right_iter():
                if right_batch.height == 0:
                    continue
                _validate_fields(
                    unmatched_left, right_batch, rule_list, keys,
                    self.left_id, self.right_id,
                )
                if keys is None:
                    blocks = [(unmatched_left, right_batch)]
                else:
                    blocks = paired_blocks_by_key(unmatched_left, right_batch, keys)
                for left_block, right_block in blocks:
                    if left_block.height == 0 or right_block.height == 0:
                        continue
                    m = algo.match(
                        left_block, right_block, rule_list,
                        self.left_id, self.right_id,
                    )
                    if m.height > 0:
                        new_matches_list.append(m)

        if not new_matches_list:
            # No new pairs; return existing results
            self._last_matched_left_ids = results.matches.select(self.left_id).unique()
            return results

        new_combined = _combine_batch_matches(
            new_matches_list, self.left_id, self.right_id
        )
        new_combined = _add_provenance_columns(new_combined, match_on, algo)

        # Union pairs: existing + new
        existing_pairs = results.matches.select([
            pl.col(self.left_id).alias("_lid"),
            pl.col(right_id_right).alias("_rid"),
        ]).unique()
        new_pairs = new_combined.select([
            pl.col(self.left_id).alias("_lid"),
            pl.col(right_id_right).alias("_rid"),
        ]).unique()
        combined_pairs = pl.concat([existing_pairs, new_pairs]).unique()

        # Build full result: re-stream to materialize full rows for combined_pairs
        full_parts: list[DataFrame] = []
        for left_batch in self._left_iter():
            for right_batch in self._right_iter():
                left_ids = left_batch.select(self.left_id).to_series().to_list()
                right_ids = right_batch.select(self.right_id).to_series().to_list()
                pairs_in_batch = combined_pairs.filter(
                    pl.col("_lid").is_in(left_ids)
                    & pl.col("_rid").is_in(right_ids)
                )
                if pairs_in_batch.height == 0:
                    continue
                right_with_suffix = right_batch.with_columns(
                    pl.col(self.right_id).alias(right_id_right)
                )
                merged = (
                    pairs_in_batch.join(
                        left_batch, left_on="_lid", right_on=self.left_id, how="left"
                    )
                    .join(
                        right_with_suffix,
                        left_on="_rid", right_on=self.right_id, how="left", suffix="_right",
                    )
                )
                # Preserve left id (join key may be dropped by Polars); ensure right_id_right exists
                if self.left_id not in merged.columns:
                    merged = merged.with_columns(pl.col("_lid").alias(self.left_id))
                if right_id_right not in merged.columns:
                    merged = merged.with_columns(pl.col("_rid").alias(right_id_right))
                merged = merged.drop(["_lid", "_rid"])
                full_parts.append(merged)

        if not full_parts:
            combined_full = results.matches
        else:
            combined_full = pl.concat(full_parts).unique(
                subset=[self.left_id, right_id_right], keep="first"
            )
            # Merge provenance from results.matches and new_combined
            existing_score_on = [c for c in results.matches.columns if is_score_on_column(c)]
            new_score_on = [c for c in new_combined.columns if is_score_on_column(c)]
            all_score_on = sorted(set(existing_score_on) | set(new_score_on))
            if all_score_on:
                existing_scores = results.matches.select(
                    [self.left_id, right_id_right] + existing_score_on
                ) if existing_score_on else None
                new_scores = new_combined.select(
                    [self.left_id, right_id_right] + new_score_on
                ) if new_score_on else None
                if existing_scores is not None and new_scores is not None:
                    merged_scores = existing_scores.join(
                        new_scores, on=[self.left_id, right_id_right],
                        how="full", suffix="_new",
                    )
                    key_left_new = f"{self.left_id}_new"
                    key_right_new = f"{right_id_right}_new"
                    if key_left_new in merged_scores.columns and key_right_new in merged_scores.columns:
                        merged_scores = merged_scores.with_columns(
                            pl.coalesce(pl.col(self.left_id), pl.col(key_left_new)).alias(self.left_id),
                            pl.coalesce(pl.col(right_id_right), pl.col(key_right_new)).alias(right_id_right),
                        )
                        merged_scores = merged_scores.select(
                            [self.left_id, right_id_right]
                            + [
                                pl.coalesce(pl.col(c), pl.col(f"{c}_new")).alias(c)
                                if f"{c}_new" in merged_scores.columns else pl.col(c)
                                for c in all_score_on
                            ]
                        )
                    combined_full = combined_full.join(
                        merged_scores, on=[self.left_id, right_id_right], how="left"
                    )
                elif existing_scores is not None:
                    combined_full = combined_full.join(
                        existing_scores, on=[self.left_id, right_id_right], how="left"
                    )
                else:
                    combined_full = combined_full.join(
                        new_scores, on=[self.left_id, right_id_right], how="left"
                    )

        self._last_matched_left_ids = combined_full.select(self.left_id).unique()
        return MatchResults(combined_full, original_left=None, source=self)
