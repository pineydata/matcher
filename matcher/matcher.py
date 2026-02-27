"""Entity resolution matching - matches records across two sources.

This module provides the Matcher class for entity resolution, which identifies records
that refer to the same real-world entity across two different data sources.

Key Concepts:
- Entity Resolution: The process of matching records from two sources (left and right)
  that represent the same entity. For example, matching customer records from two
  different databases.
- Rules: Matching rules define which fields must match. Rules can be:
  - Single field: "email" (match when email addresses are identical)
  - Multiple fields: ["first_name", "last_name"] (match when both fields match)
  - Multiple rules: ["email", ["first_name", "last_name"]] (match if ANY rule matches)
- Rule Logic: Within a rule, all fields must match (AND logic). Across rules, any
  rule can match (OR logic).
- Blocking: Optional block_on (str or list[str], e.g. "zip_code" or ["zip_code", "state"])
  restricts candidate pairs to records that share the same value(s). One rule per match(); cascade via .refine().
- Matching algorithm: Pass an algorithm at construction or per call. Default is ExactMatcher.
  Use FuzzyMatcher(threshold=0.85) for fuzzy (Jaro-Winkler) on a single string field.
  Fuzzy + exact on another column: match with FuzzyMatcher then filter or require_exact (composition).

Usage Pattern:
    >>> from matcher import Matcher
    >>> import polars as pl
    >>>
    >>> left_df = pl.read_parquet("customers_a.parquet")
    >>> right_df = pl.read_parquet("customers_b.parquet")
    >>>
    >>> matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    >>> results = matcher.match(match_on="email")
    >>> print(f"Found {results.count} matches")
    >>> # With blocking for large datasets
    >>> results = matcher.match(match_on="email", block_on="zip_code")
    >>> # Fuzzy: pass FuzzyMatcher for this call
    >>> from matcher import FuzzyMatcher
    >>> fuzzy_results = matcher.match(match_on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))

Dependencies:
- Uses MatchingAlgorithm components (from matcher.algorithms) to perform actual matching
- Returns MatchResults objects (from matcher.results) for chaining operations
- Rule and block normalization (match_on, block_on) and block pairing: matcher.rules_and_blocking
- Fuzzy matching: rapidfuzz (cdist, JaroWinkler), PyArrow (Polars–NumPy bridge)
"""

import polars as pl
from polars import DataFrame
from typing import Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from matcher.results import MatchResults

from matcher.algorithms import (
    MatchingAlgorithm,
    ExactMatcher,
    FuzzyMatcher,
    score_on_columns_for_kind,
)
from matcher.rules_and_blocking import (
    normalize_fields,
    paired_blocks_by_key,
)
from matcher.oom import warn_matcher_input_size


def _add_provenance_columns(
    df: DataFrame,
    match_on: Union[str, list[str]],
    algo: MatchingAlgorithm,
) -> DataFrame:
    """Add this algorithm's score and on columns only (from algo.kind). No-op if algo has no kind."""
    kind = getattr(algo, "kind", None)
    if kind is None:
        return df
    score_col, on_col = score_on_columns_for_kind(kind)
    n = df.height
    if n == 0:
        return df.with_columns([
            pl.Series(score_col, [], dtype=pl.Float64),
            pl.Series(on_col, [], dtype=pl.Object),
        ])
    source = getattr(algo, "source_score_column", None)
    if source and source not in df.columns:
        raise ValueError(
            f"Algorithm {type(algo).__name__} declares source_score_column={source!r} "
            f"but match() output has no column {source!r}. Available: {list(df.columns)}."
        )
    score_expr = (
        pl.col(source).alias(score_col) if source else pl.lit(1.0).alias(score_col)
    )
    on_expr = pl.Series(on_col, [match_on] * n, dtype=pl.Object)
    out = df.with_columns([score_expr, on_expr])
    if kind == "fuzzy" and score_col in out.columns:
        out = out.with_columns(pl.col(score_col).alias("confidence"))
    return out


class Matcher:
    """Entity resolution - matches records across two sources.

    In-memory only: accepts Polars DataFrames.
    """

    def __init__(
        self,
        left: DataFrame,
        right: DataFrame,
        left_id: str,
        right_id: str,
        matching_algorithm: Optional[MatchingAlgorithm] = None
    ):
        """Initialize matcher with Polars DataFrames.

        Args:
            left: Polars DataFrame (in-memory) - MUST have left_id column
            right: Polars DataFrame (in-memory) - MUST have right_id column
            left_id: Column name for left source ID
            right_id: Column name for right source ID
            matching_algorithm: MatchingAlgorithm component (default: ExactMatcher).
                               Polars handles parallelization internally for joins.

        Raises:
            ValueError: If left or right DataFrames don't have the specified ID column
        """
        if left.height == 0:
            raise ValueError("Left source is empty")
        if right.height == 0:
            raise ValueError("Right source is empty")

        # Store ID column names
        self.left_id = left_id
        self.right_id = right_id

        # REQUIRE id columns
        if self.left_id not in left.columns:
            raise ValueError(
                f"Left source MUST have '{self.left_id}' column. "
                f"Found columns: {left.columns}"
            )
        if self.right_id not in right.columns:
            raise ValueError(
                f"Right source MUST have '{self.right_id}' column. "
                f"Found columns: {right.columns}"
            )

        self.left = left
        self.right = right

        warn_matcher_input_size(left, right)

        if matching_algorithm is None:
            self.matching_algorithm = ExactMatcher()
        else:
            self.matching_algorithm = matching_algorithm

    def match(
        self,
        match_on: Union[str, list[str]],
        block_on: Optional[Union[str, list[str]]] = None,
        matching_algorithm: Optional[MatchingAlgorithm] = None,
    ) -> "MatchResults":
        """Perform matching on a single rule using the configured or passed algorithm.

        Only one rule per call. For cascading (try another rule on unmatched rows),
        chain with .refine(match_on=...) on the returned MatchResults.

        Args:
            match_on: What to match on. One field (str) or multiple fields (list[str]) that
                must all match together (AND), e.g. "email" or ["first_name", "last_name"].
            block_on: Optional column name or list of names. Only records with the same value(s) are compared.
            matching_algorithm: Optional. Use this algorithm (e.g. FuzzyMatcher).
                               refine() uses the matcher's default algorithm.

        Returns:
            MatchResults for this rule only.

        Examples:
            >>> results = matcher.match(match_on="email")
            >>> results = matcher.match(match_on="email", block_on="zip_code")
            >>> results = matcher.match(match_on="email", block_on=["zip_code", "state"])
            >>> # Cascade: email first, then name for unmatched
            >>> results = matcher.match(match_on="email").refine(match_on=["first_name", "last_name"])
        """
        algo = matching_algorithm if matching_algorithm is not None else self.matching_algorithm
        rule_list = normalize_fields(match_on, single_rule_only=True, param="match_on")
        keys = normalize_fields(block_on, allow_none=True, param="block_on")

        self._validate_fields(self.left, self.right, [rule_list])
        if keys is not None:
            self._validate_fields(self.left, self.right, [keys])

        from matcher.results import MatchResults

        if keys is None:
            blocks_for_rule = [(self.left, self.right)]
        else:
            blocks_for_rule = paired_blocks_by_key(self.left, self.right, keys)
        all_matches = []
        for left_block, right_block in blocks_for_rule:
            rule_matches = algo.match(
                left_block, right_block, rule_list, self.left_id, self.right_id
            )
            all_matches.append(rule_matches)

        if not all_matches:
            # No blocks to process (e.g. blocking had no common keys). Ask the algorithm
            # for an empty result with its schema instead of special-casing in matcher.
            empty_left = self.left.filter(pl.lit(False))
            empty_right = self.right.filter(pl.lit(False))
            empty_result = algo.match(
                empty_left, empty_right, rule_list, self.left_id, self.right_id
            )
            empty_result = _add_provenance_columns(empty_result, match_on, algo)
            return MatchResults(empty_result, original_left=self.left, source=self)
        combined = self._combine_matches(self.left, self.right, all_matches)
        combined = _add_provenance_columns(combined, match_on, algo)
        return MatchResults(combined, original_left=self.left, source=self)

    def _validate_fields(
        self,
        left: DataFrame,
        right: DataFrame,
        rules: list[list[str]]
    ):
        """Validate all fields in rules exist in data sources."""
        all_fields = set()
        for rule in rules:
            all_fields.update(rule)

        missing_left = [f for f in all_fields if f not in left.columns]
        if missing_left:
            available = ", ".join(left.columns)
            raise ValueError(
                f"Field(s) {missing_left} not found in left source. Available: {available}"
            )

        missing_right = [f for f in all_fields if f not in right.columns]
        if missing_right:
            available = ", ".join(right.columns)
            raise ValueError(
                f"Field(s) {missing_right} not found in right source. Available: {available}"
            )

    def _combine_matches(
        self,
        left: DataFrame,
        right: DataFrame,
        all_matches: list[DataFrame]
    ) -> DataFrame:
        """Combine matches from multiple rules (OR logic)."""
        right_id_right = f"{self.right_id}_right"

        # Extract match pairs (ID columns) to handle different column structures
        match_pairs = []
        direct_matches = []  # For cases without id columns

        for matches_df in all_matches:
            # Extract left_id and right_id_right if they exist
            if self.left_id in matches_df.columns and right_id_right in matches_df.columns:
                pairs = matches_df.select([
                    pl.col(self.left_id).alias("left_id"),
                    pl.col(right_id_right).alias("right_id")
                ]).unique()
                match_pairs.append(pairs)
            else:
                # No id columns - use matches directly
                direct_matches.append(matches_df)

        # Combine results
        all_results = []

        if match_pairs:
            # Combine and deduplicate match pairs
            combined_pairs = pl.concat(match_pairs).unique()

            # Rejoin with original data to get full records
            right_with_id = right.with_columns(pl.col(self.right_id).alias(right_id_right))
            result = (
                left.join(combined_pairs, left_on=self.left_id, right_on="left_id", how="inner")
                .join(right_with_id, left_on="right_id", right_on=self.right_id, how="inner", suffix="_right")
            )
            cols_to_drop = [c for c in ["left_id", "right_id"] if c in result.columns]
            if cols_to_drop:
                result = result.drop(cols_to_drop)
            # Preserve confidence from algorithm output (e.g. FuzzyMatcher) for backward compat.
            # _add_provenance_columns uses it for fuzzy_score. Merge from all blocks so pairs
            # from any block get a value (not just the first block).
            conf_parts = [
                m.select(pl.col(self.left_id), pl.col(right_id_right), pl.col("confidence"))
                for m in all_matches
                if "confidence" in m.columns
                and self.left_id in m.columns
                and right_id_right in m.columns
                and m.height > 0
            ]
            if conf_parts:
                conf_merged = (
                    pl.concat(conf_parts)
                    .group_by([self.left_id, right_id_right])
                    .agg(pl.col("confidence").max())  # same pair in multiple blocks: take max
                )
                result = result.join(conf_merged, on=[self.left_id, right_id_right], how="left")
            elif any(
                "confidence" in m.columns and self.left_id in m.columns and right_id_right in m.columns
                for m in all_matches
            ):
                # Schema consistency: algo has confidence but no block had matches; add column (empty or nulls).
                result = result.with_columns(
                    pl.Series("confidence", [], dtype=pl.Float64).alias("confidence")
                    if result.height == 0
                    else pl.lit(None).cast(pl.Float64).alias("confidence")
                )
            all_results.append(result)

        # Add direct matches (no id columns)
        if direct_matches:
            all_results.extend(direct_matches)

        if not all_results:
            empty_result = left.filter(pl.lit(False))
            return empty_result

        # Combine all results
        if len(all_results) == 1:
            final_result = all_results[0]
        else:
            # Combine and deduplicate
            combined = pl.concat(all_results)

            # Deduplicate on id columns if they exist, otherwise on all columns
            if self.left_id in combined.columns and right_id_right in combined.columns:
                final_result = combined.unique(subset=[self.left_id, right_id_right])
            else:
                final_result = combined.unique()

        return final_result
