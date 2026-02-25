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
- Blocking: Optional blocking_key (str or list[str], e.g. "zip_code" or ["zip_code", "state"])
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
    >>> results = matcher.match(on="email")
    >>> print(f"Found {results.count} matches")
    >>> # With blocking for large datasets
    >>> results = matcher.match(on="email", blocking_key="zip_code")
    >>> # Fuzzy: pass FuzzyMatcher for this call
    >>> from matcher import FuzzyMatcher
    >>> fuzzy_results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))

Dependencies:
- Uses MatchingAlgorithm components (from matcher.algorithms) to perform actual matching
- Returns MatchResults objects (from matcher.results) for chaining operations
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


def _add_provenance_columns(
    df: DataFrame,
    on: Union[str, list[str]],
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
    score_expr = (
        pl.col(source).alias(score_col)
        if source and source in df.columns
        else pl.lit(1.0).alias(score_col)
    )
    on_expr = pl.Series(on_col, [on] * n, dtype=pl.Object)
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

        if matching_algorithm is None:
            self.matching_algorithm = ExactMatcher()
        else:
            self.matching_algorithm = matching_algorithm

    def match(
        self,
        on: Union[str, list[str]],
        blocking_key: Optional[Union[str, list[str]]] = None,
        matching_algorithm: Optional[MatchingAlgorithm] = None,
    ) -> "MatchResults":
        """Perform matching on a single rule using the configured or passed algorithm.

        Only one rule per call. For cascading (try another rule on unmatched rows),
        chain with .refine(on=...) on the returned MatchResults.

        Args:
            on: What to match on. One field (str) or multiple fields (list[str]) that
                must all match together (AND), e.g. "email" or ["first_name", "last_name"].
            blocking_key: Optional column name or list of names. Only records with the same value(s) are compared.
            matching_algorithm: Optional. Use this algorithm (e.g. FuzzyMatcher).
                               refine() uses the matcher's default algorithm.

        Returns:
            MatchResults for this rule only.

        Examples:
            >>> results = matcher.match(on="email")
            >>> results = matcher.match(on="email", blocking_key="zip_code")
            >>> results = matcher.match(on="email", blocking_key=["zip_code", "state"])
            >>> # Cascade: email first, then name for unmatched
            >>> results = matcher.match(on="email").refine(on=["first_name", "last_name"])
        """
        algo = matching_algorithm if matching_algorithm is not None else self.matching_algorithm
        rule_list = self._normalize_single_rule(on)
        keys = self._normalize_blocking_keys(blocking_key)

        self._validate_fields(self.left, self.right, [rule_list])
        if keys is not None:
            self._validate_fields(self.left, self.right, [keys])

        from matcher.results import MatchResults

        if keys is None:
            blocks_for_rule = [(self.left, self.right)]
        else:
            blocks_for_rule = self._block_pairs(keys)
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
            empty_result = _add_provenance_columns(empty_result, on, algo)
            return MatchResults(empty_result, original_left=self.left, source=self)
        combined = self._combine_matches(self.left, self.right, all_matches)
        combined = _add_provenance_columns(combined, on, algo)
        return MatchResults(combined, original_left=self.left, source=self)

    def _normalize_blocking_keys(
        self, blocking_key: Optional[Union[str, list[str]]]
    ) -> Optional[list[str]]:
        """Return None or a non-empty list of column names. Reject empty list."""
        if blocking_key is None:
            return None
        if isinstance(blocking_key, str):
            return [blocking_key]
        if isinstance(blocking_key, list) and len(blocking_key) > 0:
            if all(isinstance(k, str) for k in blocking_key):
                return list(blocking_key)
            raise ValueError(
                "blocking_key must be a column name (str) or list of column names (list[str])"
            )
        raise ValueError("blocking_key must be non-empty when provided")

    def _paired_blocks_by_key(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        blocking_key: Union[str, list[str]],
    ) -> list[tuple[DataFrame, DataFrame]]:
        """Return (left_block, right_block) pairs for each common blocking key value(s).

        blocking_key can be one column or a list of columns; a block = same value for all.
        Nulls: rows where all blocking columns are null form one block.
        """
        keys = [blocking_key] if isinstance(blocking_key, str) else list(blocking_key)
        left_vals = left_df.select(keys).unique()
        right_vals = right_df.select(keys).unique()
        common = left_vals.join(right_vals, on=keys, how="inner")

        # Build (left_block, right_block) for each row in common
        pairs = []
        for row in common.iter_rows(named=True):
            block_vals = [row[k] for k in keys]
            if all(v is None for v in block_vals):
                left_b = left_df.filter(
                    pl.all_horizontal(pl.col(k).is_null() for k in keys)
                )
                right_b = right_df.filter(
                    pl.all_horizontal(pl.col(k).is_null() for k in keys)
                )
            else:
                pred_left = pl.all_horizontal(
                    pl.col(k) == v for k, v in zip(keys, block_vals)
                )
                pred_right = pl.all_horizontal(
                    pl.col(k) == v for k, v in zip(keys, block_vals)
                )
                left_b = left_df.filter(pred_left)
                right_b = right_df.filter(pred_right)
            pairs.append((left_b, right_b))

        # Add null block when both sides have rows with all blocking keys null
        null_pred = pl.all_horizontal(pl.col(k).is_null() for k in keys)
        left_has_nulls = left_df.filter(null_pred).height > 0
        right_has_nulls = right_df.filter(null_pred).height > 0
        if left_has_nulls and right_has_nulls:
            # Only add if not already in common (inner join excludes nulls)
            left_b = left_df.filter(null_pred)
            right_b = right_df.filter(null_pred)
            pairs.append((left_b, right_b))

        return pairs

    def _block_pairs(
        self, blocking_key: Union[str, list[str]]
    ) -> list[tuple[DataFrame, DataFrame]]:
        """Return list of (left_block, right_block) for each common blocking key value(s)."""
        keys = [blocking_key] if isinstance(blocking_key, str) else list(blocking_key)
        return self._paired_blocks_by_key(self.left, self.right, keys)

    def _normalize_single_rule(self, on: Union[str, list]) -> list[str]:
        """Normalize on= to a single rule as list of field names. Reject multiple rules.

        Accepts: str (one field), list[str] (multiple fields in one rule), or list of one list
        (e.g. on=[["first_name", "last_name"]] treated as that inner list). Rejects list of
        multiple lists (multiple rules); use .refine(on=...) for cascading.
        """
        if isinstance(on, str):
            return [on]
        if isinstance(on, list) and len(on) > 0:
            if all(isinstance(item, str) for item in on):
                return on
            if len(on) == 1 and isinstance(on[0], list):
                if len(on[0]) == 0:
                    raise ValueError("on must contain at least one field name")
                return list(on[0])
            raise ValueError(
                "Only a single rule is allowed per match(). "
                "For cascading (next rule on unmatched), use .refine(on=...) after match()."
            )
        raise ValueError("on must be a non-empty string or list of field names")

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
            # Preserve confidence from algorithm output (e.g. FuzzyMatcher) for backward compatibility
            for m in all_matches:
                if "confidence" in m.columns and self.left_id in m.columns and right_id_right in m.columns:
                    conf = m.select(pl.col(self.left_id), pl.col(right_id_right), pl.col("confidence"))
                    result = result.join(conf, on=[self.left_id, right_id_right], how="left")
                    break
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
