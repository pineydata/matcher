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
- Blocking: Optional blocking_key (e.g. "zip_code") restricts candidate pairs to
  records that share the same blocking key value, reducing comparisons and memory.
  Supported on match() and match_fuzzy().
- Fuzzy Matching: match_fuzzy() uses Jaro-Winkler similarity (via rapidfuzz) for
  typo-tolerant matching on a single string field, with a configurable threshold.

Usage Pattern:
    >>> from matcher import Matcher
    >>> import polars as pl
    >>>
    >>> left_df = pl.read_parquet("customers_a.parquet")
    >>> right_df = pl.read_parquet("customers_b.parquet")
    >>>
    >>> matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    >>> results = matcher.match(rules="email")
    >>> print(f"Found {results.count} matches")
    >>> # With blocking for large datasets
    >>> results = matcher.match(rules="email", blocking_key="zip_code")
    >>> # Fuzzy matching for names with typos
    >>> fuzzy_results = matcher.match_fuzzy(field="name", threshold=0.85)

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

from matcher.algorithms import MatchingAlgorithm, ExactMatcher


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
        rules: Union[str, list[str], list[Union[str, list[str]]]],
        blocking_key: Optional[str] = None
    ) -> "MatchResults":
        """Perform matching using the configured matching algorithm with sequential rule processing.

        Rules are processed sequentially and combined with OR logic.
        Optional blocking restricts candidate pairs to records that share the same blocking_key
        value (e.g. zip_code), reducing comparisons and memory for large datasets.

        Args:
            rules: Matching rule(s). Can be:
                  - Single field (str): "email"
                  - Single rule with one field (list[str]): ["email"]
                  - Single rule with multiple fields (list[str]): ["first_name", "last_name"]
                  - Multiple rules (list): ["email", ["first_name", "last_name"]]

                  Records match if ANY rule matches (OR logic).
                  Within a rule, all fields must match together (AND logic).
            blocking_key: Optional column name. When set, only records with the same value
                          in this column are compared (e.g. "zip_code"). Reduces comparisons
                          and memory; same matches as without blocking when blocks partition
                          the data sensibly.

        Returns:
            MatchResults object with matches

        Examples:
            >>> # Single field
            >>> results = matcher.match(rules="email")
            >>> # With blocking for performance
            >>> results = matcher.match(rules="email", blocking_key="zip_code")
            >>> # Multiple rules: match if email OR (first_name AND last_name)
            >>> results = matcher.match(rules=[
            ...     "email",
            ...     ["first_name", "last_name"]
            ... ])
        """
        # Normalize rules to list of lists
        normalized_rules = self._normalize_rules(rules)

        # Validate all fields exist (including blocking_key if provided)
        self._validate_fields(self.left, self.right, normalized_rules)
        if blocking_key is not None:
            self._validate_fields(
                self.left, self.right, [[blocking_key]]
            )

        # Resolve (left, right) pairs to run: either one full pair or per-block pairs
        if blocking_key is None:
            blocks_to_run = [(self.left, self.right)]
        else:
            blocks_to_run = self._block_pairs(blocking_key)

        # Process rules sequentially within each block (Polars parallelizes joins internally)
        all_matches = []
        for left_block, right_block in blocks_to_run:
            for rule in normalized_rules:
                rule_matches = self.matching_algorithm.match(
                    left_block, right_block, rule, self.left_id, self.right_id
                )
                all_matches.append(rule_matches)

        # Import here to avoid circular dependency
        from matcher.results import MatchResults

        if not all_matches:
            # Empty result with same schema as non-empty: need a column present in both
            if self.left_id == self.right_id and self.left_id in self.right.columns:
                join_col = self.left_id
            else:
                join_col = self.left.columns[0] if self.left.columns else self.left_id
                if join_col not in self.right.columns:
                    raise ValueError(
                        f"Cannot build empty result: left column '{join_col}' not in right. "
                        f"Right columns: {list(self.right.columns)}"
                    )
            empty_result = self.left.join(self.right, on=join_col, how="inner").filter(pl.lit(False))
            return MatchResults(empty_result, original_left=self.left)

        # Combine results (OR logic)
        final_result = self._combine_matches(self.left, self.right, all_matches)

        return MatchResults(final_result, original_left=self.left)

    def _paired_blocks_by_key(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        blocking_key: str,
    ) -> list[tuple[DataFrame, DataFrame]]:
        """Return (left_block, right_block) pairs for each common blocking key value.

        Nulls in the blocking key form one block. Used by both exact and fuzzy matching.
        """
        left_vals = left_df.select(blocking_key).unique()
        right_vals = right_df.select(blocking_key).unique()
        common = left_vals.join(right_vals, on=blocking_key, how="inner")
        block_values = common.to_series().to_list()

        pairs = []
        for block_val in block_values:
            if block_val is None:
                left_b = left_df.filter(pl.col(blocking_key).is_null())
                right_b = right_df.filter(pl.col(blocking_key).is_null())
            else:
                left_b = left_df.filter(pl.col(blocking_key) == block_val)
                right_b = right_df.filter(pl.col(blocking_key) == block_val)
            pairs.append((left_b, right_b))
        return pairs

    def _block_pairs(self, blocking_key: str) -> list[tuple[DataFrame, DataFrame]]:
        """Return list of (left_block, right_block) for each common blocking key value."""
        return self._paired_blocks_by_key(self.left, self.right, blocking_key)

    def _normalize_rules(self, rules: Union[str, list[str], list[Union[str, list[str]]]]) -> list[list[str]]:
        """Normalize rules input to list of lists."""
        if isinstance(rules, str):
            return [[rules]]
        elif isinstance(rules, list) and len(rules) > 0:
            if all(isinstance(item, str) for item in rules):
                # Single rule with multiple fields: ["email", "zip_code"]
                return [rules]
            else:
                # Multiple rules: ["email", ["first_name", "last_name"]]
                normalized = []
                for rule in rules:
                    if isinstance(rule, str):
                        normalized.append([rule])
                    elif isinstance(rule, list):
                        if len(rule) == 0:
                            raise ValueError("Each rule must contain at least one field name")
                        normalized.append(rule)
                    else:
                        raise ValueError(f"Each rule must be a string or list, got {type(rule)}")
                return normalized
        else:
            raise ValueError("Rules must be a string, list of strings, or list of rules")

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

    def _empty_fuzzy_result(self) -> "MatchResults":
        """Return MatchResults with zero rows and schema compatible with non-empty fuzzy results."""
        from matcher.results import MatchResults
        empty_pairs = pl.DataFrame(
            {self.left_id: [], "_right_id_val": [], "confidence": []},
            schema={
                self.left_id: self.left.schema[self.left_id],
                "_right_id_val": self.right.schema[self.right_id],
                "confidence": pl.Float64,
            },
        )
        right_id_right = f"{self.right_id}_right"
        right_with_suffix = self.right.with_columns(
            pl.col(self.right_id).alias(right_id_right)
        )
        result = (
            empty_pairs.join(self.left, on=self.left_id, how="left")
            .join(
                right_with_suffix,
                left_on="_right_id_val",
                right_on=self.right_id,
                how="left",
                suffix="_right",
            )
            .drop("_right_id_val")
        )
        return MatchResults(result, original_left=self.left)

    def match_fuzzy(
        self,
        field: str,
        threshold: float = 0.85,
        blocking_key: Optional[str] = None
    ) -> "MatchResults":
        """Fuzzy matching on a single string field using Jaro-Winkler similarity.

        Uses batch vectorized similarity (rapidfuzz.process.cdist) with Polars → Arrow
        → rapidfuzz data flow. Rows where the field is null are excluded from
        matching (same as exact match: nulls do not match).
        Optional blocking runs fuzzy only within blocks (same blocking_key value),
        reducing matrix size and memory for large datasets.

        Args:
            field: Single column name to match on (string values).
            threshold: Minimum similarity in [0, 1] to count as a match (default 0.85).
            blocking_key: Optional column name. When set, only records with the same
                          value in this column are compared; similarity matrix is built
                          per block to bound memory.

        Returns:
            MatchResults with matches including a 'confidence' column (0–1).

        Raises:
            ValueError: If field is missing in left or right, or threshold not in [0, 1].

        Example:
            >>> results = matcher.match_fuzzy(field="name", threshold=0.85)
            >>> results = matcher.match_fuzzy(field="name", blocking_key="zip_code")
        """
        if not 0 <= threshold <= 1:
            raise ValueError(f"threshold must be between 0 and 1, got {threshold}")
        if field not in self.left.columns:
            raise ValueError(
                f"Field '{field}' not found in left source. Available: {self.left.columns}"
            )
        if field not in self.right.columns:
            raise ValueError(
                f"Field '{field}' not found in right source. Available: {self.right.columns}"
            )
        if blocking_key is not None:
            if blocking_key not in self.left.columns:
                raise ValueError(
                    f"blocking_key '{blocking_key}' not found in left source. Available: {self.left.columns}"
                )
            if blocking_key not in self.right.columns:
                raise ValueError(
                    f"blocking_key '{blocking_key}' not found in right source. Available: {self.right.columns}"
                )

        # Fuzzy matching requires string columns (we use .str.to_lowercase() etc.)
        left_dtype = self.left.schema[field]
        right_dtype = self.right.schema[field]
        if left_dtype != pl.Utf8:
            raise ValueError(
                f"Field '{field}' in left source must be a string (Utf8) column for fuzzy matching, got {left_dtype}"
            )
        if right_dtype != pl.Utf8:
            raise ValueError(
                f"Field '{field}' in right source must be a string (Utf8) column for fuzzy matching, got {right_dtype}"
            )

        # Exclude nulls (same semantics as exact match)
        left_valid = self.left.filter(pl.col(field).is_not_null())
        right_valid = self.right.filter(pl.col(field).is_not_null())
        if left_valid.height == 0 or right_valid.height == 0:
            return self._empty_fuzzy_result()

        # Resolve (left_block, right_block) pairs: one full pair or per-block
        if blocking_key is None:
            block_pairs = [(left_valid, right_valid)]
        else:
            block_pairs = self._fuzzy_block_pairs(left_valid, right_valid, blocking_key)

        all_pairs = []
        for left_block, right_block in block_pairs:
            if left_block.height == 0 or right_block.height == 0:
                continue
            pairs_df = self._fuzzy_pairs_for_blocks(
                left_block, right_block, field, threshold
            )
            if pairs_df is not None and pairs_df.height > 0:
                all_pairs.append(pairs_df)

        if not all_pairs:
            return self._empty_fuzzy_result()

        pairs = pl.concat(all_pairs).unique(subset=[self.left_id, "_right_id_val"], keep="first")

        # Rejoin with full left/right to get same shape as match() and add confidence
        right_id_right = f"{self.right_id}_right"
        right_with_suffix = self.right.with_columns(
            pl.col(self.right_id).alias(right_id_right)
        )
        result = (
            pairs.join(self.left, on=self.left_id, how="left")
            .join(
                right_with_suffix,
                left_on="_right_id_val",
                right_on=self.right_id,
                how="left",
                suffix="_right",
            )
            .drop("_right_id_val")
        )

        from matcher.results import MatchResults
        return MatchResults(result, original_left=self.left)

    def _fuzzy_block_pairs(
        self,
        left_valid: DataFrame,
        right_valid: DataFrame,
        blocking_key: str
    ) -> list[tuple[DataFrame, DataFrame]]:
        """Return list of (left_block, right_block) for each common blocking key value."""
        return self._paired_blocks_by_key(left_valid, right_valid, blocking_key)

    def _fuzzy_pairs_for_blocks(
        self,
        left_block: DataFrame,
        right_block: DataFrame,
        field: str,
        threshold: float
    ) -> Optional[DataFrame]:
        """Run fuzzy similarity on one block; return DataFrame (left_id, _right_id_val, confidence) or None."""
        left_norm = left_block.with_columns(
            pl.col(field).str.to_lowercase().str.strip_chars().alias("_fuzzy_val")
        )
        right_norm = right_block.with_columns(
            pl.col(field).str.to_lowercase().str.strip_chars().alias("_fuzzy_val")
        )
        left_arrow = left_norm.select("_fuzzy_val").to_arrow()
        right_arrow = right_norm.select("_fuzzy_val").to_arrow()
        left_strings = left_arrow.column("_fuzzy_val").to_pylist()
        right_strings = right_arrow.column("_fuzzy_val").to_pylist()

        from rapidfuzz import process
        from rapidfuzz.distance import JaroWinkler
        import numpy as np

        matrix = process.cdist(
            left_strings,
            right_strings,
            scorer=JaroWinkler.similarity,
            workers=-1,
            score_cutoff=threshold,
        )
        rows, cols = np.where(matrix >= threshold)
        if len(rows) == 0:
            return None

        left_id_list = left_block.select(self.left_id).to_series().to_list()
        right_id_list = right_block.select(self.right_id).to_series().to_list()
        pairs_data = {
            self.left_id: [left_id_list[int(r)] for r in rows],
            "_right_id_val": [right_id_list[int(c)] for c in cols],
            "confidence": matrix[rows, cols].astype(float).tolist(),
        }
        return pl.DataFrame(pairs_data)
