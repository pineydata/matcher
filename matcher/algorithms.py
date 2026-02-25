"""Matching algorithm components for entity resolution and deduplication.

This module provides the core matching algorithm interface and implementations.
Matching algorithms define how records are compared and matched based on field rules.

Key Concepts:
- MatchingAlgorithm: Abstract base class that defines the interface for matching algorithms.
  All matching algorithms must implement the `match()` method that processes a single rule.
- ExactMatcher: Concrete implementation that performs exact matching via Polars inner joins.
  Records match when all specified fields have identical values (AND logic within a rule).
- FuzzyMatcher: Fuzzy matching on a single string field using Jaro-Winkler similarity (rapidfuzz).
  Returns matches with a 'confidence' column. Rule must be a single field name.
  For "fuzzy on A then exact on B", use FuzzyMatcher then filter or MatchResults.require_exact()
  (composition), not a separate blended algorithm.

Usage Context:
- Algorithms are used by Matcher and Deduplicator classes to perform the actual matching.
- Each algorithm processes one rule at a time (a rule is a list of fields).
- The Matcher class handles combining results from multiple rules (OR logic).
- Use Matcher.match(on=[field], matching_algorithm=FuzzyMatcher(threshold=...)).

Design Notes:
- Algorithms operate on in-memory Polars DataFrames only.
- Algorithms are stateless components that can be swapped for different matching strategies.
- The match() method returns a DataFrame with matched records, including ID columns.
"""

import polars as pl
from polars import DataFrame
from abc import ABC, abstractmethod
from typing import Optional


class MatchingAlgorithm(ABC):
    """Base class for matching algorithms - in-memory only."""

    @abstractmethod
    def match(
        self,
        left: DataFrame,
        right: DataFrame,
        rule: list[str],
        left_id: str,
        right_id: str
    ) -> DataFrame:
        """Perform matching between left and right sources for a single rule.

        This method processes a single rule (list of fields). The Matcher class
        handles sequential processing of multiple rules and combines results.

        Args:
            left: Left source DataFrame (in-memory)
            right: Right source DataFrame (in-memory)
            rule: Single matching rule as list of fields (e.g., ["email"] or ["first_name", "last_name"])
            left_id: Column name for left source ID
            right_id: Column name for right source ID

        Returns:
            DataFrame with matches for this rule
        """
        pass


class ExactMatcher(MatchingAlgorithm):
    """Exact matching algorithm for entity resolution.

    Parallelization is handled internally by Polars (joins are parallelized).
    """

    def __init__(self):
        """Initialize ExactMatcher."""
        pass

    def match(
        self,
        left: DataFrame,
        right: DataFrame,
        rule: list[str],
        left_id: str,
        right_id: str
    ) -> DataFrame:
        """Exact matching via inner join on field(s) for a single rule.

        Null handling: Polars inner joins exclude null values. Rows where any
        join key is null do not match (including null-to-null). Fill or drop
        nulls in join columns beforehand if you need different behavior.

        Args:
            left: Left source DataFrame
            right: Right source DataFrame
            rule: Single matching rule as list of fields (e.g., ["email"] or ["first_name", "last_name"])
                  All fields in the rule must match together (AND logic).
            left_id: Column name for left source ID
            right_id: Column name for right source ID

        Returns:
            DataFrame with matches for this rule, including left_id and {right_id}_right columns.
            When a join key equals right_id, Polars may not add a separate right_id_right column;
            this method adds it so the result always includes right_id_right for evaluation.
        """
        # Normalize to format expected by Polars join
        if len(rule) == 1:
            field = rule[0]  # String for single field
        else:
            field = rule  # List for multiple fields

        # Join left and right on specified field(s)
        result = left.join(right, on=field, how="inner", suffix="_right")

        # When a join key equals right_id, Polars may not add right_id_right; add it so
        # the result always has right_id_right for evaluation.
        right_id_right = f"{right_id}_right"
        if right_id in right.columns and right_id_right not in result.columns:
            # Add right_id_right by joining on the field(s) again
            right_ids = right.select(
                [field] if isinstance(field, str) else field,
                pl.col(right_id).alias(right_id_right)
            )
            result = result.join(right_ids, on=field, how="left", suffix="_temp")
            # Clean up if we got a temp column
            temp_col = f"{right_id_right}_temp"
            if temp_col in result.columns:
                result = result.with_columns(pl.col(temp_col).alias(right_id_right)).drop(temp_col)

        return result


class FuzzyMatcher(MatchingAlgorithm):
    """Fuzzy matching on a single string field using Jaro-Winkler similarity.

    Uses rapidfuzz (process.cdist, JaroWinkler). Rule must be a single field name
    (single-element list). Multiple rules with FuzzyMatcher are not supported:
    use one rule per call; for "fuzzy then exact on another column" use
    match with FuzzyMatcher then filter or require_exact on MatchResults (composition).
    Rows where the field is null are excluded. Returns matches with a 'confidence' column.
    """

    def __init__(self, threshold: float = 0.85):
        """Initialize with similarity threshold in [0, 1]."""
        if not 0 <= threshold <= 1:
            raise ValueError(f"threshold must be between 0 and 1, got {threshold}")
        self.threshold = threshold

    def match(
        self,
        left: DataFrame,
        right: DataFrame,
        rule: list[str],
        left_id: str,
        right_id: str,
    ) -> DataFrame:
        """Fuzzy match on the single field in rule; return full joined DataFrame with confidence."""
        if len(rule) != 1:
            raise ValueError(
                f"FuzzyMatcher expects a single-field rule, got {len(rule)} fields: {rule}"
            )
        field = rule[0]
        if field not in left.columns or field not in right.columns:
            raise ValueError(
                f"Fuzzy field '{field}' must exist in both left and right DataFrames"
            )
        if left.schema[field] != pl.Utf8 or right.schema[field] != pl.Utf8:
            raise ValueError(
                f"Fuzzy field '{field}' must be string (Utf8) in both left and right"
            )

        left_valid = left.filter(pl.col(field).is_not_null())
        right_valid = right.filter(pl.col(field).is_not_null())
        if left_valid.height == 0 or right_valid.height == 0:
            return self._empty_result(left, right, left_id, right_id)

        pairs_df = self._fuzzy_pairs(left_valid, right_valid, field, left_id, right_id)
        if pairs_df is None or pairs_df.height == 0:
            return self._empty_result(left, right, left_id, right_id)

        right_id_right = f"{right_id}_right"
        right_with_suffix = right.with_columns(
            pl.col(right_id).alias(right_id_right)
        )
        result = (
            pairs_df.join(left, on=left_id, how="left")
            .join(
                right_with_suffix,
                left_on="_right_id_val",
                right_on=right_id,
                how="left",
                suffix="_right",
            )
            .drop("_right_id_val")
        )
        return result

    def _fuzzy_pairs(
        self,
        left: DataFrame,
        right: DataFrame,
        field: str,
        left_id: str,
        right_id: str,
    ) -> Optional[DataFrame]:
        """Return DataFrame (left_id, _right_id_val, confidence) or None."""
        left_norm = left.with_columns(
            pl.col(field).str.to_lowercase().str.strip_chars().alias("_fuzzy_val")
        )
        right_norm = right.with_columns(
            pl.col(field).str.to_lowercase().str.strip_chars().alias("_fuzzy_val")
        )
        left_strings = left_norm.select("_fuzzy_val").to_series().to_list()
        right_strings = right_norm.select("_fuzzy_val").to_series().to_list()

        from rapidfuzz import process
        from rapidfuzz.distance import JaroWinkler
        import numpy as np

        matrix = process.cdist(
            left_strings,
            right_strings,
            scorer=JaroWinkler.similarity,
            workers=-1,
            score_cutoff=self.threshold,
        )
        rows, cols = np.where(matrix >= self.threshold)
        if len(rows) == 0:
            return None

        left_id_list = left.select(left_id).to_series().to_list()
        right_id_list = right.select(right_id).to_series().to_list()
        pairs_data = {
            left_id: [left_id_list[int(r)] for r in rows],
            "_right_id_val": [right_id_list[int(c)] for c in cols],
            "confidence": matrix[rows, cols].astype(float).tolist(),
        }
        return pl.DataFrame(pairs_data)

    def _empty_result(
        self, left: DataFrame, right: DataFrame, left_id: str, right_id: str
    ) -> DataFrame:
        """Return zero-row DataFrame with schema compatible with non-empty fuzzy result."""
        right_id_right = f"{right_id}_right"
        empty_pairs = pl.DataFrame(
            {left_id: [], "_right_id_val": [], "confidence": []},
            schema={
                left_id: left.schema[left_id],
                "_right_id_val": right.schema[right_id],
                "confidence": pl.Float64,
            },
        )
        right_with_suffix = right.with_columns(
            pl.col(right_id).alias(right_id_right)
        )
        return (
            empty_pairs.join(left, on=left_id, how="left")
            .join(
                right_with_suffix,
                left_on="_right_id_val",
                right_on=right_id,
                how="left",
                suffix="_right",
            )
            .drop("_right_id_val")
        )
