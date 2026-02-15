"""Matching algorithm components for entity resolution and deduplication.

This module provides the core matching algorithm interface and implementations.
Matching algorithms define how records are compared and matched based on field rules.

Key Concepts:
- MatchingAlgorithm: Abstract base class that defines the interface for matching algorithms.
  All matching algorithms must implement the `match()` method that processes a single rule.
- ExactMatcher: Concrete implementation that performs exact matching via Polars inner joins.
  Records match when all specified fields have identical values (AND logic within a rule).

Usage Context:
- Algorithms are used by Matcher and Deduplicator classes to perform the actual matching.
- Each algorithm processes one rule at a time (a rule is a list of fields).
- The Matcher class handles combining results from multiple rules (OR logic).

Design Notes:
- Algorithms operate on in-memory Polars DataFrames only.
- Algorithms are stateless components that can be swapped for different matching strategies.
- The match() method returns a DataFrame with matched records, including ID columns.
"""

import polars as pl
from polars import DataFrame
from abc import ABC, abstractmethod


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
