"""Deduplication - finds duplicate records within a single source.

This module provides the Deduplicator class, a convenience wrapper for finding
duplicate records within a single data source.

Key Concepts:
- Deduplication: The process of identifying records within a single source that
  represent the same real-world entity. For example, finding duplicate customer
  records in a database.
- Self-Matches: When matching a source against itself, records will match themselves
  (id == id). Deduplicator automatically filters these out.
- Implementation: Deduplicator wraps Matcher internally, cloning the source to create
  left/right DataFrames for matching, then filtering self-matches.

Usage Pattern:
    >>> from matcher import Deduplicator
    >>> import polars as pl
    >>>
    >>> df = pl.read_parquet("customers.parquet")
    >>> deduplicator = Deduplicator(source=df, id_col="id")
    >>> results = deduplicator.match(rules="email")
    >>> print(f"Found {results.count} duplicate pairs")

Differences from Matcher:
- Takes a single source DataFrame instead of left/right
- Uses id_col for both left_id and right_id
- Automatically filters self-matches (where id == id_right)
- Same rule syntax and matching logic as Matcher

Dependencies:
- Uses Matcher (from matcher.matcher) internally for matching logic
- Uses MatchingAlgorithm components (from matcher.algorithms) for actual matching
- Returns MatchResults objects (from matcher.results) for chaining operations
"""

from polars import DataFrame
from typing import Union, Optional
from matcher.algorithms import MatchingAlgorithm
from matcher.matcher import Matcher
from matcher.results import MatchResults
import polars as pl


class Deduplicator:
    """Deduplication - finds duplicate records within a single source.

    This is a convenience wrapper around Matcher that handles:
    - Cloning the source to create left/right for matching
    - Filtering self-matches (where left_id == right_id)
    """

    def __init__(
        self,
        source: DataFrame,
        id_col: str,
        matching_algorithm: Optional[MatchingAlgorithm] = None,
        max_workers: Optional[int] = None
    ):
        """Initialize deduplicator with a single source DataFrame.

        Args:
            source: Polars DataFrame (in-memory) - MUST have id_col column
            id_col: Column name for source ID
            matching_algorithm: MatchingAlgorithm component (default: ExactMatcher)
            max_workers: Maximum number of parallel workers for operations within a rule.
                        Only used if the matching algorithm supports it (e.g., ExactMatcher).
                        Defaults to None (uses CPU count). Set to 1 to disable parallelization.

        Raises:
            ValueError: If source DataFrame doesn't have the specified ID column
        """
        # Create Matcher internally with source cloned
        self._matcher = Matcher(
            left=source,
            right=source.clone(),
            left_id=id_col,
            right_id=id_col,
            matching_algorithm=matching_algorithm,
            max_workers=max_workers
        )
        self._id_col = id_col

    def match(
        self,
        rules: Union[str, list[str], list[Union[str, list[str]]]]
    ) -> MatchResults:
        """Perform deduplication using the configured matching algorithm.

        Rules are processed sequentially and combined with OR logic.

        Args:
            rules: Matching rule(s). Can be:
                  - Single field (str): "email"
                  - Single rule with one field (list[str]): ["email"]
                  - Single rule with multiple fields (list[str]): ["first_name", "last_name"]
                  - Multiple rules (list): ["email", ["first_name", "last_name"]]

                  Records match if ANY rule matches (OR logic).
                  Within a rule, all fields must match together (AND logic).

        Returns:
            MatchResults object with duplicate pairs (self-matches filtered out)

        Examples:
            >>> # Single field
            >>> results = deduplicator.match(rules="email")
            >>> # Single rule, single field
            >>> results = deduplicator.match(rules=["email"])
            >>> # Single rule, multiple fields
            >>> results = deduplicator.match(rules=["email", "zip_code"])
            >>> # Multiple rules: match if email OR (first_name AND last_name)
            >>> results = deduplicator.match(rules=[
            ...     "email",
            ...     ["first_name", "last_name"]
            ... ])
        """
        # Delegate to Matcher
        results = self._matcher.match(rules)

        # Filter self-matches (id_col == id_col_right)
        id_col_right = f"{self._id_col}_right"
        filtered_matches = results.matches.filter(
            pl.col(self._id_col) != pl.col(id_col_right)
        )

        return MatchResults(filtered_matches, original_left=self._matcher.left)
