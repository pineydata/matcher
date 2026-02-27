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
    >>> results = deduplicator.match(match_on="email")
    >>> print(f"Found {results.count} duplicate pairs")
    >>> # Fuzzy: pass FuzzyMatcher for this call
    >>> from matcher import FuzzyMatcher
    >>> fuzzy_results = deduplicator.match(match_on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))

Differences from Matcher:
- Takes a single source DataFrame instead of left/right
- Uses id_col for both left_id and right_id
- Automatically filters self-matches (where id == id_right)
- Same match() API: match_on, block_on, matching_algorithm (single entry point)

Dependencies:
- Uses Matcher (from matcher.matcher) internally for matching logic
- Uses MatchingAlgorithm components (from matcher.algorithms) for actual matching
- Returns MatchResults objects (from matcher.results) for chaining operations
"""

from polars import DataFrame
from typing import Union, Optional
from matcher.algorithms import MatchingAlgorithm
from matcher.matcher import Matcher
from matcher.oom import warn_deduplicator_source_size
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
        matching_algorithm: Optional[MatchingAlgorithm] = None
    ):
        """Initialize deduplicator with a single source DataFrame.

        Args:
            source: Polars DataFrame (in-memory) - MUST have id_col column
            id_col: Column name for source ID
            matching_algorithm: MatchingAlgorithm component (default: ExactMatcher).
                               Polars handles parallelization internally for joins.

        Raises:
            ValueError: If source DataFrame doesn't have the specified ID column
        """
        warn_deduplicator_source_size(source)

        # Create Matcher internally with source cloned
        self._matcher = Matcher(
            left=source,
            right=source.clone(),
            left_id=id_col,
            right_id=id_col,
            matching_algorithm=matching_algorithm
        )
        self._id_col = id_col

    def match(
        self,
        match_on: Union[str, list[str]],
        block_on: Optional[Union[str, list[str]]] = None,
        matching_algorithm: Optional[MatchingAlgorithm] = None,
    ) -> MatchResults:
        """Perform deduplication for a single rule. Self-matches are filtered out.

        Same API as Matcher.match(): single rule (match_on=), optional block_on (str or list[str]), optional
        matching_algorithm. For cascading, chain with .refine(match_on=...) on the result.

        Args:
            match_on: Single rule (str or list[str]).
            block_on: Optional column name or list of names.
            matching_algorithm: Optional (e.g. FuzzyMatcher(threshold=0.85)).

        Returns:
            MatchResults with duplicate pairs (self-matches excluded).
        """
        id_col_right = f"{self._id_col}_right"
        results = self._matcher.match(
            match_on=match_on,
            block_on=block_on,
            matching_algorithm=matching_algorithm,
        )
        filtered_matches = results.matches.filter(
            pl.col(self._id_col) != pl.col(id_col_right)
        )
        return MatchResults(filtered_matches, original_left=self._matcher.left, source=self)
