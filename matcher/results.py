"""Match results and operations for chaining and refinement.

This module provides the MatchResults class, which wraps match results and provides
methods for chaining operations, refining matches, and evaluating against ground truth.

Key Concepts:
- MatchResults: Wrapper class that holds match results and provides a fluent API
  for post-processing, refinement, and evaluation.
- Pipe Pattern: Allows chaining arbitrary Polars operations on match results.
- Refine: Cascading matching - apply additional rules to records that didn't match
  on previous rules. Useful for progressive matching strategies.
- Evaluate: Compare matches against ground truth to measure quality.

Usage Pattern:
    >>> from matcher import Matcher
    >>> import polars as pl
    >>>
    >>> matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    >>>
    >>> # Basic matching
    >>> results = matcher.match(rules="email")
    >>> print(f"Found {results.count} matches")
    >>>
    >>> # Chain operations (pipe pattern)
    >>> filtered = results.pipe(lambda df: df.filter(pl.col("confidence") > 0.9))
    >>>
    >>> # Cascading matching (refine)
    >>> refined = results.refine(matcher, rule=["first_name", "last_name"])
    >>>
    >>> # Evaluate against ground truth
    >>> metrics = results.evaluate(ground_truth)

Key Methods:
- count: Property returning the number of matches
- pipe(): Chain arbitrary DataFrame transformations
- refine(): Apply additional matching rules to unmatched records
- evaluate(): Compare matches against ground truth and return metrics

Dependencies:
- Works with both Matcher and Deduplicator instances
- Uses Evaluator components (from matcher.evaluators) for evaluation
- Stores original_left DataFrame for refine operations

Design Notes:
- Immutable: operations return new MatchResults instances
- Stores original_left DataFrame to enable refine() operations
- Supports both entity resolution and deduplication workflows
"""

import polars as pl
from polars import DataFrame
from typing import Union, Optional, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from matcher.matcher import Matcher
    from matcher.deduplicator import Deduplicator
    from matcher.evaluators import Evaluator

from matcher.evaluators import SimpleEvaluator


class MatchResults:
    """Match results with pipe and refine support for chaining operations."""

    def __init__(self, matches: DataFrame, original_left: Optional[DataFrame] = None):
        """Initialize with matches DataFrame.

        Args:
            matches: Polars DataFrame with match results
            original_left: Original left DataFrame (stored for refine operations)
        """
        self.matches = matches
        self._original_left = original_left

    @property
    def count(self) -> int:
        """Number of matches found."""
        return len(self.matches)

    def pipe(
        self,
        func: Callable[[DataFrame], DataFrame]
    ) -> "MatchResults":
        """Chain operations on matches (Polars pipe pattern).

        Args:
            func: Function that takes DataFrame and returns DataFrame

        Returns:
            New MatchResults with transformed matches

        Example:
            >>> results = matcher.match(rule=["email"])
            >>> filtered = results.pipe(lambda df: df.filter(pl.col("confidence") > 0.9))
        """
        return MatchResults(func(self.matches), self._original_left)

    def refine(
        self,
        matcher: Union["Matcher", "Deduplicator"],
        rule: list[str]
    ) -> "MatchResults":
        """Refine matches by applying another rule to unmatched left records (optional).

        This implements cascading matching:
        1. First match on high-confidence rule (e.g., email)
        2. Then match on lower-confidence rule (e.g., name) for unmatched records
        3. Combine all matches

        Note: This is one approach to combining matches. For probabilistic matching
        with confidence scores, you may want to use composite confidence scores instead
        of cascading (matching all rules first, then combining with weighted scores).

        Args:
            matcher: Matcher or Deduplicator instance (for access to original data and algorithm)
            rule: Matching rule to apply to unmatched records

        Returns:
            New MatchResults with combined matches (original + refined)

        Raises:
            ValueError: If matches don't have the expected ID column structure

        Example:
            >>> # First match on email
            >>> results = matcher.match(rule=["email"])
            >>> # Then match on name for records that didn't match on email
            >>> refined = results.refine(matcher, rule=["first_name", "last_name"])
        """
        # Import here to avoid circular dependency
        from matcher.deduplicator import Deduplicator

        # Get the actual Matcher instance (Deduplicator wraps one)
        if isinstance(matcher, Deduplicator):
            actual_matcher = matcher._matcher
            left_id = matcher._id_col
            right_id = matcher._id_col
            is_deduplication = True
        else:
            actual_matcher = matcher
            left_id = matcher.left_id
            right_id = matcher.right_id
            is_deduplication = False

        right_id_right = f"{right_id}_right"

        # ID column is always required (enforced at Matcher initialization)
        # This check is defensive programming
        if left_id not in self.matches.columns:
            raise ValueError(
                f"Cannot refine: matches must have '{left_id}' column to identify unmatched records. "
                "This should not happen - id columns are required at Matcher initialization."
            )

        if self._original_left is None:
            raise ValueError(
                "Cannot refine: original left data not available. "
                "Use Matcher.match() first, or pass original_left to MatchResults."
            )

        # Extract matched left IDs from current matches
        if right_id_right in self.matches.columns:
            # Extract matched left IDs
            matched_left_ids = self.matches.select(left_id).unique()
        else:
            # Fallback: if no right_id_right, assume all columns represent matches
            # This shouldn't happen with proper matching, but handle gracefully
            raise ValueError(
                f"Cannot refine: matches don't have expected structure. "
                f"Expected '{left_id}' and '{right_id_right}' columns."
            )

        # Filter left to only unmatched records (anti-join)
        unmatched_left = (
            self._original_left
            .join(matched_left_ids, left_on=left_id, right_on=left_id, how="anti")
        )

        # All records matched - return current matches
        if unmatched_left.height == 0:
            return MatchResults(self.matches, self._original_left)

        # Get right source (for deduplication, it's a copy of left; for entity resolution, it's the original right)
        right_source = actual_matcher.right

        # Apply new rule to unmatched left + right source
        new_matches = actual_matcher.matching_algorithm.match(
            left=unmatched_left,
            right=right_source,
            rule=rule,
            left_id=left_id,
            right_id=right_id
        )

        # Combine with existing matches
        if new_matches.height > 0:
            combined = pl.concat([self.matches, new_matches])
            # Deduplicate on id columns if they exist, otherwise on all columns
            if left_id in combined.columns and right_id_right in combined.columns:
                combined = combined.unique(subset=[left_id, right_id_right])
            else:
                combined = combined.unique()
        else:
            combined = self.matches

        # Filter self-matches for deduplication if needed
        if is_deduplication:
            if left_id in combined.columns and right_id_right in combined.columns:
                combined = combined.filter(
                    pl.col(left_id) != pl.col(right_id_right)
                )

        return MatchResults(combined, self._original_left)

    def evaluate(
        self,
        ground_truth: Union[DataFrame, str],
        left_id_col: str = "id",
        right_id_col: str = "id",
        evaluator: Optional["Evaluator"] = None
    ) -> dict:
        """Evaluate matches against ground truth.

        Args:
            ground_truth: DataFrame with known matches (must have left_id, right_id columns)
                         or path to parquet file
            left_id_col: Column name for left ID in matches (default: "id")
            right_id_col: Column name for right ID in matches (default: "id" or "id_right" for dedup)
            evaluator: Evaluator component (default: SimpleEvaluator)

        Returns:
            dict with precision, recall, f1, accuracy, and counts

        Example:
            >>> ground_truth = pl.DataFrame({
            ...     "left_id": ["left_1", "left_2"],
            ...     "right_id": ["right_1", "right_2"]
            ... })
            >>> results = matcher.match(rules="email")
            >>> metrics = results.evaluate(ground_truth)
            >>> print(f"Precision: {metrics['precision']:.2%}")
            >>> print(f"Recall: {metrics['recall']:.2%}")
        """
        # Load ground truth if it's a path
        if isinstance(ground_truth, str):
            ground_truth = pl.read_parquet(ground_truth)

        # Use default evaluator if not provided
        if evaluator is None:
            evaluator = SimpleEvaluator()

        return evaluator.evaluate(
            self.matches,
            ground_truth,
            left_id_col=left_id_col,
            right_id_col=right_id_col
        )
