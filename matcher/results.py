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
- sample(): Return a random sample of matches (for review or inspection)
- export_for_review(): Export matches to CSV for human review (Phase 4)

Dependencies:
- Works with both Matcher and Deduplicator instances
- Uses Evaluator components (from matcher.evaluators) for evaluation
- Stores original_left DataFrame for refine operations

Design Notes:
- Immutable: operations return new MatchResults instances
- Stores original_left DataFrame to enable refine() operations
- Supports both entity resolution and deduplication workflows
"""

from pathlib import Path

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

    def sample(
        self,
        n: Optional[int] = None,
        fraction: Optional[float] = None,
        seed: Optional[int] = None,
    ) -> "MatchResults":
        """Return a random sample of matches (for review or inspection).

        Provide either n (number of rows) or fraction (proportion of rows, 0–1).
        Useful before export_for_review to send reviewers a manageable sample.

        Args:
            n: Number of rows to sample (without replacement). If n exceeds the
                number of rows, all rows are returned.
            fraction: Fraction of rows to sample (0–1), e.g. 0.1 for 10%.
            seed: Random seed for reproducibility.

        Returns:
            New MatchResults with sampled matches.

        Raises:
            ValueError: If neither n nor fraction is set, or both are set.

        Example:
            >>> results = matcher.match_fuzzy(field="name", threshold=0.85)
            >>> results.sample(n=50, seed=42).export_for_review("sample_for_review.csv")
        """
        if n is not None and fraction is not None:
            raise ValueError("Provide either n or fraction, not both.")
        if n is None and fraction is None:
            raise ValueError("Provide either n (number of rows) or fraction (0–1).")
        if n is not None and n < 0:
            raise ValueError("n must be non-negative.")
        if fraction is not None and not (0 < fraction <= 1):
            raise ValueError("fraction must be in the range (0, 1].")
        if self.matches.height == 0:
            return MatchResults(self.matches, self._original_left)
        if n is not None:
            sampled = self.matches.sample(n=min(n, self.matches.height), seed=seed)
        else:
            sampled = self.matches.sample(fraction=fraction, seed=seed)
        return MatchResults(sampled, self._original_left)

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
        ground_truth: DataFrame,
        left_id_col: str = "id",
        right_id_col: str = "id",
        evaluator: Optional["Evaluator"] = None
    ) -> dict:
        """Evaluate matches against ground truth.

        Args:
            ground_truth: DataFrame with known matches (must have left_id, right_id columns).
                         Load from CSV/Parquet yourself if needed, e.g. pl.read_csv(path).
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
        # Use default evaluator if not provided
        if evaluator is None:
            evaluator = SimpleEvaluator()

        return evaluator.evaluate(
            self.matches,
            ground_truth,
            left_id_col=left_id_col,
            right_id_col=right_id_col
        )

    def export_for_review(self, path: Union[str, Path]) -> None:
        """Export match results to CSV for human review.

        Writes the matches DataFrame to CSV so reviewers can open it in Excel
        or any spreadsheet tool. The file includes identifiers and joined
        columns so reviewers have enough context without opening other systems.
        Use sample() first to export a manageable sample, or pipe/select for
        a focused set of columns.

        Args:
            path: Output path for the CSV file (str or pathlib.Path; use .csv).

        Example:
            >>> results = matcher.match_fuzzy(field="name", threshold=0.85)
            >>> results.export_for_review("matches_for_review.csv")
            >>> # Export a sample for reviewers
            >>> results.sample(n=50, seed=42).export_for_review("sample_for_review.csv")
            >>> # Focused export: only selected columns
            >>> results.pipe(lambda df: df.select(["id", "id_right", "confidence", "name", "name_right"])).export_for_review("review.csv")
        """
        self.matches.write_csv(path)
