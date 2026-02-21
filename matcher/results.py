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
    >>> refined = results.refine(rule=["first_name", "last_name"])
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

    def __init__(
        self,
        matches: DataFrame,
        original_left: Optional[DataFrame] = None,
        source: Optional[Union["Matcher", "Deduplicator"]] = None,
    ):
        """Initialize with matches DataFrame.

        Args:
            matches: Polars DataFrame with match results
            original_left: Original left DataFrame (stored for refine operations)
            source: Matcher or Deduplicator that produced these results (enables refine(rule=...) without passing matcher)
        """
        self.matches = matches
        self._original_left = original_left
        self._source = source

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
        return MatchResults(func(self.matches), self._original_left, self._source)

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
        return MatchResults(sampled, self._original_left, self._source)

    def refine(
        self,
        rule: list[str],
        matcher: Optional[Union["Matcher", "Deduplicator"]] = None,
        blocking_key: Optional[str] = None,
    ) -> "MatchResults":
        """Refine matches by applying another rule to unmatched left records (optional).

        This implements cascading matching:
        1. First match on high-confidence rule (e.g., email)
        2. Then match on lower-confidence rule (e.g., name) for unmatched records
        3. Combine all matches

        When results come from matcher.match() or deduplicator.match(), the matcher
        is stored and you can call refine(rule=[...]) without passing matcher.

        Optional blocking_key restricts this rule to pairs that share the same value
        in that column (e.g. zip_code), reducing comparisons and memory.

        Note: This is one approach to combining matches. For probabilistic matching
        with confidence scores, you may want to use composite confidence scores instead
        of cascading (matching all rules first, then combining with weighted scores).

        Args:
            rule: Matching rule to apply to unmatched records
            matcher: Optional. Matcher or Deduplicator (only needed if results were
                    not from matcher.match() / deduplicator.match(), e.g. hand-built MatchResults)
            blocking_key: Optional column name. When set, the rule runs only within
                         blocks (unmatched left and right with the same blocking_key value).

        Returns:
            New MatchResults with combined matches (original + refined)

        Raises:
            ValueError: If matches don't have the expected ID column structure, or
                        if no matcher is available (not stored and not passed), or
                        if blocking_key is missing from left or right.

        Example:
            >>> # First match on email
            >>> results = matcher.match(rules="email")
            >>> # Then match on name for records that didn't match on email
            >>> refined = results.refine(rule=["first_name", "last_name"])
            >>> # With blocking: name match only within same zip_code
            >>> refined = results.refine(rule=["first_name", "last_name"], blocking_key="zip_code")
        """
        # Import here to avoid circular dependency
        from matcher.deduplicator import Deduplicator

        source = self._source if matcher is None else matcher
        if source is None:
            raise ValueError(
                "refine() requires a matcher. Results from matcher.match() or "
                "deduplicator.match() have it stored; otherwise pass matcher: "
                "refine(rule=[...], matcher=matcher)."
            )

        # Get the actual Matcher instance (Deduplicator wraps one)
        if isinstance(source, Deduplicator):
            actual_matcher = source._matcher
            left_id = source._id_col
            right_id = source._id_col
            is_deduplication = True
        else:
            actual_matcher = source
            left_id = source.left_id
            right_id = source.right_id
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
            # Matches must have left_id and right_id_right; fail fast with clear message
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
            return MatchResults(self.matches, self._original_left, self._source)

        # Get right source (for deduplication, it's a copy of left; for entity resolution, it's the original right)
        right_source = actual_matcher.right

        if blocking_key is not None:
            if blocking_key not in unmatched_left.columns:
                raise ValueError(
                    f"blocking_key '{blocking_key}' not found in (unmatched) left. "
                    f"Available: {unmatched_left.columns}"
                )
            if blocking_key not in right_source.columns:
                raise ValueError(
                    f"blocking_key '{blocking_key}' not found in right source. "
                    f"Available: {right_source.columns}"
                )
            blocks = actual_matcher._paired_blocks_by_key(
                unmatched_left, right_source, blocking_key
            )
            block_matches = []
            for left_block, right_block in blocks:
                block_result = actual_matcher.matching_algorithm.match(
                    left_block, right_block, rule, left_id, right_id
                )
                block_matches.append(block_result)
            new_matches = actual_matcher._combine_matches(
                unmatched_left, right_source, block_matches
            )
        else:
            new_matches = actual_matcher.matching_algorithm.match(
                left=unmatched_left,
                right=right_source,
                rule=rule,
                left_id=left_id,
                right_id=right_id
            )

        # Combine by (left_id, right_id_right) then rejoin to get consistent schema
        # (algorithm may return different columns for different rules, e.g. multi-field join)
        if new_matches.height > 0:
            existing_pairs = self.matches.select([
                pl.col(left_id).alias("_lid"),
                pl.col(right_id_right).alias("_rid"),
            ]).unique()
            new_pairs = new_matches.select([
                pl.col(left_id).alias("_lid"),
                pl.col(right_id_right).alias("_rid"),
            ]).unique()
            combined_pairs = pl.concat([existing_pairs, new_pairs]).unique()
            right_with_suffix = right_source.with_columns(
                pl.col(right_id).alias(right_id_right)
            )
            combined = actual_matcher.left.join(
                combined_pairs, left_on=left_id, right_on="_lid", how="inner"
            ).join(
                right_with_suffix,
                left_on="_rid",
                right_on=right_id,
                how="inner",
                suffix="_right",
            )
            to_drop = [c for c in ["_lid", "_rid"] if c in combined.columns]
            if to_drop:
                combined = combined.drop(to_drop)
        else:
            combined = self.matches

        # Filter self-matches for deduplication if needed
        if is_deduplication:
            if left_id in combined.columns and right_id_right in combined.columns:
                combined = combined.filter(
                    pl.col(left_id) != pl.col(right_id_right)
                )

        return MatchResults(combined, self._original_left, self._source)

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
