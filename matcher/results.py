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
    >>> results = matcher.match(on="email")
    >>> print(f"Found {results.count} matches")
    >>>
    >>> # Chain operations (pipe pattern)
    >>> filtered = results.pipe(lambda df: df.filter(pl.col("confidence") > 0.9))
    >>>
    >>> # Cascading (refine)
    >>> refined = results.refine(on=["first_name", "last_name"])
    >>>
    >>> # Evaluate against ground truth
    >>> metrics = results.evaluate(ground_truth)

Key Methods:
- count: Property returning the number of matches
- pipe(): Chain arbitrary DataFrame transformations
- refine(): Apply additional matching rules to unmatched records
- union(): Combine with other MatchResults (same source); pair set is union; score/on preserved per run (first run → kind_score/kind_on, later runs → kind_score_2/kind_on_2, etc.; no coalescing)
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
            source: Matcher or Deduplicator that produced these results (enables refine(on=...) without passing matcher)
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
            >>> results = matcher.match(on="email")
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
            >>> results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))
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
            return MatchResults(self.matches, self._original_left, self._source)
        if n is not None:
            sampled = self.matches.sample(n=min(n, self.matches.height), seed=seed)
        else:
            sampled = self.matches.sample(fraction=fraction, seed=seed)
        return MatchResults(sampled, self._original_left, self._source)

    def refine(
        self,
        on: Union[str, list[str]],
        matcher: Optional[Union["Matcher", "Deduplicator"]] = None,
        blocking_key: Optional[Union[str, list[str]]] = None,
    ) -> "MatchResults":
        """Refine matches by matching on another rule for unmatched left records.

        Cascade: first match(on=...), then refine(on=...) for unmatched rows.

        When results come from matcher.match() or deduplicator.match(), the matcher
        is stored and you can call refine(on=[...]) without passing matcher.

        Args:
            on: What to match on for unmatched records (str or list[str]).
            matcher: Optional. Only needed if results were not from match().
            blocking_key: Optional column name or list of names. Restricts this step to same block(s).

        Returns:
            New MatchResults with combined matches (original + refined).

        Example:
            >>> results = matcher.match(on="email")
            >>> refined = results.refine(on=["first_name", "last_name"])
            >>> refined = results.refine(on=["first_name", "last_name"], blocking_key="zip_code")
        """
        from matcher.deduplicator import Deduplicator

        rule_list = [on] if isinstance(on, str) else list(on)
        if not rule_list:
            raise ValueError("refine(on=...) must be a non-empty string or list of field names")

        source = self._source if matcher is None else matcher
        if source is None:
            raise ValueError(
                "refine() requires a matcher. Results from matcher.match() or "
                "deduplicator.match() have it stored; otherwise pass matcher: "
                "refine(on=[...], matcher=matcher)."
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
            keys = actual_matcher._normalize_blocking_keys(blocking_key)
            assert keys is not None  # only None when blocking_key is None
            for k in keys:
                if k not in unmatched_left.columns:
                    raise ValueError(
                        f"blocking_key '{k}' not found in (unmatched) left. "
                        f"Available: {unmatched_left.columns}"
                    )
                if k not in right_source.columns:
                    raise ValueError(
                        f"blocking_key '{k}' not found in right source. "
                        f"Available: {right_source.columns}"
                    )
            blocks = actual_matcher._paired_blocks_by_key(
                unmatched_left, right_source, keys
            )
            block_matches = []
            for left_block, right_block in blocks:
                block_result = actual_matcher.matching_algorithm.match(
                    left_block, right_block, rule_list, left_id, right_id
                )
                block_matches.append(block_result)
            new_matches = actual_matcher._combine_matches(
                unmatched_left, right_source, block_matches
            )
        else:
            new_matches = actual_matcher.matching_algorithm.match(
                left=unmatched_left,
                right=right_source,
                rule=rule_list,
                left_id=left_id,
                right_id=right_id
            )

        # Add this algorithm's score/on columns to new matches (for union/provenance)
        from matcher.matcher import _add_provenance_columns
        from matcher.algorithms import is_score_on_column
        algo = actual_matcher.matching_algorithm
        new_matches = _add_provenance_columns(new_matches, on, algo)

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
            # Merge provenance: first non-null per pair from existing then new.
            score_on_cols = [c for c in new_matches.columns if is_score_on_column(c)]
            existing_score_on = [c for c in self.matches.columns if is_score_on_column(c)]
            all_score_on = sorted(set(score_on_cols) | set(existing_score_on))
            if all_score_on:
                # Build one (left_id, right_id_right, ...score/on) table then join once.
                existing_scores = (
                    self.matches.select([left_id, right_id_right] + existing_score_on)
                    if existing_score_on
                    else None
                )
                new_scores = (
                    new_matches.select([left_id, right_id_right] + score_on_cols)
                    if score_on_cols
                    else None
                )
                if existing_scores is not None and new_scores is not None:
                    merged = existing_scores.join(
                        new_scores, on=[left_id, right_id_right], how="full", suffix="_new"
                    )
                    # Full join: rows from right-only have null keys from left; coalesce so every row has keys
                    key_new_left = f"{left_id}_new"
                    key_new_right = f"{right_id_right}_new"
                    if key_new_left not in merged.columns or key_new_right not in merged.columns:
                        raise AssertionError(
                            "Full join with suffix=_new should produce key_new columns; "
                            "cannot coalesce join keys for refine provenance."
                        )
                    merged = merged.with_columns(
                        pl.coalesce(pl.col(left_id), pl.col(key_new_left)).alias(left_id),
                        pl.coalesce(pl.col(right_id_right), pl.col(key_new_right)).alias(right_id_right),
                    )
                    merged = merged.select(
                        [left_id, right_id_right]
                        + [
                            pl.coalesce(pl.col(c), pl.col(f"{c}_new")).alias(c)
                            if f"{c}_new" in merged.columns
                            else pl.col(c)
                            for c in all_score_on
                        ]
                    )
                    combined = combined.join(merged, on=[left_id, right_id_right], how="left")
                elif existing_scores is not None:
                    combined = combined.join(existing_scores, on=[left_id, right_id_right], how="left")
                else:
                    combined = combined.join(new_scores, on=[left_id, right_id_right], how="left")
            elif existing_score_on:
                # New run added no score/on (e.g. custom algo); preserve existing provenance.
                existing_scores = self.matches.select([left_id, right_id_right] + existing_score_on)
                combined = combined.join(existing_scores, on=[left_id, right_id_right], how="left")
        else:
            combined = self.matches

        # Filter self-matches for deduplication if needed
        if is_deduplication:
            if left_id in combined.columns and right_id_right in combined.columns:
                combined = combined.filter(
                    pl.col(left_id) != pl.col(right_id_right)
                )

        return MatchResults(combined, self._original_left, self._source)

    def union(self, *others: "MatchResults") -> "MatchResults":
        """Combine this MatchResults with one or more others; pair set is the union, deduplicated.

        All inputs must share the same original_left and same right (same matcher/source)
        and same ID column names. Score/on columns are preserved per run: when the same
        algorithm type (e.g. exact) appears in multiple inputs, the first run gets
        exact_score/exact_on, the second gets exact_score_2/exact_on_2, and so on.
        No coalescing: you see every run's values and can apply your own rule.

        Returns:
            New MatchResults with rows = union of (left_id, right_id_right), canonical
            left + right columns, and score/on columns (with _2, _3 suffixes for
            multiple runs of the same algorithm type). Nulls where a pair did not
            appear in that run.
        """
        from collections import defaultdict

        from matcher.algorithms import kind_of_score_on_column
        from matcher.deduplicator import Deduplicator

        all_results = [self] + list(others)
        if not all_results:
            return MatchResults(self.matches, self._original_left, self._source)

        # Require same source so we have same right and ID names
        source = self._source
        if source is None:
            raise ValueError(
                "union() requires a matcher/source on all MatchResults (from matcher.match() or deduplicator.match())."
            )
        for i, r in enumerate(all_results):
            if r._source is not source:
                raise ValueError(
                    "union() requires all MatchResults to share the same source (same matcher/deduplicator)."
                )
            if r._original_left is not self._original_left:
                raise ValueError(
                    "union() requires all MatchResults to share the same original_left."
                )

        if isinstance(source, Deduplicator):
            left_id = source._id_col
            right_id = source._id_col
            right_df = source._matcher.right
        else:
            left_id = source.left_id
            right_id = source.right_id
            right_df = source.right
        right_id_right = f"{right_id}_right"

        # Require ID columns in all
        for r in all_results:
            if left_id not in r.matches.columns or right_id_right not in r.matches.columns:
                raise ValueError(
                    f"union() requires matches to have '{left_id}' and '{right_id_right}' columns."
                )

        # Union of pairs (deduplicate by left_id, right_id_right)
        pair_dfs = [
            r.matches.select(pl.col(left_id).alias("_lid"), pl.col(right_id_right).alias("_rid"))
            for r in all_results
        ]
        combined_pairs = pl.concat(pair_dfs).unique(subset=["_lid", "_rid"])

        if combined_pairs.height == 0:
            # All empty: return empty with same schema as self (canonical + any score/on)
            return MatchResults(self.matches, self._original_left, self._source)

        # Rejoin to get canonical left + right columns
        right_with_suffix = right_df.with_columns(pl.col(right_id).alias(right_id_right))
        combined = self._original_left.join(
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

        # Group by algorithm kind: for each kind, ordered list of result indices that have it
        runs_per_kind = defaultdict(list)
        for i, r in enumerate(all_results):
            kinds_in_r = set()
            for c in r.matches.columns:
                k = kind_of_score_on_column(c)
                if k and f"{k}_score" in r.matches.columns and f"{k}_on" in r.matches.columns:
                    kinds_in_r.add(k)
            for k in kinds_in_r:
                runs_per_kind[k].append(i)

        # For each (kind, run_index j), add columns: first run -> kind_score, kind_on; later -> kind_score_2, kind_on_2, ...
        for kind in sorted(runs_per_kind.keys()):
            result_indices = runs_per_kind[kind]
            score_col = f"{kind}_score"
            on_col = f"{kind}_on"
            for j, result_idx in enumerate(result_indices):
                suffix = "" if j == 0 else f"_{j + 1}"  # first run unsuffixed, then _2, _3, ...
                target_score = score_col + suffix
                target_on = on_col + suffix
                r = all_results[result_idx]
                sub = r.matches.select(
                    pl.col(left_id),
                    pl.col(right_id_right),
                    pl.col(score_col).alias(target_score),
                    pl.col(on_col).alias(target_on),
                )
                combined = combined.join(sub, on=[left_id, right_id_right], how="left")

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
            >>> results = matcher.match(on="email")
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
            >>> results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))
            >>> results.export_for_review("matches_for_review.csv")
            >>> # Export a sample for reviewers
            >>> results.sample(n=50, seed=42).export_for_review("sample_for_review.csv")
            >>> # Focused export: only selected columns
            >>> results.pipe(lambda df: df.select(["id", "id_right", "confidence", "name", "name_right"])).export_for_review("review.csv")
        """
        self.matches.write_csv(path)
