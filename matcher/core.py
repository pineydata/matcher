"""Core matching functionality for entity resolution and deduplication.

Built with hygge philosophy: making matching feel natural, comfortable, and reliable.
"""

import polars as pl
from polars import DataFrame
from typing import Union, Optional, Callable
from abc import ABC, abstractmethod


class MatchingAlgorithm(ABC):
    """Base class for matching algorithms - in-memory only."""

    @abstractmethod
    def match(
        self,
        left: DataFrame,
        right: Optional[DataFrame],
        rule: list[str]
    ) -> DataFrame:
        """Perform matching between left and right sources for a single rule.

        This method processes a single rule (list of fields). The Matcher class
        handles sequential processing of multiple rules and combines results.

        Args:
            left: Left source DataFrame (in-memory)
            right: Right source DataFrame (in-memory). For deduplication, this will be a copy of left.
            rule: Single matching rule as list of fields (e.g., ["email"] or ["first_name", "last_name"])

        Returns:
            DataFrame with matches for this rule
        """
        pass


class ExactMatcher(MatchingAlgorithm):
    """Exact matching algorithm - unified for entity resolution and deduplication."""

    def __init__(self, max_workers: Optional[int] = None):
        """Initialize ExactMatcher.

        Args:
            max_workers: Maximum number of parallel workers for operations within a rule.
                        Defaults to None (uses CPU count). Set to 1 to disable parallelization.
        """
        self.max_workers = max_workers

    def match(
        self,
        left: DataFrame,
        right: Optional[DataFrame],
        rule: list[str]
    ) -> DataFrame:
        """Exact matching via inner join on field(s) for a single rule.

        For both entity resolution and deduplication:
        - Joins left and right on specified field(s)
        - Returns matches with id and id_right columns
        - Self-matches (id == id_right) are filtered by Matcher for deduplication

        Args:
            left: Left source DataFrame
            right: Right source DataFrame (for deduplication, this is a copy of left)
            rule: Single matching rule as list of fields (e.g., ["email"] or ["first_name", "last_name"])
                  All fields in the rule must match together (AND logic).

        Returns:
            DataFrame with matches for this rule
        """
        if right is None:
            # This shouldn't happen if Matcher handles deduplication correctly
            # But handle gracefully
            right = left

        # Normalize to format expected by Polars join
        if len(rule) == 1:
            field = rule[0]  # String for single field
        else:
            field = rule  # List for multiple fields

        # Unified join logic (works for both entity resolution and deduplication)
        result = left.join(right, on=field, how="inner", suffix="_right")

        # Ensure id_right exists for evaluation (if id column exists in right)
        if "id" in right.columns and "id_right" not in result.columns:
            # Add id_right by joining on the field(s) again
            right_ids = right.select(
                [field] if isinstance(field, str) else field,
                pl.col("id").alias("id_right")
            )
            result = result.join(right_ids, on=field, how="left", suffix="_temp")
            # Clean up if we got a temp column
            if "id_right_temp" in result.columns:
                result = result.with_columns(pl.col("id_right_temp").alias("id_right")).drop("id_right_temp")

        return result


class Matcher:
    """Main matcher class with sequential rule processing.

    In-memory only: accepts Polars DataFrames. For deduplication (right=None),
    internally treats it as entity resolution where right = left.clone(), then filters self-matches.
    """

    def __init__(
        self,
        left: DataFrame,
        right: Optional[DataFrame] = None,
        matching_algorithm: Optional[MatchingAlgorithm] = None,
        max_workers: Optional[int] = None
    ):
        """Initialize matcher with Polars DataFrames.

        Args:
            left: Polars DataFrame (in-memory) - MUST have 'id' column
            right: Polars DataFrame (in-memory) - MUST have 'id' column. If None, performs deduplication on left.
            matching_algorithm: MatchingAlgorithm component (default: ExactMatcher)
            max_workers: Maximum number of parallel workers for operations within a rule.
                        Only used if the matching algorithm supports it (e.g., ExactMatcher).
                        Defaults to None (uses CPU count). Set to 1 to disable parallelization.

        Raises:
            ValueError: If left or right DataFrames don't have 'id' column
        """
        if left.height == 0:
            raise ValueError("Left source is empty")

        # REQUIRE id column
        if "id" not in left.columns:
            raise ValueError(
                "Left source MUST have 'id' column. "
                f"Found columns: {left.columns}"
            )

        # For deduplication: create copy of left as right
        if right is None:
            right = left.clone()  # Clone for deduplication
            self._is_deduplication = True
        else:
            if right.height == 0:
                raise ValueError("Right source is empty")
            # REQUIRE id column
            if "id" not in right.columns:
                raise ValueError(
                    "Right source MUST have 'id' column. "
                    f"Found columns: {right.columns}"
                )
            self._is_deduplication = False

        self.left = left
        self.right = right  # Always set (copy of left for deduplication)

        # Initialize matching algorithm with max_workers if not provided and algorithm supports it
        if matching_algorithm is None:
            self.matching_algorithm = ExactMatcher(max_workers=max_workers)
        else:
            self.matching_algorithm = matching_algorithm
            # If algorithm supports max_workers and it's not set, set it
            if hasattr(self.matching_algorithm, 'max_workers') and max_workers is not None:
                self.matching_algorithm.max_workers = max_workers

    def match(
        self,
        rules: Union[str, list[str], list[Union[str, list[str]]]]
    ) -> "MatchResults":
        """Perform matching using the configured matching algorithm with sequential rule processing.

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
            MatchResults object with matches

        Examples:
            >>> # Single field
            >>> results = matcher.match(rules="email")
            >>> # Single rule, single field
            >>> results = matcher.match(rules=["email"])
            >>> # Single rule, multiple fields
            >>> results = matcher.match(rules=["email", "zip_code"])
            >>> # Multiple rules: match if email OR (first_name AND last_name)
            >>> results = matcher.match(rules=[
            ...     "email",
            ...     ["first_name", "last_name"]
            ... ])
        """
        # Normalize rules to list of lists
        normalized_rules = self._normalize_rules(rules)

        # Validate all fields exist
        self._validate_fields(self.left, self.right, normalized_rules)

        # Process rules sequentially (Polars parallelizes joins internally)
        all_matches = []
        for rule in normalized_rules:
            rule_matches = self.matching_algorithm.match(self.left, self.right, rule)
            all_matches.append(rule_matches)

        if not all_matches:
            # No matches - return empty DataFrame with same schema
            first_col = self.left.columns[0] if self.left.columns else "id"
            empty_result = self.left.join(self.right, on=first_col, how="inner").filter(pl.lit(False))
            return MatchResults(empty_result, original_left=self.left)

        # Combine results (OR logic)
        final_result = self._combine_matches(self.left, self.right, all_matches)

        # Filter self-matches for deduplication (id columns are always present)
        if self._is_deduplication:
            final_result = final_result.filter(
                pl.col("id") != pl.col("id_right")
            )

        return MatchResults(final_result, original_left=self.left)

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
        # Extract match pairs (ID columns) to handle different column structures
        match_pairs = []
        direct_matches = []  # For cases without id columns

        for matches_df in all_matches:
            # Entity resolution or deduplication: extract id and id_right
            if "id" in matches_df.columns and "id_right" in matches_df.columns:
                pairs = matches_df.select([
                    pl.col("id").alias("left_id"),
                    pl.col("id_right").alias("right_id")
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
            right_with_id = right.with_columns(pl.col("id").alias("id_right"))
            result = (
                left.join(combined_pairs, left_on="id", right_on="left_id", how="inner")
                .join(right_with_id, left_on="right_id", right_on="id", how="inner", suffix="_right")
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
            if "id" in combined.columns and "id_right" in combined.columns:
                final_result = combined.unique(subset=["id", "id_right"])
            else:
                final_result = combined.unique()

        return final_result


class Evaluator(ABC):
    """Base class for evaluation components."""

    @abstractmethod
    def evaluate(
        self,
        predicted: DataFrame,
        ground_truth: DataFrame,
        left_id_col: str = "id",
        right_id_col: str = "id"
    ) -> dict:
        """Evaluate predicted matches against ground truth.

        Args:
            predicted: DataFrame with predicted matches
            ground_truth: DataFrame with known matches (must have left_id, right_id columns)
            left_id_col: Column name for left ID in predicted matches
            right_id_col: Column name for right ID in predicted matches (or id_right for dedup)

        Returns:
            dict with precision, recall, f1, accuracy, and counts
        """
        pass


class SimpleEvaluator(Evaluator):
    """Simple evaluator that calculates precision, recall, F1, and accuracy."""

    def evaluate(
        self,
        predicted: DataFrame,
        ground_truth: DataFrame,
        left_id_col: str = "id",
        right_id_col: str = "id"
    ) -> dict:
        """Evaluate predicted matches against ground truth."""
        # Normalize ground truth to have left_id and right_id columns
        if "left_id" not in ground_truth.columns or "right_id" not in ground_truth.columns:
            raise ValueError(
                "Ground truth must have 'left_id' and 'right_id' columns. "
                f"Found columns: {ground_truth.columns}"
            )

        # Create normalized predicted matches DataFrame
        # Handle different column naming in predicted matches
        pred_left_col = left_id_col
        pred_right_col = right_id_col

        # For deduplication, right_id might be id_right (unified naming) or id_match (backward compat)
        if pred_right_col not in predicted.columns:
            # Try common alternatives
            if "id_right" in predicted.columns:
                pred_right_col = "id_right"
            elif "id_match" in predicted.columns:
                pred_right_col = "id_match"  # Backward compatibility
            elif f"{left_id_col}_right" in predicted.columns:
                pred_right_col = f"{left_id_col}_right"
            else:
                raise ValueError(
                    f"Could not find right ID column. Expected '{right_id_col}' or alternatives. "
                    f"Found columns: {predicted.columns}"
                )

        # Create normalized predicted pairs
        predicted_pairs = predicted.select([
            pl.col(pred_left_col).alias("pred_left_id"),
            pl.col(pred_right_col).alias("pred_right_id")
        ]).unique()

        # Create normalized ground truth pairs
        ground_truth_pairs = ground_truth.select([
            pl.col("left_id").alias("true_left_id"),
            pl.col("right_id").alias("true_right_id")
        ]).unique()

        # Convert to sets of tuples for comparison
        predicted_set = set(
            predicted_pairs.iter_rows()
        )
        ground_truth_set = set(
            ground_truth_pairs.iter_rows()
        )

        # Calculate metrics
        true_positives = len(predicted_set & ground_truth_set)
        false_positives = len(predicted_set - ground_truth_set)
        false_negatives = len(ground_truth_set - predicted_set)

        # Precision: TP / (TP + FP)
        precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0.0

        # Recall: TP / (TP + FN)
        recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0.0

        # F1: 2 * (precision * recall) / (precision + recall)
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

        # Accuracy: (TP + TN) / (TP + FP + FN + TN)
        # For matching, we don't have true negatives in the same way
        # So accuracy = TP / (TP + FP + FN) - proportion of correct predictions
        total_predictions = true_positives + false_positives + false_negatives
        accuracy = true_positives / total_predictions if total_predictions > 0 else 0.0

        return {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "accuracy": accuracy,
            "true_positives": true_positives,
            "false_positives": false_positives,
            "false_negatives": false_negatives,
            "total_predicted": len(predicted_set),
            "total_ground_truth": len(ground_truth_set),
        }


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
        matcher: "Matcher",
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
            matcher: Matcher instance (for access to original data and algorithm)
            rule: Matching rule to apply to unmatched records

        Returns:
            New MatchResults with combined matches (original + refined)

        Raises:
            ValueError: If matches don't have 'id' column (required for refine)

        Example:
            >>> # First match on email
            >>> results = matcher.match(rule=["email"])
            >>> # Then match on name for records that didn't match on email
            >>> refined = results.refine(matcher, rule=["first_name", "last_name"])
        """
        # ID column is always required (enforced at Matcher initialization)
        # This check is defensive programming
        if "id" not in self.matches.columns:
            raise ValueError(
                "Cannot refine: matches must have 'id' column to identify unmatched records. "
                "This should not happen - id columns are required at Matcher initialization."
            )

        if self._original_left is None:
            raise ValueError(
                "Cannot refine: original left data not available. "
                "Use Matcher.match() first, or pass original_left to MatchResults."
            )

        # Extract matched left IDs from current matches
        # For both entity resolution and deduplication, we use "id" and "id_right"
        # (deduplication now uses same column naming as entity resolution)
        if "id_right" in self.matches.columns:
            # Entity resolution or deduplication: extract matched left IDs
            matched_left_ids = self.matches.select("id").unique()
        else:
            # Fallback: if no id_right, assume all columns represent matches
            # This shouldn't happen with proper matching, but handle gracefully
            raise ValueError(
                "Cannot refine: matches don't have expected structure. "
                "Expected 'id' and 'id_right' columns."
            )

        # Filter left to only unmatched records (anti-join)
        unmatched_left = (
            self._original_left
            .join(matched_left_ids, left_on="id", right_on="id", how="anti")
        )

        # All records matched - return current matches
        if unmatched_left.height == 0:
            return MatchResults(self.matches, self._original_left)

        # For deduplication, right is a copy of left
        # For entity resolution, right is the original right
        right_source = matcher.right if matcher.right is not None else self._original_left

        # Apply new rule to unmatched left + right source
        new_matches = matcher.matching_algorithm.match(
            left=unmatched_left,
            right=right_source,
            rule=rule
        )

        # Combine with existing matches
        if new_matches.height > 0:
            combined = pl.concat([self.matches, new_matches])
            # Deduplicate on id and id_right (works for both entity resolution and deduplication)
            if "id_right" in combined.columns:
                combined = combined.unique(subset=["id", "id_right"])
            else:
                combined = combined.unique()
        else:
            combined = self.matches

        # Filter self-matches for deduplication if needed
        if matcher._is_deduplication:
            if "id" in combined.columns and "id_right" in combined.columns:
                combined = combined.filter(
                    pl.col("id") != pl.col("id_right")
                )

        return MatchResults(combined, self._original_left)

    def evaluate(
        self,
        ground_truth: Union[DataFrame, str],
        left_id_col: str = "id",
        right_id_col: str = "id",
        evaluator: Optional[Evaluator] = None
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
