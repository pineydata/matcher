"""Core matching functionality for entity resolution and deduplication."""

import polars as pl
from polars import DataFrame
from typing import Union, Optional
from abc import ABC, abstractmethod


class DataLoader(ABC):
    """Base class for data loading components."""

    @abstractmethod
    def load(self, source: Union[str, DataFrame]) -> DataFrame:
        """Load data from source.

        Args:
            source: Path to file or DataFrame

        Returns:
            Loaded DataFrame
        """
        pass


class ParquetLoader(DataLoader):
    """Load data from parquet files or use DataFrame directly."""

    def load(self, source: Union[str, DataFrame]) -> DataFrame:
        """Load from parquet file or return DataFrame as-is."""
        if isinstance(source, str):
            return pl.read_parquet(source)
        return source


class MatchingAlgorithm(ABC):
    """Base class for matching algorithms."""

    @abstractmethod
    def match(
        self,
        left: DataFrame,
        right: Optional[DataFrame],
        field: str
    ) -> DataFrame:
        """Perform matching between left and right sources.

        Args:
            left: Left source DataFrame
            right: Right source DataFrame (None for deduplication)
            field: Field name to match on

        Returns:
            DataFrame with matches
        """
        pass


class ExactMatcher(MatchingAlgorithm):
    """Exact matching algorithm - joins on field value."""

    def match(
        self,
        left: DataFrame,
        right: Optional[DataFrame],
        field: str
    ) -> DataFrame:
        """Exact matching via inner join on field.

        For entity resolution (right is not None):
        - Joins left and right DataFrames on the specified field
        - Columns with duplicate names (except join key) are automatically
          suffixed: left side keeps original name, right side gets "_right" suffix
        - Example: if both have "id" column, result has "id" (from left) and "id_right" (from right)

        For deduplication (right is None):
        - Self-joins the DataFrame on the specified field
        - Excludes self-matches (same record matched to itself)
        - All columns from matched records are suffixed with "_match"
        """
        if right is not None:
            # Entity resolution: cross-source join
            # Polars automatically handles column naming: duplicate columns
            # (except join key) get "_right" suffix on the right side
            return left.join(right, on=field, how="inner")
        else:
            # Deduplication: self-join (exclude same record)
            # Add row indices to track original positions for self-match filtering
            # This works regardless of whether an "id" column exists
            left_with_idx = left.with_row_index("_row_idx")

            matches = left_with_idx.join(
                left_with_idx,
                on=field,
                how="inner",
                suffix="_match"
            )

            # Exclude self-matches: filter where row indices differ
            # This ensures we never match a record to itself, even if there's no "id" column
            matches = matches.filter(pl.col("_row_idx") != pl.col("_row_idx_match"))

            # Drop temporary row indices
            matches = matches.drop("_row_idx", "_row_idx_match")

            return matches


class Matcher:
    """Main matcher class that composes data loading and matching components."""

    def __init__(
        self,
        left_source: Union[str, DataFrame],
        right_source: Optional[Union[str, DataFrame]] = None,
        data_loader: Optional[DataLoader] = None,
        matching_algorithm: Optional[MatchingAlgorithm] = None
    ):
        """Initialize matcher with data sources and components.

        Args:
            left_source: Path to parquet file or Polars DataFrame
            right_source: Path to parquet file or Polars DataFrame.
                        If None, performs deduplication on left_source.
            data_loader: DataLoader component (default: ParquetLoader)
            matching_algorithm: MatchingAlgorithm component (default: ExactMatcher)
        """
        # Use default components if not provided
        self.data_loader = data_loader or ParquetLoader()
        self.matching_algorithm = matching_algorithm or ExactMatcher()

        # Load data using loader component
        self.left = self.data_loader.load(left_source)
        self.right = self.data_loader.load(right_source) if right_source is not None else None

        self._validate_sources()

    def _validate_sources(self):
        """Basic validation."""
        if self.left.height == 0:
            raise ValueError("Left source is empty")
        if self.right is not None and self.right.height == 0:
            raise ValueError("Right source is empty")

    def match_exact(self, field: str) -> "MatchResults":
        """Exact matching on single field. Field must exist in both sources.

        Args:
            field: Field name (must exist in both sources with same name)

        Returns:
            MatchResults object with matches
        """
        if field not in self.left.columns:
            available = ", ".join(self.left.columns)
            raise ValueError(
                f"Field '{field}' not found in left source. Available: {available}"
            )

        if self.right is not None:
            if field not in self.right.columns:
                available = ", ".join(self.right.columns)
                raise ValueError(
                    f"Field '{field}' not found in right source. Available: {available}"
                )

        # Use matching algorithm component
        matches = self.matching_algorithm.match(
            self.left,
            self.right,
            field
        )

        return MatchResults(matches)


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
            right_id_col: Column name for right ID in predicted matches (or id_match for dedup)

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

        # For deduplication, right_id might be id_match
        if pred_right_col not in predicted.columns:
            # Try common alternatives
            if "id_match" in predicted.columns:
                pred_right_col = "id_match"
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
    """Simple result object with matches."""

    def __init__(self, matches: DataFrame):
        """Initialize with matches DataFrame.

        Args:
            matches: Polars DataFrame with match results
        """
        self.matches = matches

    @property
    def count(self) -> int:
        """Number of matches found."""
        return len(self.matches)

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
            right_id_col: Column name for right ID in matches (default: "id" or "id_match" for dedup)
            evaluator: Evaluator component (default: SimpleEvaluator)

        Returns:
            dict with precision, recall, f1, accuracy, and counts

        Example:
            >>> ground_truth = pl.DataFrame({
            ...     "left_id": ["left_1", "left_2"],
            ...     "right_id": ["right_1", "right_2"]
            ... })
            >>> results = matcher.match_exact(field="email")
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