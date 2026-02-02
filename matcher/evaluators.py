"""Evaluation components for measuring match quality.

This module provides evaluation components for measuring the quality of matching results
by comparing predicted matches against ground truth data.

Key Concepts:
- Evaluator: Abstract base class that defines the interface for evaluation components.
  All evaluators must implement the `evaluate()` method.
- SimpleEvaluator: Concrete implementation that calculates standard classification metrics:
  - Precision: TP / (TP + FP) - proportion of predicted matches that are correct
  - Recall: TP / (TP + FN) - proportion of true matches that were found
  - F1 Score: Harmonic mean of precision and recall
  - Accuracy: TP / (TP + FP + FN) - proportion of correct predictions
- Ground Truth: DataFrame with known matches, must have 'left_id' and 'right_id' columns
- Predicted Matches: DataFrame with matches from Matcher or Deduplicator

Usage Pattern:
    >>> from matcher import Matcher, SimpleEvaluator
    >>> import polars as pl
    >>>
    >>> matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    >>> results = matcher.match(rules="email")
    >>>
    >>> ground_truth = pl.DataFrame({
    ...     "left_id": ["left_1", "left_2"],
    ...     "right_id": ["right_1", "right_2"]
    ... })
    >>> metrics = results.evaluate(ground_truth)
    >>> print(f"Precision: {metrics['precision']:.2%}, Recall: {metrics['recall']:.2%}")

Column Naming:
- Ground truth must have 'left_id' and 'right_id' columns
- Predicted matches can use various column names (id, id_right, id_match, etc.)
- SimpleEvaluator automatically detects common column naming patterns

Design Notes:
- Evaluators are stateless components that can be swapped for different evaluation strategies
- Supports custom evaluators via dependency injection
- Handles different column naming conventions in predicted matches for flexibility
"""

import polars as pl
from polars import DataFrame
from abc import ABC, abstractmethod


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
