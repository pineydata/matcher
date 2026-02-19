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
- find_best_threshold: Sweeps confidence thresholds on fuzzy match results and returns the
  threshold that maximizes F1 (data-driven threshold tuning).
"""

import polars as pl
from polars import DataFrame
from abc import ABC, abstractmethod
from typing import Optional, List


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
        """Evaluate predicted matches against ground truth.

        Right ID column resolution (explicit, documented fallback): If right_id_col
        is not present in predicted columns, the evaluator tries in order:
        'id_right', 'id_match', then '{left_id_col}_right'. If none exist,
        ValueError is raised. Pass the actual column name (e.g. right_id_col="id_right")
        to avoid ambiguity and make behavior explicit.
        """
        # Normalize ground truth to have left_id and right_id columns
        if "left_id" not in ground_truth.columns or "right_id" not in ground_truth.columns:
            raise ValueError(
                "Ground truth must have 'left_id' and 'right_id' columns. "
                f"Found columns: {ground_truth.columns}"
            )

        # Create normalized predicted matches DataFrame
        # Resolve right ID column: when left and right id share the same name (e.g. both "id"),
        # prefer the suffixed column (e.g. "id_right") so we don't use left ids for both sides.
        pred_left_col = left_id_col
        pred_right_col = right_id_col
        suffixed = f"{left_id_col}_right"

        if pred_right_col == left_id_col and suffixed in predicted.columns:
            pred_right_col = suffixed
        elif pred_right_col not in predicted.columns:
            alternatives = ["id_right", "id_match", suffixed]
            for alt in alternatives:
                if alt in predicted.columns:
                    pred_right_col = alt
                    break
            else:
                raise ValueError(
                    f"Could not find right ID column. Tried '{right_id_col}', then "
                    f"{alternatives}. Found columns: {predicted.columns}"
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


def find_best_threshold(
    matches: DataFrame,
    ground_truth: DataFrame,
    left_id_col: str = "id",
    right_id_col: str = "id",
    evaluator: Optional[Evaluator] = None,
    thresholds: Optional[List[float]] = None,
) -> dict:
    """Find the confidence threshold that maximizes F1 for fuzzy match results.

    Sweeps thresholds over the matches' confidence column, at each step keeps only
    pairs with confidence >= threshold, then evaluates with the given evaluator.
    Returns the threshold and metrics that give the highest F1, plus the full curve
    for optional plotting.

    Use this when you have fuzzy MatchResults and ground truth and want to choose
    a threshold from data instead of guessing (e.g. 0.85).

    Args:
        matches: DataFrame of fuzzy matches with a 'confidence' column (from match_fuzzy).
        ground_truth: DataFrame with left_id and right_id columns (known true pairs).
        left_id_col: Column name for left ID in matches (default: "id").
        right_id_col: Column name for right ID in matches (default: "id" or "id_right").
        evaluator: Evaluator to use (default: SimpleEvaluator()).
        thresholds: List of thresholds to try (default: 0.50, 0.55, ..., 1.00).

    Returns:
        dict with:
        - best_threshold: threshold that achieved best F1
        - best_f1, best_precision, best_recall: metrics at best threshold
        - curve: list of dicts {threshold, precision, recall, f1} for each threshold

    Example:
        >>> # Run fuzzy with a low threshold so you have scored pairs to sweep
        >>> results = matcher.match_fuzzy(field="name", threshold=0.5)
        >>> best = find_best_threshold(results.matches, ground_truth, right_id_col="id_right")
        >>> print(f"Best threshold: {best['best_threshold']}, F1: {best['best_f1']:.2%}")
    """
    if "confidence" not in matches.columns:
        raise ValueError(
            "find_best_threshold requires a 'confidence' column (use match_fuzzy results). "
            f"Columns: {matches.columns}"
        )
    if evaluator is None:
        evaluator = SimpleEvaluator()
    if thresholds is None:
        thresholds = [round(0.5 + i * 0.05, 2) for i in range(11)]  # 0.50 .. 1.00
    else:
        if not isinstance(thresholds, (list, tuple)):
            raise TypeError(
                "thresholds must be a non-empty list or tuple of numeric values in [0.0, 1.0]."
            )
        if len(thresholds) == 0:
            raise ValueError("thresholds must be a non-empty list or tuple.")
        validated = []
        for t in thresholds:
            if not isinstance(t, (int, float)):
                raise TypeError(
                    "All threshold values must be numeric (int or float) in [0.0, 1.0]."
                )
            t_float = float(t)
            if not 0.0 <= t_float <= 1.0:
                raise ValueError(
                    f"Threshold value {t} is out of range; expected values in [0.0, 1.0]."
                )
            validated.append(t_float)
        thresholds = validated

    curve = []
    best_f1 = -1.0
    best_threshold = None
    best_precision = None
    best_recall = None

    for t in thresholds:
        filtered = matches.filter(pl.col("confidence") >= t)
        metrics = evaluator.evaluate(
            filtered,
            ground_truth,
            left_id_col=left_id_col,
            right_id_col=right_id_col,
        )
        curve.append({
            "threshold": t,
            "precision": metrics["precision"],
            "recall": metrics["recall"],
            "f1": metrics["f1"],
        })
        if metrics["f1"] > best_f1:
            best_f1 = metrics["f1"]
            best_threshold = t
            best_precision = metrics["precision"]
            best_recall = metrics["recall"]

    return {
        "best_threshold": best_threshold,
        "best_f1": best_f1,
        "best_precision": best_precision,
        "best_recall": best_recall,
        "curve": curve,
    }
