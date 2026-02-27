"""Tests for evaluation functionality."""

import polars as pl
import pytest
from pathlib import Path
from matcher import Matcher, Deduplicator, SimpleEvaluator, find_best_threshold


@pytest.fixture
def sample_data_dir():
    """Return path to sample data directory."""
    return Path(__file__).parent.parent / "data"


@pytest.fixture
def ground_truth_30_pairs():
    """Ground truth: first 30 eval_left_/eval_right_ pairs (matches evaluation dataset)."""
    return pl.DataFrame([
        {"left_id": f"eval_left_{i+1}", "right_id": f"eval_right_{i+1}"}
        for i in range(30)
    ])


def test_evaluation_with_entity_resolution(sample_data_dir, ground_truth_30_pairs):
    """Test evaluation metrics for entity resolution using simple evaluation dataset."""
    # Use dedicated evaluation dataset (simple, stable ground truth)
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"

    # Load data and create matcher
    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(match_on="email")

    ground_truth = ground_truth_30_pairs

    # Evaluate (entity resolution join creates id_right column)
    metrics = results.evaluate(ground_truth, left_id_col="id", right_id_col="id_right")

    # Should have perfect precision and recall for exact matching
    assert metrics["precision"] == 1.0, f"Expected precision=1.0, got {metrics['precision']}"
    assert metrics["recall"] == 1.0, f"Expected recall=1.0, got {metrics['recall']}"
    assert metrics["f1"] == 1.0, f"Expected f1=1.0, got {metrics['f1']}"
    assert metrics["true_positives"] == 30, f"Expected 30 TP, got {metrics['true_positives']}"
    assert metrics["false_positives"] == 0, f"Expected 0 FP, got {metrics['false_positives']}"
    assert metrics["false_negatives"] == 0, f"Expected 0 FN, got {metrics['false_negatives']}"


def test_evaluation_with_deduplication(sample_data_dir):
    """Test evaluation metrics for deduplication."""
    source_path = sample_data_dir / "ExactMatcher" / "deduplication" / "customers.parquet"

    # Load data and create deduplicator
    df = pl.read_parquet(source_path)
    deduplicator = Deduplicator(source=df, id_col="id")
    results = deduplicator.match(match_on="email")

    # For deduplication, we need to create ground truth from known duplicate groups
    # Load the data to understand the structure
    df = pl.read_parquet(source_path)

    # Create ground truth: pairs of IDs that should match (same email, different IDs)
    # This is a simplified version - in practice, you'd load from ground truth file
    ground_truth_data = []

    # Group by email and create pairs
    email_groups = df.group_by("email").agg(pl.col("id").alias("ids"))
    for row in email_groups.iter_rows(named=True):
        ids = row["ids"]
        if len(ids) > 1:
            # Create pairs from duplicate group
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    ground_truth_data.append({
                        "left_id": ids[i],
                        "right_id": ids[j]
                    })

    if ground_truth_data:
        ground_truth = pl.DataFrame(ground_truth_data)

        # Evaluate (for deduplication, right_id_col is "id_right")
        metrics = results.evaluate(
            ground_truth,
            left_id_col="id",
            right_id_col="id_right"
        )

        # Should have some matches
        assert metrics["true_positives"] > 0, "Should have some true positives"
        assert metrics["precision"] >= 0.0, "Precision should be non-negative"
        assert metrics["recall"] >= 0.0, "Recall should be non-negative"


def test_evaluation_with_custom_evaluator(sample_data_dir, ground_truth_30_pairs):
    """Test evaluation with custom evaluator component using simple evaluation dataset."""
    # Use dedicated evaluation dataset (simple, stable ground truth)
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(match_on="email")

    ground_truth = ground_truth_30_pairs

    # Use custom evaluator
    custom_evaluator = SimpleEvaluator()
    metrics = results.evaluate(ground_truth, evaluator=custom_evaluator)

    assert "precision" in metrics
    assert "recall" in metrics
    assert "f1" in metrics
    assert "accuracy" in metrics


def test_evaluation_with_parquet_ground_truth(sample_data_dir, ground_truth_30_pairs, tmp_path):
    """Test evaluation with ground truth loaded from parquet (user loads, then evaluate on DataFrame)."""
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(match_on="email")

    # Save to parquet, load back (simulates user loading from file)
    ground_truth_path = tmp_path / "ground_truth.parquet"
    ground_truth_30_pairs.write_parquet(ground_truth_path)
    ground_truth = pl.read_parquet(ground_truth_path)

    metrics = results.evaluate(ground_truth, left_id_col="id", right_id_col="id_right")

    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 1.0


def test_evaluation_with_csv_ground_truth(sample_data_dir, ground_truth_30_pairs, tmp_path):
    """Test evaluate() with ground truth loaded from CSV (user loads, then evaluate on DataFrame)."""
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"
    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(match_on="email")

    csv_path = tmp_path / "ground_truth.csv"
    ground_truth_30_pairs.write_csv(csv_path)
    ground_truth = pl.read_csv(csv_path)

    metrics = results.evaluate(ground_truth, left_id_col="id", right_id_col="id_right")

    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 1.0


def test_evaluation_error_handling(sample_data_dir):
    """Test evaluation error handling."""
    # Use any dataset - this test just checks error handling
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(match_on="email")

    # Ground truth with wrong column names
    bad_ground_truth = pl.DataFrame({
        "wrong_left": ["eval_left_1"],
        "wrong_right": ["eval_right_1"]
    })

    with pytest.raises(ValueError, match="left_id.*right_id"):
        results.evaluate(bad_ground_truth)


def test_find_best_threshold(sample_data_dir, ground_truth_30_pairs):
    """Test find_best_threshold returns best threshold by F1 and a curve for fuzzy results."""
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"
    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")

    # Fuzzy with low threshold so we have scored pairs to sweep
    from matcher import FuzzyMatcher
    results = matcher.match(match_on=["first_name"], matching_algorithm=FuzzyMatcher(threshold=0.5))
    ground_truth = ground_truth_30_pairs

    best = find_best_threshold(
        results.matches,
        ground_truth,
        left_id_col="id",
        right_id_col="id_right",
    )

    assert "best_threshold" in best
    assert "best_f1" in best
    assert "best_precision" in best
    assert "best_recall" in best
    assert "curve" in best
    assert best["best_threshold"] is not None
    assert 0 <= best["best_f1"] <= 1.0
    assert len(best["curve"]) == 11  # default 0.50 .. 1.00 step 0.05
    for point in best["curve"]:
        assert "threshold" in point and "precision" in point and "recall" in point and "f1" in point
    max_f1_in_curve = max(p["f1"] for p in best["curve"])
    assert best["best_f1"] == max_f1_in_curve


def test_find_best_threshold_requires_confidence():
    """Test find_best_threshold raises if matches have no confidence column."""
    matches = pl.DataFrame({"id": [1], "id_right": [2]})  # no confidence
    ground_truth = pl.DataFrame({"left_id": [1], "right_id": [2]})
    with pytest.raises(ValueError, match="confidence"):
        find_best_threshold(matches, ground_truth)


def test_find_best_threshold_empty_thresholds_raises():
    """Test find_best_threshold raises if thresholds is empty."""
    matches = pl.DataFrame({"id": [1], "id_right": [2], "confidence": [0.9]})
    ground_truth = pl.DataFrame({"left_id": [1], "right_id": [2]})
    with pytest.raises(ValueError, match="non-empty"):
        find_best_threshold(matches, ground_truth, thresholds=[])


def test_find_best_threshold_invalid_threshold_value_raises():
    """Test find_best_threshold raises if a threshold is out of [0, 1]."""
    matches = pl.DataFrame({"id": [1], "id_right": [2], "confidence": [0.9]})
    ground_truth = pl.DataFrame({"left_id": [1], "right_id": [2]})
    with pytest.raises(ValueError, match="out of range"):
        find_best_threshold(matches, ground_truth, thresholds=[0.5, 1.5])
