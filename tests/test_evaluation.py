"""Tests for evaluation functionality."""

import polars as pl
import pytest
from pathlib import Path
from matcher import Matcher, Deduplicator, SimpleEvaluator


@pytest.fixture
def sample_data_dir():
    """Return path to sample data directory."""
    return Path(__file__).parent.parent / "data"


def test_evaluation_with_entity_resolution(sample_data_dir):
    """Test evaluation metrics for entity resolution using simple evaluation dataset."""
    # Use dedicated evaluation dataset (simple, stable ground truth)
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"

    # Load data and create matcher
    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(rules="email")

    # Create ground truth DataFrame - simple: first 30 records match on email
    ground_truth_data = []
    for i in range(30):
        ground_truth_data.append({
            "left_id": f"eval_left_{i+1}",
            "right_id": f"eval_right_{i+1}"
        })
    ground_truth = pl.DataFrame(ground_truth_data)

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
    results = deduplicator.match(rules="email")

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


def test_evaluation_with_custom_evaluator(sample_data_dir):
    """Test evaluation with custom evaluator component using simple evaluation dataset."""
    # Use dedicated evaluation dataset (simple, stable ground truth)
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(rules="email")

    # Create ground truth - simple: first 30 records match on email
    ground_truth_data = []
    for i in range(30):
        ground_truth_data.append({
            "left_id": f"eval_left_{i+1}",
            "right_id": f"eval_right_{i+1}"
        })
    ground_truth = pl.DataFrame(ground_truth_data)

    # Use custom evaluator
    custom_evaluator = SimpleEvaluator()
    metrics = results.evaluate(ground_truth, evaluator=custom_evaluator)

    assert "precision" in metrics
    assert "recall" in metrics
    assert "f1" in metrics
    assert "accuracy" in metrics


def test_evaluation_with_parquet_ground_truth(sample_data_dir, tmp_path):
    """Test evaluation with ground truth loaded from parquet file using simple evaluation dataset."""
    # Use dedicated evaluation dataset (simple, stable ground truth)
    left_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_a.parquet"
    right_path = sample_data_dir / "ExactMatcher" / "evaluation" / "customers_b.parquet"

    left_df = pl.read_parquet(left_path)
    right_df = pl.read_parquet(right_path)
    matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
    results = matcher.match(rules="email")

    # Create and save ground truth to parquet - simple: first 30 records match on email
    ground_truth_data = []
    for i in range(30):
        ground_truth_data.append({
            "left_id": f"eval_left_{i+1}",
            "right_id": f"eval_right_{i+1}"
        })
    ground_truth = pl.DataFrame(ground_truth_data)
    ground_truth_path = tmp_path / "ground_truth.parquet"
    ground_truth.write_parquet(ground_truth_path)

    # Evaluate using file path (entity resolution join creates id_right column)
    metrics = results.evaluate(str(ground_truth_path), left_id_col="id", right_id_col="id_right")

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
    results = matcher.match(rules="email")

    # Ground truth with wrong column names
    bad_ground_truth = pl.DataFrame({
        "wrong_left": ["eval_left_1"],
        "wrong_right": ["eval_right_1"]
    })

    with pytest.raises(ValueError, match="left_id.*right_id"):
        results.evaluate(bad_ground_truth)
