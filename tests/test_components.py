"""Tests for component-based architecture."""

import polars as pl
import pytest
from matcher import (
    Matcher,
    Deduplicator,
    MatchingAlgorithm,
    ExactMatcher,
    SimpleEvaluator,
)


class TestMatchingAlgorithm:
    """Tests for MatchingAlgorithm components."""

    def test_exact_matcher_entity_resolution(self):
        """Test ExactMatcher for entity resolution."""
        left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        matcher = ExactMatcher()
        results = matcher.match(left, right, ["email"], "id", "id")
        # Results are now DataFrame (in-memory only)

        assert results.height == 1
        assert "email" in results.columns

    def test_exact_matcher_deduplication(self):
        """Test ExactMatcher for deduplication."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "email": ["a@test.com", "a@test.com", "b@test.com"]
        })

        matcher = ExactMatcher()
        # For deduplication, pass cloned DataFrame
        results = matcher.match(df, df.clone(), ["email"], "id", "id")
        # Results are now DataFrame (in-memory only)

        # Should find duplicate pair (id 1 and 2 have same email)
        assert results.height > 0

    def test_exact_matcher_deduplication_no_id_column(self):
        """Test Deduplicator requires id column."""
        df = pl.DataFrame({
            "email": ["a@test.com", "a@test.com", "b@test.com"],
            "name": ["Alice", "Alice", "Bob"]
        })

        # Deduplicator requires id column
        with pytest.raises(ValueError, match="MUST have 'id' column"):
            deduplicator = Deduplicator(source=df, id_col="id")

        # Add id column and it should work
        df_with_id = df.with_row_index("id")
        deduplicator = Deduplicator(source=df_with_id, id_col="id")
        results = deduplicator.match(on=["email"])

        # Should find duplicate pair (a@test.com appears twice)
        # Deduplicator filters self-matches, so we get 2 match rows (both directions)
        assert results.count == 2
        # Verify it's the duplicate email
        assert results.matches["email"][0] == "a@test.com"
        assert results.matches["email"][1] == "a@test.com"

    def test_exact_matcher_multi_field_entity_resolution(self):
        """Test ExactMatcher with multiple fields for entity resolution."""
        left = pl.DataFrame({
            "id": [1, 2],
            "email": ["a@test.com", "b@test.com"],
            "zip_code": ["10001", "10002"]
        })
        right = pl.DataFrame({
            "id": [3, 4],
            "email": ["a@test.com", "b@test.com"],
            "zip_code": ["10001", "10003"]  # Only first matches on both fields
        })

        matcher = ExactMatcher()
        results = matcher.match(left, right, ["email", "zip_code"], "id", "id")
        # Results are now DataFrame (in-memory only)

        # Should find 1 match: (a@test.com, 10001)
        assert results.height == 1
        assert results["email"][0] == "a@test.com"
        assert results["zip_code"][0] == "10001"

    def test_exact_matcher_multi_field_deduplication(self):
        """Test ExactMatcher with multiple fields for deduplication."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "email": ["a@test.com", "a@test.com", "b@test.com"],
            "zip_code": ["10001", "10001", "10002"]
        })

        # Use Deduplicator instead of ExactMatcher directly (Deduplicator filters self-matches)
        deduplicator = Deduplicator(source=df, id_col="id")
        results = deduplicator.match(on=["email", "zip_code"])

        # Should find duplicate pair (id 1 and 2 match on both email and zip_code)
        # Deduplicator filters self-matches, so we get 2 match rows
        assert results.count == 2
        # Verify both matches have same email and zip_code
        assert results.matches["email"][0] == "a@test.com"
        assert results.matches["zip_code"][0] == "10001"


class TestComponentComposition:
    """Tests for composing components in Matcher."""

    def test_default_components_used(self):
        """Test that default components are used when not provided."""
        left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

        # Should use default matching algorithm
        assert isinstance(matcher.matching_algorithm, ExactMatcher)

    def test_matcher_with_dataframe(self):
        """Test Matcher accepts DataFrames (in-memory only)."""
        left_df = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right_df = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")

        # Data should be DataFrames (in-memory)
        assert isinstance(matcher.left, pl.DataFrame)
        assert isinstance(matcher.right, pl.DataFrame)
        # Verify data
        assert matcher.left.height == 2
        assert matcher.right.height == 2

    def test_custom_matching_algorithm(self):
        """Test Matcher with custom MatchingAlgorithm."""
        class TestMatcher(MatchingAlgorithm):
            def match(self, left, right, rule, left_id, right_id):
                # Always return empty matches
                return pl.DataFrame()

        left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        algorithm = TestMatcher()
        matcher = Matcher(
            left=left,
            right=right,
            left_id="id",
            right_id="id",
            matching_algorithm=algorithm
        )

        assert isinstance(matcher.matching_algorithm, TestMatcher)
        results = matcher.match(on="email")
        assert results.count == 0  # Custom algorithm returns empty

    def test_custom_matching_algorithm_with_data(self):
        """Test Matcher with custom MatchingAlgorithm."""
        class TestMatcher(MatchingAlgorithm):
            def match(self, left, right, rule, left_id, right_id):
                # Return a single match
                return pl.DataFrame({"id": [1], "email": ["test@test.com"]})

        left = pl.DataFrame({"id": [1], "email": ["test@test.com"]})
        right = pl.DataFrame({"id": [2], "email": ["test@test.com"]})

        matcher = Matcher(
            left=left,
            right=right,
            left_id="id",
            right_id="id",
            matching_algorithm=TestMatcher()
        )

        assert isinstance(matcher.matching_algorithm, TestMatcher)

        results = matcher.match(on="email")
        assert results.count == 1


class TestComponentEdgeCases:
    """Tests for edge cases in component system."""

    def test_empty_dataframe_raises_error(self):
        """Test that empty DataFrame raises error during initialization."""
        empty_df = pl.DataFrame()

        # Error should be raised during initialization
        with pytest.raises(ValueError, match="Left source is empty"):
            matcher = Matcher(left=empty_df, right=empty_df, left_id="id", right_id="id")

    def test_algorithm_with_missing_field(self):
        """Test that algorithm receives correct field even if validation fails."""
        left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        matcher = Matcher(left=left, right=right, left_id="id", right_id="id")

        # Should raise error before algorithm is called
        with pytest.raises(ValueError, match="Field\\(s\\) .* not found in left source"):
            matcher.match(on="missing")

    def test_deduplication_with_custom_algorithm(self):
        """Test deduplication with custom algorithm."""
        class DedupMatcher(MatchingAlgorithm):
            def match(self, left, right, rule, left_id, right_id):
                # Custom deduplication logic - simplified for testing
                # For this test, return matches for all rows (custom logic would filter)
                field = rule[0] if len(rule) > 0 else "id"
                # Join to create matches (will include self-matches which Deduplicator filters)
                # This will match: 1->1, 1->2, 2->1, 2->2, 3->3, 4->4
                # After filtering self-matches: 1->2, 2->1 (the duplicate email)
                return left.join(right, on=field, how="inner")

        df = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "email": ["a@test.com", "a@test.com", "b@test.com", "c@test.com"]
        })

        deduplicator = Deduplicator(
            source=df,
            id_col="id",
            matching_algorithm=DedupMatcher()
        )

        results = deduplicator.match(on="email")
        # Custom algorithm returns matches for all rows on email
        # After Deduplicator filters self-matches (id == id_right), we get:
        # - 1->2 and 2->1 (duplicate email matches)
        # So 2 matches total
        assert results.count == 2
        assert isinstance(deduplicator._matcher.matching_algorithm, DedupMatcher)


class TestSimpleEvaluator:
    """Tests for SimpleEvaluator component."""

    def test_perfect_matches(self):
        """Test evaluator with perfect matches (all predicted are correct)."""
        predicted = pl.DataFrame({
            "id": ["left_1", "left_2", "left_3"],
            "id_right": ["right_1", "right_2", "right_3"]
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1", "left_2", "left_3"],
            "right_id": ["right_1", "right_2", "right_3"]
        })

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

        assert metrics["precision"] == 1.0
        assert metrics["recall"] == 1.0
        assert metrics["f1"] == 1.0
        assert metrics["accuracy"] == 1.0
        assert metrics["true_positives"] == 3
        assert metrics["false_positives"] == 0
        assert metrics["false_negatives"] == 0

    def test_partial_matches(self):
        """Test evaluator with partial matches (some TP, FP, FN)."""
        predicted = pl.DataFrame({
            "id": ["left_1", "left_2", "left_3", "left_4"],
            "id_right": ["right_1", "right_2", "right_3", "right_5"]  # left_4->right_5 is FP
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1", "left_2", "left_3", "left_5"],
            "right_id": ["right_1", "right_2", "right_3", "right_6"]  # left_5->right_6 is FN
        })

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

        # TP = 3 (left_1->right_1, left_2->right_2, left_3->right_3)
        # FP = 1 (left_4->right_5)
        # FN = 1 (left_5->right_6)
        assert metrics["true_positives"] == 3
        assert metrics["false_positives"] == 1
        assert metrics["false_negatives"] == 1
        assert metrics["precision"] == 3 / 4  # TP / (TP + FP)
        assert metrics["recall"] == 3 / 4  # TP / (TP + FN)
        assert metrics["f1"] == 3 / 4  # 2 * (precision * recall) / (precision + recall)

    def test_no_matches(self):
        """Test evaluator when no matches are found."""
        predicted = pl.DataFrame({
            "id": ["left_1", "left_2"],
            "id_right": ["right_1", "right_2"]
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_3", "left_4"],
            "right_id": ["right_3", "right_4"]
        })

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

        assert metrics["true_positives"] == 0
        assert metrics["false_positives"] == 2
        assert metrics["false_negatives"] == 2
        assert metrics["precision"] == 0.0
        assert metrics["recall"] == 0.0
        assert metrics["f1"] == 0.0

    def test_empty_predictions(self):
        """Test evaluator with empty predictions."""
        predicted = pl.DataFrame({
            "id": [],
            "id_right": []
        }).with_columns([
            pl.col("id").cast(pl.Utf8),
            pl.col("id_right").cast(pl.Utf8)
        ])
        ground_truth = pl.DataFrame({
            "left_id": ["left_1", "left_2"],
            "right_id": ["right_1", "right_2"]
        })

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

        assert metrics["true_positives"] == 0
        assert metrics["false_positives"] == 0
        assert metrics["false_negatives"] == 2
        assert metrics["precision"] == 0.0  # No predictions, so precision is 0
        assert metrics["recall"] == 0.0

    def test_empty_ground_truth(self):
        """Test evaluator with empty ground truth."""
        predicted = pl.DataFrame({
            "id": ["left_1", "left_2"],
            "id_right": ["right_1", "right_2"]
        })
        ground_truth = pl.DataFrame({
            "left_id": [],
            "right_id": []
        }).with_columns([
            pl.col("left_id").cast(pl.Utf8),
            pl.col("right_id").cast(pl.Utf8)
        ])

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

        assert metrics["true_positives"] == 0
        assert metrics["false_positives"] == 2
        assert metrics["false_negatives"] == 0
        assert metrics["precision"] == 0.0
        assert metrics["recall"] == 0.0  # No ground truth, so recall is 0

    def test_deduplication_column_names(self):
        """Test evaluator with deduplication column names (id_match)."""
        predicted = pl.DataFrame({
            "id": ["left_1", "left_2"],
            "id_match": ["left_3", "left_4"]  # Deduplication uses id_match
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1", "left_2"],
            "right_id": ["left_3", "left_4"]
        })

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_match")

        assert metrics["true_positives"] == 2
        assert metrics["precision"] == 1.0
        assert metrics["recall"] == 1.0

    def test_alternative_column_name_id_right(self):
        """Test evaluator with alternative column name (id_right)."""
        predicted = pl.DataFrame({
            "id": ["left_1", "left_2"],
            "id_right": ["right_1", "right_2"]
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1", "left_2"],
            "right_id": ["right_1", "right_2"]
        })

        evaluator = SimpleEvaluator()
        # Should automatically find id_right even if right_id_col is different
        metrics = evaluator.evaluate(predicted, ground_truth, left_id_col="id", right_id_col="id_right")

        assert metrics["true_positives"] == 2
        assert metrics["precision"] == 1.0

    def test_custom_left_id_column(self):
        """Test evaluator with custom left_id column name."""
        predicted = pl.DataFrame({
            "custom_left": ["left_1", "left_2"],
            "id_right": ["right_1", "right_2"]
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1", "left_2"],
            "right_id": ["right_1", "right_2"]
        })

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, left_id_col="custom_left", right_id_col="id_right")

        assert metrics["true_positives"] == 2
        assert metrics["precision"] == 1.0

    def test_duplicate_pairs_handled(self):
        """Test that duplicate pairs in predicted or ground truth are handled correctly."""
        predicted = pl.DataFrame({
            "id": ["left_1", "left_1", "left_2"],  # Duplicate left_1->right_1
            "id_right": ["right_1", "right_1", "right_2"]
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1", "left_2"],
            "right_id": ["right_1", "right_2"]
        })

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

        # Duplicates should be removed, so TP = 2
        assert metrics["true_positives"] == 2
        assert metrics["precision"] == 1.0
        assert metrics["recall"] == 1.0

    def test_error_missing_left_id_in_ground_truth(self):
        """Test that evaluator raises error when ground truth missing left_id column."""
        predicted = pl.DataFrame({
            "id": ["left_1"],
            "id_right": ["right_1"]
        })
        ground_truth = pl.DataFrame({
            "wrong_left": ["left_1"],
            "right_id": ["right_1"]
        })

        evaluator = SimpleEvaluator()
        with pytest.raises(ValueError, match="left_id.*right_id"):
            evaluator.evaluate(predicted, ground_truth)

    def test_error_missing_right_id_in_ground_truth(self):
        """Test that evaluator raises error when ground truth missing right_id column."""
        predicted = pl.DataFrame({
            "id": ["left_1"],
            "id_right": ["right_1"]
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1"],
            "wrong_right": ["right_1"]
        })

        evaluator = SimpleEvaluator()
        with pytest.raises(ValueError, match="left_id.*right_id"):
            evaluator.evaluate(predicted, ground_truth)

    def test_error_missing_right_id_in_predicted(self):
        """Test that evaluator raises error when predicted missing right ID column."""
        predicted = pl.DataFrame({
            "id": ["left_1"],
            "wrong_col": ["right_1"]
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1"],
            "right_id": ["right_1"]
        })

        evaluator = SimpleEvaluator()
        with pytest.raises(ValueError, match="Could not find right ID column"):
            evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

    def test_auto_detect_id_match_column(self):
        """Test that evaluator auto-detects id_match column when right_id_col not found."""
        predicted = pl.DataFrame({
            "id": ["left_1", "left_2"],
            "id_match": ["left_3", "left_4"]  # id_match instead of id_right
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1", "left_2"],
            "right_id": ["left_3", "left_4"]
        })

        evaluator = SimpleEvaluator()
        # Should auto-detect id_match even if right_id_col is specified as something else
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

        assert metrics["true_positives"] == 2
        assert metrics["precision"] == 1.0

    def test_metrics_includes_all_fields(self):
        """Test that metrics dict includes all expected fields."""
        predicted = pl.DataFrame({
            "id": ["left_1"],
            "id_right": ["right_1"]
        })
        ground_truth = pl.DataFrame({
            "left_id": ["left_1"],
            "right_id": ["right_1"]
        })

        evaluator = SimpleEvaluator()
        metrics = evaluator.evaluate(predicted, ground_truth, right_id_col="id_right")

        expected_keys = {
            "precision", "recall", "f1", "accuracy",
            "true_positives", "false_positives", "false_negatives",
            "total_predicted", "total_ground_truth"
        }
        assert set(metrics.keys()) == expected_keys
        assert metrics["total_predicted"] == 1
        assert metrics["total_ground_truth"] == 1
