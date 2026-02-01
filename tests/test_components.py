"""Tests for component-based architecture."""

import polars as pl
import pytest
from matcher import (
    Matcher,
    DataLoader,
    ParquetLoader,
    MatchingAlgorithm,
    ExactMatcher,
)


class TestDataLoader:
    """Tests for DataLoader components."""

    def test_parquet_loader_with_dataframe(self):
        """Test ParquetLoader returns DataFrame as-is."""
        df = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        loader = ParquetLoader()
        result = loader.load(df)

        assert result.equals(df)
        assert result.height == 2

    def test_parquet_loader_with_string(self, tmp_path):
        """Test ParquetLoader loads from parquet file."""
        df = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        file_path = tmp_path / "test.parquet"
        df.write_parquet(file_path)

        loader = ParquetLoader()
        result = loader.load(str(file_path))

        assert result.height == 2
        assert "email" in result.columns


class TestMatchingAlgorithm:
    """Tests for MatchingAlgorithm components."""

    def test_exact_matcher_entity_resolution(self):
        """Test ExactMatcher for entity resolution."""
        left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        matcher = ExactMatcher()
        results = matcher.match(left, right, "email")

        assert results.height == 1
        assert "email" in results.columns

    def test_exact_matcher_deduplication(self):
        """Test ExactMatcher for deduplication."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "email": ["a@test.com", "a@test.com", "b@test.com"]
        })

        matcher = ExactMatcher()
        results = matcher.match(df, None, "email")

        # Should find duplicate pair (id 1 and 2 have same email)
        assert results.height > 0

    def test_exact_matcher_deduplication_no_id_column(self):
        """Test ExactMatcher deduplication when no id column exists."""
        df = pl.DataFrame({
            "email": ["a@test.com", "a@test.com", "b@test.com"],
            "name": ["Alice", "Alice", "Bob"]
        })

        matcher = ExactMatcher()
        results = matcher.match(df, None, "email")

        # Should find duplicate pair (a@test.com appears twice)
        # Self-join creates both directions: (row 0 -> row 1) and (row 1 -> row 0)
        # Self-matches are filtered using row indices, so we get 2 match rows (both directions)
        assert results.height == 2
        # Verify it's the duplicate email
        assert results["email"][0] == "a@test.com"
        assert results["email"][1] == "a@test.com"


class TestComponentComposition:
    """Tests for composing components in Matcher."""

    def test_default_components_used(self):
        """Test that default components are used when not provided."""
        left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        matcher = Matcher(left_source=left, right_source=right)

        # Should use default components
        assert isinstance(matcher.data_loader, ParquetLoader)
        assert isinstance(matcher.matching_algorithm, ExactMatcher)

    def test_custom_data_loader(self):
        """Test Matcher with custom DataLoader."""
        class TestLoader(DataLoader):
            def load(self, source):
                # Always return a test DataFrame
                return pl.DataFrame({"id": [1, 2], "email": ["test@test.com", "test2@test.com"]})

        loader = TestLoader()
        matcher = Matcher(
            left_source="ignored",
            right_source="ignored",
            data_loader=loader
        )

        assert isinstance(matcher.data_loader, TestLoader)
        assert matcher.left.height == 2
        assert matcher.right.height == 2

    def test_custom_matching_algorithm(self):
        """Test Matcher with custom MatchingAlgorithm."""
        class TestMatcher(MatchingAlgorithm):
            def match(self, left, right, field):
                # Always return empty matches
                return pl.DataFrame()

        left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        algorithm = TestMatcher()
        matcher = Matcher(
            left_source=left,
            right_source=right,
            matching_algorithm=algorithm
        )

        assert isinstance(matcher.matching_algorithm, TestMatcher)
        results = matcher.match_exact(field="email")
        assert results.count == 0  # Custom algorithm returns empty

    def test_custom_components_together(self):
        """Test Matcher with both custom components."""
        class TestLoader(DataLoader):
            def load(self, source):
                return pl.DataFrame({"id": [1], "email": ["test@test.com"]})

        class TestMatcher(MatchingAlgorithm):
            def match(self, left, right, field):
                # Return a single match
                return pl.DataFrame({"id": [1], "email": ["test@test.com"]})

        matcher = Matcher(
            left_source="ignored",
            right_source="ignored",
            data_loader=TestLoader(),
            matching_algorithm=TestMatcher()
        )

        assert isinstance(matcher.data_loader, TestLoader)
        assert isinstance(matcher.matching_algorithm, TestMatcher)

        results = matcher.match_exact(field="email")
        assert results.count == 1


class TestComponentEdgeCases:
    """Tests for edge cases in component system."""

    def test_empty_dataframe_from_loader(self):
        """Test that empty DataFrame from loader raises error."""
        class EmptyLoader(DataLoader):
            def load(self, source):
                return pl.DataFrame()

        with pytest.raises(ValueError, match="Left source is empty"):
            Matcher(
                left_source="ignored",
                data_loader=EmptyLoader()
            )

    def test_algorithm_with_missing_field(self):
        """Test that algorithm receives correct field even if validation fails."""
        left = pl.DataFrame({"id": [1, 2], "email": ["a@test.com", "b@test.com"]})
        right = pl.DataFrame({"id": [3, 4], "email": ["a@test.com", "c@test.com"]})

        matcher = Matcher(left_source=left, right_source=right)

        # Should raise error before algorithm is called
        with pytest.raises(ValueError, match="Field 'missing' not found"):
            matcher.match_exact(field="missing")

    def test_deduplication_with_custom_algorithm(self):
        """Test deduplication with custom algorithm."""
        class DedupMatcher(MatchingAlgorithm):
            def match(self, left, right, field):
                # Custom deduplication logic - simplified for testing
                if right is None:
                    # Simple approach: return all rows (custom logic would filter)
                    return left
                return left.join(right, on=field, how="inner")

        df = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "email": ["a@test.com", "a@test.com", "b@test.com", "c@test.com"]
        })

        matcher = Matcher(
            left_source=df,
            matching_algorithm=DedupMatcher()
        )

        results = matcher.match_exact(field="email")
        # Custom algorithm returns all rows
        assert results.count == 4
        assert isinstance(matcher.matching_algorithm, DedupMatcher)
