# Generate Tests

Generate comprehensive tests for the selected code following matcher's testing philosophy.

## Context
- matcher emphasizes "Test immediately after functionality"
- Focus on behavior that matters to users, not implementation details
- Test matching accuracy and user experience
- Follow existing test patterns in `tests/` directory

## Test Structure
- Use pytest with fixtures
- Follow patterns from existing test files
- Test the happy path first, then error scenarios
- Use descriptive test names: `test_<what>_<expected_behavior>`

## Test Patterns
- **Integration tests**: Test complete matching workflows end-to-end
- **Unit tests**: Test individual components in isolation (DataLoader, MatchingAlgorithm)
- **Fixtures**: Use test data from `tests/data/` or create temporary data
- **Data**: Use Polars DataFrames for test data (matcher's core stack)

## matcher-Specific Guidelines
- Test user experience: Does matching work as expected?
- Verify matching accuracy: Are matches correct?
- Test simple defaults: Do minimal configs "just work"?
- Test error scenarios: Clear failure handling
- Test both entity resolution and deduplication

## Output
Generate complete test files with:
1. Proper imports (pytest, polars, matcher modules)
2. Fixtures for test data and temporary directories
3. Happy path tests first
4. Error scenario tests
5. Clear docstrings explaining what each test verifies
6. Assertions that verify behavior, not implementation

## Example Test Structure
```python
"""
Tests for [component name].

Following matcher's testing principles:
- Test behavior that matters to users
- Focus on matching accuracy and reliability
- Verify [primary use case]
"""
import pytest
from pathlib import Path
import polars as pl

from matcher.[module] import [Component]

@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pl.DataFrame({
        "email": ["test@example.com", "other@example.com"],
        "name": ["Test User", "Other User"]
    })

def test_[component]_[happy_path]():
    """Test [what] works correctly."""
    # Arrange
    # Act
    # Assert
```

Generate tests that follow this structure and matcher's philosophy of simplicity, clarity, and incremental development.

