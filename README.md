# Matcher

A simple Python library for entity resolution and deduplication.

## Overview

Matcher is a lightweight library for matching records across data sources (entity resolution) and identifying duplicate records within a single source (deduplication).

**Key Design Principles:**
- **KISS (Keep It Simple, Stupid)**: Start with the absolute minimum, add complexity only when proven necessary
- **YAGNI (You Aren't Gonna Need It)**: Don't build features until you have a real, current use case
- **Library-First**: Python API optimized for notebooks, not CLI-first
- **Data-Driven Decisions**: Compare approaches, measure results, and make decisions based on evidence, not assumptions

## Installation

```bash
uv add matcher
```

Or with pip:
```bash
pip install matcher
```

## Quick Start

```python
from matcher import Matcher

# Entity resolution (using default components)
matcher = Matcher(
    left_source="data/customers_a.parquet",
    right_source="data/customers_b.parquet"
)

results = matcher.match_exact(field="email")
print(f"Found {results.count} matches")
results.matches.head(10)

# Deduplication
matcher = Matcher(left_source="data/customers.parquet")
results = matcher.match_exact(field="email")
print(f"Found {results.count} duplicate pairs")
```

## Data-Driven Development Philosophy

**Matcher is built for experimentation and comparison.** The ability to swap components, test different approaches, and measure results is foundational to matcher's design. This enables data-driven decisions about which matching strategies work best for your specific use case.

### Why Component-Based Architecture?

The component-based architecture (similar to scikit-learn) enables you to:
- **Compare approaches**: Swap matching algorithms, data loaders, or evaluators to test alternatives
- **Measure impact**: Use built-in evaluation to quantify which approach performs better
- **Make informed decisions**: Choose components based on actual results, not assumptions
- **Iterate quickly**: Test new ideas without rewriting core logic

### Example: Comparing Matching Approaches

```python
from matcher import Matcher, ExactMatcher, SimpleEvaluator
import polars as pl

# Load ground truth for evaluation
ground_truth = pl.read_parquet("data/ground_truth.parquet")

# Test exact matching
matcher_exact = Matcher(
    left_source="data/customers_a.parquet",
    right_source="data/customers_b.parquet",
    matching_algorithm=ExactMatcher()
)
results_exact = matcher_exact.match_exact(field="email")
metrics_exact = results_exact.evaluate(ground_truth)

# Test case-insensitive matching (custom algorithm)
from examples.component_usage import CaseInsensitiveExactMatcher
matcher_case_insensitive = Matcher(
    left_source="data/customers_a.parquet",
    right_source="data/customers_b.parquet",
    matching_algorithm=CaseInsensitiveExactMatcher()
)
results_ci = matcher_case_insensitive.match_exact(field="email")
metrics_ci = results_ci.evaluate(ground_truth)

# Compare results
print(f"Exact matching: Precision={metrics_exact['precision']:.2%}, Recall={metrics_exact['recall']:.2%}")
print(f"Case-insensitive: Precision={metrics_ci['precision']:.2%}, Recall={metrics_ci['recall']:.2%}")
# Choose the approach that performs better for your data
```

## Component-Based Architecture

Matcher uses a component-based architecture (similar to scikit-learn), allowing you to customize data loading and matching algorithms:

```python
from matcher import Matcher, DataLoader, MatchingAlgorithm

# Use custom components
class MyCustomLoader(DataLoader):
    def load(self, source):
        # Your custom loading logic
        pass

class MyCustomMatcher(MatchingAlgorithm):
    def match(self, left, right, field):
        # Your custom matching logic
        pass

matcher = Matcher(
    left_source="data/customers_a.parquet",
    right_source="data/customers_b.parquet",
    data_loader=MyCustomLoader(),
    matching_algorithm=MyCustomMatcher()
)
```

See `examples/component_usage.py` for more examples.

## Evaluation & Measurement

Matcher includes built-in evaluation capabilities to measure matching performance:

```python
from matcher import Matcher
import polars as pl

# Run matching
matcher = Matcher(
    left_source="data/customers_a.parquet",
    right_source="data/customers_b.parquet"
)
results = matcher.match_exact(field="email")

# Evaluate against ground truth
ground_truth = pl.DataFrame({
    "left_id": ["left_1", "left_2"],
    "right_id": ["right_1", "right_2"]
})
metrics = results.evaluate(ground_truth)

print(f"Precision: {metrics['precision']:.2%}")
print(f"Recall: {metrics['recall']:.2%}")
print(f"F1 Score: {metrics['f1']:.2%}")
```

Use evaluation to:
- **Compare approaches**: Test different algorithms and choose the best performer
- **Validate improvements**: Measure impact before committing to a new approach
- **Track quality**: Monitor matching quality as you iterate

## Development

```bash
# Install dependencies
uv sync

# Generate test datasets
uv run python scripts/generate_test_data.py

# Run tests
uv run pytest
```

### Test Data

The project includes sample datasets for validation:

- **Entity Resolution**: `data/customers_a.parquet` and `data/customers_b.parquet`
  - 500 records each
  - 30 known matches (documented in `data/ground_truth_entity_resolution.md`)

- **Deduplication**: `data/customers.parquet`
  - 1000 records
  - 50 known duplicate pairs (documented in `data/ground_truth_deduplication.md`)

Regenerate test data with: `uv run python scripts/generate_test_data.py`

## Documentation

See `MATCHING_PLAN_V2.md` for the implementation plan and `CLAUDE.md` for development guidelines.
