# hygge-match

![hygge logo](hygge.svg)

A cozy, comfortable matching library that makes entity resolution and deduplication feel natural.

## Philosophy

hygge (pronounced "hoo-ga") is a Danish word representing comfort, coziness, and well-being. This library brings those qualities to entity resolution and deduplication:

- **Comfort**: You should relax while you match some records.
- **Simplicity**: Clean, intuitive APIs that feel natural
- **Reliability**: Robust, predictable behavior without surprises
- **Flow**: Smooth, efficient matching without friction

## Overview

hygge-match is a lightweight library for matching records across data sources (entity resolution) and identifying duplicate records within a single source (deduplication). Built on **Polars** for optimal performance, it provides a cozy, notebook-friendly API that makes matching feel natural and comfortable.

### Built on Polars

hygge-match is built on **Polars** for optimal matching performance. This provides:

- Efficient columnar operations for large datasets
- Automatic parallelization of joins
- Clean, intuitive API that feels natural
- Zero-copy operations where possible

We chose Polars because it provides the best balance of performance, developer experience, and reliability for matching workflows.

**Key Design Principles:**

- **Comfort Over Complexity**: APIs should feel natural and intuitive
- **Flow Over Force**: Matching should work smoothly between data sources
- **Reliability Over Speed**: Prefer robust, predictable behavior
- **Clarity Over Cleverness**: Simple, clear code over complex optimizations
- **Progress over Perfection**: Ship working solutions that solve real problems

## Installation

```bash
uv add hygge-match
```

Or with pip:

```bash
pip install hygge-match
```

## Quick Start

```python
import polars as pl
from matcher import Matcher

# Load data (you load DataFrames, matcher operates on them)
left_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_a.parquet")
right_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_b.parquet")

# Entity resolution (using default components)
matcher = Matcher(left=left_df, right=right_df)
results = matcher.match(rules="email")
print(f"Found {results.count} matches")
results.matches.head(10)

# Deduplication (single source)
df = pl.read_parquet("data/ExactMatcher/deduplication/customers.parquet")
matcher = Matcher(left=df)
results = matcher.match(rules="email")
print(f"Found {results.count} duplicate pairs")

# Multiple matching rules (OR logic)
# Match if email OR (first_name AND last_name)
results = matcher.match(rules=[
    "email",
    ["first_name", "last_name"]
])
```

## Data-Driven Development Philosophy

**hygge-match is built for experimentation and comparison.** The ability to swap components, test different approaches, and measure results is foundational to hygge-match's design. This enables data-driven decisions about which matching strategies work best for your specific use case.

Like a cozy experiment, you can try different approaches, see what feels right, and choose what works best for your data.

### Why Component-Based Architecture?

The component-based architecture (similar to scikit-learn) enables you to:

- **Compare approaches**: Swap matching algorithms or evaluators to test alternatives
- **Measure impact**: Use built-in evaluation to quantify which approach performs better
- **Make informed decisions**: Choose components based on actual results, not assumptions
- **Iterate quickly**: Test new ideas without rewriting core logic

### Example: Comparing Matching Approaches

```python
from matcher import Matcher, ExactMatcher, SimpleEvaluator
import polars as pl

# Load data
left_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_a.parquet")
right_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_b.parquet")
ground_truth = pl.read_parquet("data/ground_truth.parquet")

# Test exact matching
matcher_exact = Matcher(
    left=left_df,
    right=right_df,
    matching_algorithm=ExactMatcher()
)
results_exact = matcher_exact.match(rules="email")
metrics_exact = results_exact.evaluate(ground_truth)

# Test case-insensitive matching (custom algorithm)
from examples.component_usage import CaseInsensitiveExactMatcher
matcher_case_insensitive = Matcher(
    left=left_df,
    right=right_df,
    matching_algorithm=CaseInsensitiveExactMatcher()
)
results_ci = matcher_case_insensitive.match(rules="email")
metrics_ci = results_ci.evaluate(ground_truth)

# Compare results
print(f"Exact matching: Precision={metrics_exact['precision']:.2%}, Recall={metrics_exact['recall']:.2%}")
print(f"Case-insensitive: Precision={metrics_ci['precision']:.2%}, Recall={metrics_ci['recall']:.2%}")
# Choose the approach that performs better for your data
```

## Component-Based Architecture

hygge-match uses a component-based architecture (similar to scikit-learn), allowing you to customize matching algorithms:

```python
from matcher import Matcher, MatchingAlgorithm
import polars as pl

# Use custom matching algorithm
class MyCustomMatcher(MatchingAlgorithm):
    def match(self, left, right, rule):
        # Your custom matching logic
        # rule is a list of fields (e.g., ["email"] or ["first_name", "last_name"])
        # Return a DataFrame with matches
        pass

# Load data
left_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_a.parquet")
right_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_b.parquet")

# Use custom algorithm
matcher = Matcher(
    left=left_df,
    right=right_df,
    matching_algorithm=MyCustomMatcher()
)
results = matcher.match(rules="email")
```

See `examples/component_usage.py` for more examples.

## Evaluation & Measurement

hygge-match includes built-in evaluation capabilities to measure matching performance:

```python
from matcher import Matcher
import polars as pl

# Load data
left_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_a.parquet")
right_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_b.parquet")

# Run matching
matcher = Matcher(left=left_df, right=right_df)
results = matcher.match(rules="email")

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

The project includes sample datasets organized by component:

- **ExactMatcher** (`data/ExactMatcher/`):
  - **Entity Resolution** (`entity_resolution/`):
    - `customers_a.parquet` and `customers_b.parquet` - 500 records each
    - 40 known matches with exotic matching scenarios (documented in `ground_truth.md`)
    - Tests various matching rules: email-only, name-only, address+zip, mixed
  - **Deduplication** (`deduplication/`):
    - `customers.parquet` - 1000 records
    - 50 known duplicate pairs (documented in `ground_truth.md`)
  - **Evaluation** (`evaluation/`):
    - `customers_a.parquet` and `customers_b.parquet` - 50 records each
    - 30 simple email matches for stable evaluation testing

- **SimpleEvaluator** (`data/SimpleEvaluator/`):
  - **Evaluation** (`evaluation/`):
    - Test datasets for evaluator component testing

Regenerate test data with: `uv run python scripts/generate_test_data.py`

## Documentation

See `MATCHING_PLAN_V2.md` for the implementation plan and `CLAUDE.md` for development guidelines.

## Design Principles

hygge-match follows hygge philosophy in its design:

1. **Comfort Over Complexity**
   - APIs should feel natural and intuitive
   - Configuration should be simple but flexible
   - Defaults should "just work"

2. **Flow Over Force**
   - Matching should work smoothly between data sources
   - Results should be immediately explorable
   - Progress should be visible but unobtrusive

3. **Reliability Over Speed**
   - Prefer robust, predictable behavior
   - Handle errors gracefully
   - Make recovery simple

4. **Clarity Over Cleverness**
   - Simple, clear code over complex optimizations
   - Explicit configuration over implicit behavior
   - Clear error messages and helpful guidance

hygge-match isn't just about matching records - it's about making entity resolution and deduplication feel natural, comfortable, and reliable. Like a warm blanket for your data matching needs.
