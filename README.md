# matcher

A cozy, comfortable matching library that makes entity resolution and deduplication feel natural.

## Overview

matcher is a lightweight library for matching records across data sources (entity resolution) and identifying duplicate records within a single source (deduplication). Built on **Polars** for optimal performance, it provides a cozy, notebook-friendly API that makes matching feel natural and comfortable.

### Built on Polars

matcher is built on **Polars** for optimal matching performance. This provides:

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
uv add matcher
```

Or with pip:

```bash
pip install matcher
```

## Quick Start

The examples below use paths from the project's generated test data. Create them with `uv run python scripts/generate_test_data.py`, or use your own Parquet/CSV paths.

```python
import polars as pl
from matcher import Matcher, Deduplicator

# Load data (you load DataFrames, matcher operates on them)
left_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_a.parquet")
right_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_b.parquet")

# Entity resolution (using default components)
matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")
results = matcher.match(rules="email")
print(f"Found {results.count} matches")
results.matches.head(10)

# Deduplication (single source)
df = pl.read_parquet("data/ExactMatcher/deduplication/customers.parquet")
deduplicator = Deduplicator(source=df, id_col="id")
results = deduplicator.match(rules="email")
print(f"Found {results.count} duplicate pairs")

# Multiple matching rules (OR logic)
# Match if email OR (first_name AND last_name)
results = matcher.match(rules=[
    "email",
    ["first_name", "last_name"]
])

# Fuzzy matching (typo-tolerant, single field) and optional blocking
# results = matcher.match_fuzzy(field="name", threshold=0.85)
# results = matcher.match(rules="email", blocking_key="zip_code")
```

### Null handling

Exact matching uses Polars inner joins. Rows where any join key (e.g. `email`, `first_name`) is null are excluded from matches—including null-to-null. Fill or drop nulls in your match columns beforehand if you need different behavior.

## Data-Driven Development Philosophy

**matcher is built for experimentation and comparison.** The ability to swap components, test different approaches, and measure results is foundational to matcher's design. This enables data-driven decisions about which matching strategies work best for your specific use case.

Like a cozy experiment, you can try different approaches, see what feels right, and choose what works best for your data.

### Why Component-Based Architecture?

The component-based architecture (similar to scikit-learn) enables you to:

- **Compare approaches**: Swap matching algorithms or evaluators to test alternatives
- **Measure impact**: Use built-in evaluation to quantify which approach performs better
- **Make informed decisions**: Choose components based on actual results, not assumptions
- **Iterate quickly**: Test new ideas without rewriting core logic

### Example: Comparing Matching Approaches

```python
from matcher import Matcher
import polars as pl

# Load data (generate first: uv run python scripts/generate_test_data.py)
left_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_a.parquet")
right_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_b.parquet")
# Ground truth: known pairs as DataFrame with left_id, right_id
ground_truth = pl.DataFrame({
    "left_id": ["left_1", "left_2", "left_3"],
    "right_id": ["right_1", "right_2", "right_3"]
})  # or pl.read_parquet("your_ground_truth.parquet")

matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")

# Test email-only matching
results_email = matcher.match(rules="email")
metrics_email = results_email.evaluate(ground_truth)

# Test name-only matching
results_name = matcher.match(rules=["first_name", "last_name"])
metrics_name = results_name.evaluate(ground_truth)

# Compare results
print(f"Email rule: Precision={metrics_email['precision']:.2%}, Recall={metrics_email['recall']:.2%}")
print(f"Name rule: Precision={metrics_name['precision']:.2%}, Recall={metrics_name['recall']:.2%}")
# You can also swap matching_algorithm (e.g. custom case-insensitive matcher) and compare
```

## Component-Based Architecture

matcher uses a component-based architecture (similar to scikit-learn), allowing you to customize matching algorithms:

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

### Cascading matching (refine)

Apply a second rule only to left-side records that did not match the first. Useful for “match on email first, then on name for the rest”:

```python
results = matcher.match(rules="email")
refined = results.refine(matcher, rule=["first_name", "last_name"])  # adds name matches for unmatched
```

See the test suite (`tests/`) for more examples of custom algorithms and usage.

## Evaluation & Measurement

matcher includes built-in evaluation so you can **measure matching performance and improve over time**. Ground truth should be provided as a Polars `DataFrame` with `left_id` and `right_id` columns listing known true pairs. If your labels are stored on disk (e.g. Parquet or CSV), load them first with `pl.read_parquet` or `pl.read_csv` before calling `evaluate()`.

```python
from matcher import Matcher
import polars as pl

# Load data
left_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_a.parquet")
right_df = pl.read_parquet("data/ExactMatcher/entity_resolution/customers_b.parquet")
matcher = Matcher(left=left_df, right=right_df, left_id="id", right_id="id")

# Run matching
results = matcher.match(rules="email")

# Evaluate against ground truth (DataFrame with left_id, right_id columns)
ground_truth = pl.DataFrame({
    "left_id": ["left_1", "left_2"],
    "right_id": ["right_1", "right_2"]
})
metrics = results.evaluate(ground_truth)

print(f"Precision: {metrics['precision']:.2%}")
print(f"Recall: {metrics['recall']:.2%}")
print(f"F1 Score: {metrics['f1']:.2%}")
```

### Improvement loop (use evaluation to get better)

Use evaluate so **you** can improve: get ground truth, run match, evaluate, change something, re-run, compare metrics until the result is good enough.

1. **Get ground truth** — Known pairs (e.g. from a human-reviewed sample or existing labels) as a DataFrame with `left_id` and `right_id`. Load from CSV or Parquet if needed: `ground_truth = pl.read_csv("reviewed.csv")`.
2. **Run your matcher** — e.g. `results = matcher.match(rules="email")` or `matcher.match_fuzzy(field="name", threshold=0.85)`.
3. **Evaluate** — `metrics = results.evaluate(ground_truth)`. For deduplication, or when the left and right id columns share the same name (e.g. both `id`), pass `right_id_col="id_right"` so the evaluator can correctly resolve right-side ids.
4. **Change something** — Adjust rules, threshold, or blocking_key.
5. **Re-run and compare** — Run again, call `evaluate(ground_truth)`, compare precision/recall/F1 to the previous run.
6. **Repeat** until quality is good enough.

Example: compare two thresholds by running each and comparing metrics:

```python
# Try threshold 0.85
results_85 = matcher.match_fuzzy(field="name", threshold=0.85)
m85 = results_85.evaluate(ground_truth)

# Try threshold 0.82 (more recall, maybe more false positives)
results_82 = matcher.match_fuzzy(field="name", threshold=0.82)
m82 = results_82.evaluate(ground_truth)

# Choose based on evidence
print(f"0.85: precision={m85['precision']:.2%}, recall={m85['recall']:.2%}")
print(f"0.82: precision={m82['precision']:.2%}, recall={m82['recall']:.2%}")
```

Use evaluation to:

- **Compare approaches**: Test different algorithms or thresholds and choose the best performer
- **Validate improvements**: Measure impact before committing to a new approach
- **Track quality**: Iterate until precision/recall are good enough for your use case

### Tuning fuzzy threshold

For fuzzy matching, use `find_best_threshold()` to pick a confidence threshold from match results and ground truth (it sweeps thresholds and returns the one that maximizes F1). Requires a `confidence` column, so use `match_fuzzy()` results:

```python
from matcher import Matcher, find_best_threshold

results = matcher.match_fuzzy(field="name", threshold=0.85)
best = find_best_threshold(results.matches, ground_truth, right_id_col="id_right")
print(f"Best threshold: {best['best_threshold']}, F1: {best['best_f1']:.2%}")
```

### Export for review

Export match results to **CSV** for human review (opens in Excel, Power BI, or any tool). The file includes identifiers and joined columns so reviewers have enough context without opening other systems. Use `sample(n=...)` to export a manageable sample for reviewers.

```python
results = matcher.match_fuzzy(field="name", threshold=0.85)
results.export_for_review("matches_for_review.csv")

# Export a sample for reviewers
results.sample(n=50, seed=42).export_for_review("sample_for_review.csv")

# Focused export: only selected columns
results.pipe(lambda df: df.select(["id", "id_right", "confidence", "name", "name_right"])).export_for_review("review.csv")
```

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

See `docs/archive/MATCHING_PLAN_V2.md` for the implementation plan and `CLAUDE.md` for development guidelines.

## Design Principles

matcher follows hygge philosophy in its design:

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

matcher isn't just about matching records - it's about making entity resolution and deduplication feel natural, comfortable, and reliable. Like a warm blanket for your data matching needs.
