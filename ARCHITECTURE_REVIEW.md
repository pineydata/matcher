# Matching Implementation Plan - Architecture Review

**Key Insight:** This plan assumes CLI + YAML config (from hygge's extract/load pattern), but matching is fundamentally different and may need a different approach.

---

## The Core Mismatch

### Extract/Load (Hygge's Domain)
- **Pattern:** Configure once, run repeatedly
- **Workflow:** YAML config → CLI command → Done
- **User needs:** Repeatability, automation, simple config
- **Interface:** CLI + YAML is perfect ✅

### Matching (This Project)
- **Pattern:** Explore, tune, iterate, validate
- **Workflow:** Try config → See results → Adjust thresholds → Try again → Validate → Deploy
- **User needs:** Interactive exploration, quick iteration, visual feedback
- **Interface:** CLI + YAML is awkward ❌

---

## What Matching Actually Needs

### 1. **Exploratory Workflow**
Users need to:
- Try different thresholds quickly
- See results immediately
- Compare different configs side-by-side
- Understand why matches happened
- Tune iteratively

**CLI + YAML is slow for this:**
- Edit YAML → Run CLI → Wait → Check results → Edit YAML → Repeat
- No immediate feedback
- Hard to compare configs
- No interactive exploration

**Better approach:**
- Python library/API (primary interface)
- Notebook-friendly for exploration
- Interactive tuning tools
- Optional CLI for batch/automated runs

### 2. **Iterative Tuning**
Matching requires tuning:
- Thresholds (what confidence is "good enough"?)
- Field weights (which fields matter more?)
- Algorithms (which algorithm works best for this data?)
- Blocking keys (what reduces comparisons without missing matches?)

**YAML config is rigid:**
- Hard to A/B test different configs
- No way to see impact of changes before committing
- Difficult to experiment

**Better approach:**
- Programmatic API for easy experimentation
- Config objects that can be modified in code
- Side-by-side comparison tools
- Interactive threshold tuning

### 3. **Validation & Debugging**
Users need to:
- See sample matches to validate quality
- Understand why matches were/were not found
- Debug false positives/negatives
- Review low-confidence matches

**CLI output is limited:**
- Can't easily inspect matches
- Hard to drill into specific cases
- No visual feedback
- Limited debugging capabilities

**Better approach:**
- Rich result objects with metadata
- Sample match viewing
- Match explanation/debugging tools
- Visual feedback (in notebooks or UI)

### 4. **Integration Patterns**
Matching is used in different contexts:
- **Exploration:** Data scientist in notebook
- **Batch processing:** Scheduled job in pipeline
- **Interactive tool:** Data analyst tuning matching
- **API service:** Real-time matching in application

**CLI + YAML only covers batch:**
- Doesn't work well in notebooks
- Hard to integrate into applications
- Not suitable for interactive tools

**Better approach:**
- Library-first design (works everywhere)
- Optional CLI for batch jobs
- Optional config files for repeatability
- Works in notebooks, scripts, APIs, CLI

---

## Recommended Architecture

### Primary Interface: Python Library

```python
from matcher import Matcher

# Simple, programmatic API
matcher = Matcher(
    left_source="data/customers_a.parquet",
    right_source="data/customers_b.parquet"
)

# Exact matching (Phase 1)
results = matcher.match_exact(field="email")

# Multi-field (Phase 2)
results = matcher.match_exact(
    fields=["email", "phone"],
    strategy="any"  # or "all"
)

# Fuzzy matching (Phase 3+)
results = matcher.match_fuzzy(
    fields={
        "email": {"algorithm": "jaro_winkler", "threshold": 0.85},
        "address": {"algorithm": "levenshtein", "threshold": 0.80, "weight": 1.5}
    },
    min_confidence=0.75
)

# Results are rich objects
print(f"Found {len(results.matches)} matches")
print(f"Average confidence: {results.stats.avg_confidence}")
results.sample(10)  # View sample matches
results.explain(match_id=123)  # Why did this match?
```

### Optional: Config Files (For Repeatability)

```yaml
# match_config.yml
sources:
  left: data/customers_a.parquet
  right: data/customers_b.parquet

matching:
  fields:
    email:
      algorithm: jaro_winkler
      threshold: 0.85
```

```python
# Load from config when needed
from matcher import Matcher

matcher = Matcher.from_config("match_config.yml")
results = matcher.run()
```

### Optional: CLI (For Batch/Automation)

```bash
# Simple CLI for batch runs
matcher run match_config.yml --output results.parquet

# Or use in scripts
matcher run match_config.yml | jq '.stats'
```

### Notebook-Friendly

```python
# Perfect for exploration
import pandas as pd
from matcher import Matcher

matcher = Matcher(left_source=df1, right_source=df2)

# Try different thresholds
for threshold in [0.8, 0.85, 0.9]:
    results = matcher.match_fuzzy(
        fields={"email": {"threshold": threshold}}
    )
    print(f"Threshold {threshold}: {len(results.matches)} matches")

# Compare configs
results_a = matcher.match_fuzzy(config_a)
results_b = matcher.match_fuzzy(config_b)
compare_results(results_a, results_b)
```

---

## Revised Phase 1: Library-First MVP

### What to Build

**Core Library:**
- `Matcher` class (main interface)
- Simple exact matching method
- Result objects with matches and metadata
- Works with Polars DataFrames (or paths to parquet)

**Not CLI + YAML, but:**
- Python API (primary)
- Optional config loading (for convenience)
- Optional CLI (for batch jobs)

### Implementation

```python
# matcher/core.py
from polars import DataFrame
from typing import Union, List

class Matcher:
    def __init__(
        self,
        left_source: Union[str, DataFrame],
        right_source: Union[str, DataFrame] = None  # None = deduplication
    ):
        """Initialize matcher with data sources."""
        self.left = self._load_source(left_source)
        self.right = self._load_source(right_source) if right_source else None

    def match_exact(
        self,
        field: str = None,
        fields: List[str] = None,
        strategy: str = "any"  # "any" or "all"
    ) -> MatchResults:
        """Exact matching on one or more fields."""
        # Implementation using Polars joins
        pass

    def _load_source(self, source: Union[str, DataFrame]) -> DataFrame:
        """Load from path or use DataFrame directly."""
        if isinstance(source, DataFrame):
            return source
        return pl.read_parquet(source)

class MatchResults:
    """Rich result object with matches and metadata."""
    def __init__(self, matches: DataFrame, stats: dict):
        self.matches = matches  # Polars DataFrame
        self.stats = stats

    def sample(self, n: int = 10) -> DataFrame:
        """View sample matches."""
        return self.matches.head(n)

    def to_parquet(self, path: str):
        """Save results."""
        self.matches.write_parquet(path)
```

### Usage Examples

**Phase 1 - Simple:**
```python
from matcher import Matcher

matcher = Matcher(
    left_source="data/customers_a.parquet",
    right_source="data/customers_b.parquet"
)

results = matcher.match_exact(field="email")
print(f"Found {len(results.matches)} matches")
results.to_parquet("results.parquet")
```

**Phase 1 - In Notebook:**
```python
import polars as pl
from matcher import Matcher

# Load data
df1 = pl.read_parquet("data/customers_a.parquet")
df2 = pl.read_parquet("data/customers_b.parquet")

# Match
matcher = Matcher(left_source=df1, right_source=df2)
results = matcher.match_exact(field="email")

# Explore results
results.sample(20)
results.stats
```

---

## Why This Is Better

### 1. **Fits Actual Workflow**
- Exploration: Use in notebook, iterate quickly
- Tuning: Modify config in code, see results immediately
- Production: Use config file or code, run in batch

### 2. **More Flexible**
- Works in notebooks (primary use case for matching)
- Works in scripts
- Works in applications (can be imported)
- Optional CLI for automation

### 3. **Better Developer Experience**
- Immediate feedback (no edit → run → wait cycle)
- Easy to experiment
- Rich result objects (not just files)
- Can integrate with existing tools

### 4. **Still Supports Automation**
- Config files for repeatability
- CLI for batch jobs
- Can be called from other tools
- But not the only way to use it

---

## Revised Plan Structure

### Phase 1: Library-First Exact Matching

**Build:**
- `Matcher` class with `match_exact()` method
- `MatchResults` class with matches and stats
- Works with Polars DataFrames or file paths
- Optional: Config loading (from dict or YAML)
- Optional: Simple CLI (`matcher run config.yml`)

**Not:**
- CLI-first design
- YAML-required workflow
- Tight coupling to hygge patterns

### Phase 2+: Add Features to Library

- Phase 2: Multi-field matching → `match_exact(fields=[...])`
- Phase 3: Fuzzy matching → `match_fuzzy(fields={...})`
- Phase 4: Multi-field fuzzy → Already in Phase 3 API
- Phase 5: Blocking → `match_fuzzy(blocking_keys=[...])`
- Phase 6: Deduplication → `Matcher(source)` (no right_source)

**All phases:** Add to library API, not CLI commands.

---

## Integration with Hygge (If Needed)

If matching needs to integrate with hygge pipelines:

```python
# In hygge flow
from matcher import Matcher

def match_step(config):
    matcher = Matcher(
        left_source=config.sources[0],
        right_source=config.sources[1]
    )
    results = matcher.match_exact(field=config.matching.field)
    return results.matches  # Returns DataFrame for hygge
```

**Key:** Matching is a library that hygge can use, not a hygge command.

---

## Questions to Answer

1. **Primary use case:**
   - Exploration/tuning in notebooks? → Library-first ✅
   - Batch automation only? → CLI + YAML might be OK
   - Both? → Library-first with optional CLI ✅

2. **User personas:**
   - Data scientists? → Need notebooks, library
   - Data engineers? → Need automation, CLI/config
   - Both? → Library-first with optional CLI

3. **Integration needs:**
   - Standalone tool? → Library or CLI both work
   - Part of larger system? → Library is better
   - Hygge integration? → Library that hygge can call

---

## Recommendation

**Build a Python library first**, with:
- Clean, simple API
- Works in notebooks (primary interface)
- Optional config file loading (for convenience)
- Optional CLI (for batch/automation)
- Rich result objects (not just files)

**Don't copy hygge's CLI + YAML pattern** - matching needs a different interface.

**The plan's phases are still good**, but change the interface from CLI + YAML to Library + Optional Config/CLI.
