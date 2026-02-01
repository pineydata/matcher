# Matching Algorithm Implementation Plan

Results-driven phased implementation for entity resolution and deduplication.

**Architecture:** Library-first design - optimized for exploration, tuning, and iteration.

**Design Principles:** KISS (Keep It Simple, Stupid) and YAGNI (You Aren't Gonna Need It)
- Build only what you need right now, not what you might need later
- Start with the absolute minimum, add complexity only when proven necessary
- One way to do things initially, add options only when users request them

---

## Overview

This plan outlines a focused 4-phase approach to building matching capabilities. Each phase builds on the previous, adding features only when needed and validated.

**Core Principle:** Start simple, add complexity only when there's a real need and measurable improvement.

**KISS/YAGNI Approach:**
- Phase 1: Absolute minimum - single field exact matching only (~100 lines of code)
- Phase 2+: Add features only when you have a real, current use case
- Dependencies: Start with Polars only, add others when needed
- API: One way to do things, add options later when requested

**Key Architectural Decision:** Build a Python library first, optimized for:
- Interactive exploration in notebooks
- Quick iteration and tuning
- Rich result objects for debugging
- Human-in-the-loop workflows from the start

---

## Standing on Giants' Shoulders: Python Ecosystem

**What Already Exists (Use These):**

### Data Processing: Polars ✅
- **Already in plan** - Rust-backed DataFrame library
- Blazing fast performance with automatic parallelization
- Lazy evaluation and query optimization
- Streaming API for out-of-core processing
- Perfect for exact matching and blocking (Phase 1-2)

### Fuzzy Matching: rapidfuzz ✅
- **Already in plan** - Battle-tested fuzzy matching library
- Fast string similarity algorithms (Jaro-Winkler, Levenshtein, etc.)
- Python-native, well-maintained
- Use for Phase 3 fuzzy matching

### Configuration & Validation: Pydantic ⚠️ (Add Later If Needed)
- **YAGNI Decision:** Don't add in Phase 1. Python API is simplest configuration.
- **When to Add:** When users request config files, or when you need to save/load complex configs
- Type-safe configuration models
- Schema validation before execution
- JSON schema generation (useful for Power BI integration)

### Data Standardization Libraries ⚠️ (Add Later If Needed)
- **YAGNI Decision:** Don't add in Phase 1. Exact matching doesn't need standardization.
- **When to Add:** When you have real data where exact matching fails due to formatting, and you can measure improvement
- **nameparser**: Parse and standardize name components (for Phase 3+ if needed)
- **usaddress-scourgify**: Standardize US addresses (for Phase 3+ if needed)
- **jellyfish**: Phonetic matching (for Phase 3+ if needed)
- **Simple normalization**: Lowercase, trim, remove extra spaces (build yourself if needed)

### Existing Record Linkage Libraries (Evaluate, Don't Reinvent)
- **recordlinkage**: Full-featured record linkage library with blocking, indexing, comparison
  - **Consider:** Use for blocking/indexing strategies (Phase 2)
  - **Skip:** Heavy framework, may be overkill for our use case
- **dedupe**: Machine learning-based deduplication
  - **Consider:** For advanced matching (beyond Phase 3)
  - **Skip:** Requires training data, adds complexity

**What's Missing (Build This):**
- **Matching orchestration library** - No dominant Python library exists
- **Human-in-the-loop workflows** - Power BI + translytical flows integration
- **Library-first API** - Most existing tools are CLI or framework-heavy
- **Polars-native matching** - Existing libraries use pandas, we need Polars

**Our Value Proposition:**
- Lightweight orchestration layer on top of proven components
- Polars-native (not pandas) for performance
- Library-first API (not CLI) for exploration
- Simple, minimal API - add complexity only when needed
- Human-in-the-loop workflows (Phase 4, when needed)
- Focus on UX and developer experience

---

## Phase 0: Decision Criteria

**Goal:** Establish measurable outcomes and design decisions before building anything.

### Success Criteria

**Deduplication (Single Source):**
- [ ] Can identify duplicate records in a single source
- [ ] Identifies all known duplicates (100% recall on test dataset)
- [ ] No false positives (100% precision, or acceptable threshold)
- [ ] Output format is usable (match pairs with metadata)

**Entity Resolution (Cross-Source):**
- [ ] Can match records across 2 sources with known matches
- [ ] Identifies all known matches (100% recall on test dataset)
- [ ] No false positives (100% precision, or acceptable threshold)
- [ ] Output format is usable (match pairs with metadata)

**Auditability:**
- [ ] Can verify why records matched (which fields matched)
- [ ] Can trace back to source records
- [ ] Match results include enough metadata to validate
- [ ] Results are deterministic (reproducible)

**Performance:**
- [ ] Test dataset (1K-10K records)
- [ ] Small production (100K records)
- [ ] Scales reasonably (doesn't break with 10x data)

### Test Dataset Requirements

**Deduplication:**
- Single source with 1000 records
- 50 known duplicates (documented ground truth)
- Include variations: exact, near-duplicates, partial duplicates

**Entity Resolution:**
- 2 sources with 500 records each
- 30 known matches (documented ground truth)
- Include variations: exact, fuzzy, multi-field matches

**Ground Truth:**
- Document which records should match
- Include edge cases (nulls, typos, variations)
- Include non-matches (similar but shouldn't match)

### Architecture Decision: Library-First

**Primary Interface:** Python library (works in notebooks, scripts, applications)

```python
from matcher import Matcher

matcher = Matcher(
    left_source="data/customers_a.parquet",
    right_source="data/customers_b.parquet"
)

# Simple case: same field name in both sources (Phase 1)
results = matcher.match(field="email")

print(f"Found {len(results.matches)} matches")
results.matches.head(10)  # Explore immediately
```

**Rationale:**
- Matching requires exploration and tuning (not just "run and done")
- Notebooks are primary use case
- Rich result objects enable better debugging
- Human-in-the-loop requires interactive workflows

### Output Format Decision

**Phase 0 Decision:** Start with simple match pairs format:

```python
# Match pairs (default) - Phase 1
results.matches  # Polars DataFrame with match results

# For human review: Add in Phase 4 when needed
# For Power BI: Add in Phase 4 when needed
```

**Rationale:**
- Pairs are transparent and auditable
- Start simple - add review formats when you actually need them (Phase 4)
- Power BI integration comes later, not in Phase 1

### Decision Gate

**Proceed to Phase 1 if:**
- [ ] Test dataset created with ground truth
- [ ] Success criteria defined and measurable
- [ ] Architecture decision made (library-first)
- [ ] Output format decided
- [ ] Performance targets set

**If test dataset missing:** Create one first. Don't proceed without validation data.

**Time Estimate:** 2-3 days (including test dataset creation)

---

## Phase 1: Exact Matching (MVP)

**Goal:** Prove the architecture works with the simplest possible matching.

**KISS/YAGNI Approach:** Build the absolute minimum - single field exact matching only. No standardization, no multi-field, no Power BI prep, no field mapping suggestions.

### What to Build

**Core Library (Minimal):**
- `Matcher` class (main interface)
- `match(field="email")` method - single field only, same name in both sources
- `MatchResults` class - just wraps matches DataFrame
- Works with Polars DataFrames or file paths
- Basic error handling (file not found, field doesn't exist, empty data)

### API Design

**Phase 1: Absolute Minimum**

```python
from matcher import Matcher
import polars as pl

# Initialize with file paths or DataFrames
matcher = Matcher(
    left_source="data/customers_a.parquet",  # or pl.DataFrame
    right_source="data/customers_b.parquet"  # or pl.DataFrame, None for dedup
)

# Single field exact matching (same field name in both sources)
# That's it - this is all Phase 1 does
results = matcher.match(field="email")

# View results
print(f"Found {len(results.matches)} matches")
results.matches.head(10)  # Explore matches
```

**Note:** Multi-field, different field names, standardization, field mapping, and Power BI export are NOT in Phase 1. Add them in later phases only when you have a proven need.

### Implementation

**Phase 1: Absolute Minimum (~100 lines of code)**

```python
# matcher/core.py
import polars as pl
from polars import DataFrame
from typing import Union, Optional

class Matcher:
    def __init__(
        self,
        left_source: Union[str, DataFrame],
        right_source: Optional[Union[str, DataFrame]] = None
    ):
        """Initialize matcher with data sources.

        Args:
            left_source: Path to parquet file or Polars DataFrame
            right_source: Path to parquet file or Polars DataFrame.
                        If None, performs deduplication on left_source.
        """
        if isinstance(left_source, str):
            self.left = pl.read_parquet(left_source)
        else:
            self.left = left_source

        if right_source is None:
            self.right = None
        elif isinstance(right_source, str):
            self.right = pl.read_parquet(right_source)
        else:
            self.right = right_source

        self._validate_sources()

    def _validate_sources(self):
        """Basic validation."""
        if self.left.height == 0:
            raise ValueError("Left source is empty")
        if self.right is not None and self.right.height == 0:
            raise ValueError("Right source is empty")

    def match(self, field: str) -> MatchResults:
        """Exact matching on single field. Field must exist in both sources.

        Args:
            field: Field name (must exist in both sources with same name)
        """
        if field not in self.left.columns:
            raise ValueError(f"Field '{field}' not found in left source. Available: {self.left.columns}")

        if self.right is not None:
            if field not in self.right.columns:
                raise ValueError(f"Field '{field}' not found in right source. Available: {self.right.columns}")
            # Entity resolution: cross-source join
            matches = self.left.join(
                self.right,
                on=field,
                how="inner"
            )
                else:
            # Deduplication: self-join (exclude same record)
            matches = self.left.join(
                self.left,
                on=field,
                how="inner",
                suffix="_match"
            ).filter(
                pl.col("id") != pl.col("id_match")  # Exclude self-matches
            )

        return MatchResults(matches)

class MatchResults:
    """Simple result object with matches."""

    def __init__(self, matches: DataFrame):
        self.matches = matches

    @property
    def count(self) -> int:
        """Number of matches found."""
        return len(self.matches)
```

**That's it. Nothing else in Phase 1.**

### Error Handling

**Phase 1: Handle the basics:**
- Missing source files (clear error message)
- Missing fields (list available fields)
- Empty sources (informative message)

**Don't handle yet (add later if needed):**
- Complex validation
- Type mismatches (let Polars handle it)
- Null value handling (handle as you encounter them)
- Field name suggestions (add in Phase 2 if needed)

### Success Criteria

- [ ] Finds all 30 known matches in test dataset (100% recall)
- [ ] Zero false positives (100% precision)
- [ ] Runs in <30 seconds for test dataset
- [ ] Works in notebook (can explore results interactively)
- [ ] Error messages are clear and helpful
- [ ] Results are auditable (can verify matches)
- [ ] Can load from file paths or DataFrames
- [ ] Can access matches via `results.matches`

### Decision Gate

**Proceed to Phase 2 if:**
- [ ] All success criteria met
- [ ] Performance too slow for production datasets
- [ ] Have datasets >100K records

**Proceed to Phase 3 if:**
- [ ] Phase 1 works
- [ ] Real data has typos/variations that exact matching misses
- [ ] Need fuzzy matching to find important matches

**Proceed to Phase 4 if:**
- [ ] Phase 1 works
- [ ] Need human review workflow
- [ ] Have matches that need validation

**Time Estimate:** 1-2 days (simplified from 3-4 days per KISS/YAGNI)

---

## Phase 2: Blocking (Performance Optimization)

**Goal:** Handle larger datasets efficiently by reducing comparisons.

### What to Add

- Single blocking key (zip code, area code, etc.)
- Generate candidate pairs within blocks only
- Reduce O(n²) comparisons dramatically

**YAGNI Decision:** No automatic blocking suggestions. Users know their data and can specify blocking keys.

### API Design

```python
# Blocking for performance - simple, one key
results = matcher.match(
    field="email",
    blocking_key="zip_code"  # Optional, user-specified
)
```

**Note:** Multiple blocking keys and auto-blocking are NOT in Phase 2. Add later if users request them.

### Implementation

```python
def match(
    self,
    field: str,
    blocking_key: Optional[str] = None
) -> MatchResults:
    """Exact matching with optional blocking for performance.

    Args:
        field: Field name to match on
        blocking_key: Optional field to use for blocking (e.g., "zip_code")
    """
    if blocking_key:
        # Group records by blocking key
        left_blocks = self.left.group_by(blocking_key)
        right_blocks = self.right.group_by(blocking_key) if self.right else left_blocks

        # Match within each block
        matches = []
        for block_value in left_blocks.keys() & right_blocks.keys():
            left_block = left_blocks[block_value]
            right_block = right_blocks[block_value]

            block_matches = left_block.join(
                right_block,
                on=field,
                how="inner"
            )
            matches.append(block_matches)

        matches_df = pl.concat(matches)
    else:
        # No blocking: simple join
        matches_df = self.left.join(
            self.right,
            on=field,
            how="inner"
        )

    return MatchResults(matches_df)
```

### Success Criteria

- [ ] Can process 1M records in <30 minutes
- [ ] Blocking reduces comparisons by >90%
- [ ] Match quality unchanged (same matches found)
- [ ] Performance scales linearly with data size
### Decision Gate

**Proceed to Phase 3 if:**
- [ ] Phase 2 works
- [ ] Real data has typos/variations that exact matching misses
- [ ] Need fuzzy matching to find important matches

**Proceed to Phase 4 if:**
- [ ] Phase 2 works
- [ ] Need human review workflow
- [ ] Have matches that need validation

**Time Estimate:** 2-3 days (simplified from 3-4 days)

---

## Phase 3: Fuzzy Matching

**Goal:** Handle typos and variations ("John Smith" vs "J. Smith").

### What to Add

- New `match_fuzzy()` method
- Single algorithm (Jaro-Winkler - good default)
- Single threshold (no per-field thresholds initially)
- Add `rapidfuzz` dependency
- Simple normalization (lowercase, trim) if needed

**YAGNI Decision:**
- No algorithm selection - use Jaro-Winkler only
- No per-field thresholds - use one threshold for all fields
- No weights - use simple averaging if multi-field added later
- No complex normalization - add libraries only if simple normalization doesn't work

### API Design

**Phase 3: Start Simple**

```python
# Single-field fuzzy matching - simple
results = matcher.match_fuzzy(
    field="name",
    threshold=0.85  # That's it
)
```

**Add complexity only if needed:**
- Multi-field fuzzy (if users request it)
- Different field names (if users request it)
- Algorithm selection (if Jaro-Winkler doesn't work well)
- Per-field thresholds (if one threshold doesn't work)

### Implementation

**Phase 3: Simple Fuzzy Matching**

```python
def match_fuzzy(
    self,
    field: str,
    threshold: float = 0.85,
    blocking_key: Optional[str] = None
) -> MatchResults:
    """Fuzzy matching on single field using Jaro-Winkler.

    Args:
        field: Field name to match on (must exist in both sources)
        threshold: Minimum similarity score (0.0-1.0)
        blocking_key: Optional field to use for blocking
    """
    from rapidfuzz import fuzz

    # Simple normalization (lowercase, trim)
    left_normalized = self.left.with_columns([
        pl.col(field).str.to_lowercase().str.strip().alias(f"_norm_{field}")
    ])
    right_normalized = self.right.with_columns([
        pl.col(field).str.to_lowercase().str.strip().alias(f"_norm_{field}")
    ])

    # Compare all pairs (or within blocks if blocking_key provided)
    matches = []
    if blocking_key:
        # Match within blocks
        left_blocks = left_normalized.group_by(blocking_key)
        right_blocks = right_normalized.group_by(blocking_key)

        for block_value in left_blocks.keys() & right_blocks.keys():
            left_block = left_blocks[block_value]
            right_block = right_blocks[block_value]

            for left_row in left_block.iter_rows(named=True):
                for right_row in right_block.iter_rows(named=True):
                    score = fuzz.jaro_winkler(
                        left_row[f"_norm_{field}"],
                        right_row[f"_norm_{field}"]
                    ) / 100.0
                    if score >= threshold:
                        matches.append({
                            "left_id": left_row.get("id"),
                            "right_id": right_row.get("id"),
                            "confidence": score
                        })
    else:
        # Match all pairs
        for left_row in left_normalized.iter_rows(named=True):
            for right_row in right_normalized.iter_rows(named=True):
                score = fuzz.jaro_winkler(
                    left_row[f"_norm_{field}"],
                    right_row[f"_norm_{field}"]
                ) / 100.0
                if score >= threshold:
                    matches.append({
                        "left_id": left_row.get("id"),
                        "right_id": right_row.get("id"),
                        "confidence": score
                    })

    return MatchResults(pl.DataFrame(matches))
```

### Success Criteria

- [ ] Can match "John Smith" to "J. Smith" with confidence >0.8
- [ ] Can match addresses with typos
- [ ] False positive rate acceptable (<5% on test dataset)
- [ ] Performance acceptable (<5 minutes for 10K records)
- [ ] Confidence scores make sense (can audit matches)
- [ ] Can combine with blocking for performance

### Decision Gate

**Proceed to Phase 4 if:**
- [ ] Phase 3 works
- [ ] Need human review workflow
- [ ] Have matches that need validation

**Time Estimate:** 2-3 days (simplified from 4-5 days)

---

## Phase 4: Human in the Loop

**Goal:** Enable human review and scoring of matches using Power BI Reports and translytical flows.

### What to Build

**YAGNI Approach:** Start with the minimum - export/import workflow. Users build their own Power BI reports.

**Core Functionality:**
- Export matches with source data for review
- Load reviewed matches back
- Simple statistics (approved/rejected counts)

**Add Later (if needed):**
- Power BI report template (users can build their own)
- Translytical flows (file export/import is enough initially)
- Automatic tuning (users can tune manually)
- Complex statistics (precision/recall when you have ground truth)

### API Design

**Phase 4: Simple Review Workflow**

```python
# Export matches for review
results = matcher.match_fuzzy(field="name", threshold=0.85)

# Export with source data
results.export_for_review("matches_for_review.parquet")

# Users review in Power BI (or Excel, or any tool)
# They add columns: human_score (True/False), human_notes

# Load reviewed matches back
reviewed = pl.read_parquet("reviewed_matches.parquet")

# Simple analysis
stats = results.analyze_review(reviewed)
print(f"Total: {stats['total']}")
print(f"Approved: {stats['approved']}")
print(f"Rejected: {stats['rejected']}")
```

**Add later if needed:**
- Precision/recall (when you have ground truth)
- Automatic tuning (when manual tuning is too time-consuming)
- Power BI report template (when multiple users need the same report)

### Review Workflow Design

**Simple File-Based Workflow:**

1. **Export for Review:**
   ```
   Matching Results → Parquet File → Users load into their tool
   ```

2. **Human Review:**
   ```
   Users review in Power BI/Excel/any tool → Add human_score column → Save
   ```

3. **Load Results:**
   ```
   Load reviewed file → Analyze statistics → User adjusts thresholds manually
   ```

**YAGNI Decision:** No Power BI report template, no translytical flows, no automatic tuning. Keep it simple - file export/import is enough.

### Implementation

**Phase 4: Simple Review Workflow**

```python
class MatchResults:
    def export_for_review(self, output_path: str):
        """Export matches with source data for review.

        Args:
            output_path: Path to save review dataset
        """
        # Join matches back to source data
        review_data = self.matches.join(
            self.left_source,
            left_on="left_id",
            right_on="id",
            how="left"
        ).join(
            self.right_source,
            left_on="right_id",
            right_on="id",
            how="left",
            suffix="_right"
        )

        # Add empty columns for human review
        review_data = review_data.with_columns([
            pl.lit(None).alias("human_score"),  # True/False
            pl.lit(None).alias("human_notes")
        ])

        review_data.write_parquet(output_path)

    def analyze_review(self, reviewed: pl.DataFrame) -> dict:
        """Simple statistics from review.

        Args:
            reviewed: DataFrame with human_score column added by reviewers

        Returns:
            dict with total, approved, rejected counts
        """
        return {
            "total": len(reviewed),
            "approved": len(reviewed.filter(pl.col("human_score") == True)),
            "rejected": len(reviewed.filter(pl.col("human_score") == False))
        }
```

**Add later if needed:**
- Precision/recall (when you have ground truth)
- Automatic tuning (when users request it)
- Complex statistics (when needed)

### Review Workflow Implementation

**Step 1: Export for Review**
```python
# After matching
results = matcher.match_fuzzy(field="name", threshold=0.85)
results.export_for_review("matches_for_review.parquet")
```

**Step 2: Human Review**
- Users load parquet file into Power BI, Excel, or any tool
- Users add `human_score` column (True/False) and `human_notes` (optional)
- Users save reviewed file

**Step 3: Analyze Results**
```python
# Load reviewed matches
reviewed = pl.read_parquet("reviewed_matches.parquet")

# Simple statistics
stats = results.analyze_review(reviewed)
print(f"Total: {stats['total']}")
print(f"Approved: {stats['approved']}")
print(f"Rejected: {stats['rejected']}")

# User adjusts threshold manually based on results
# Then re-runs matching with new threshold
```

### Success Criteria

- [ ] Can export matches with source data for review
- [ ] Users can load exported file into their tool of choice
- [ ] Can load reviewed matches back
- [ ] Can analyze review results (simple counts)
- [ ] Users can manually adjust thresholds based on results

### Decision Gate

**This is the final phase.** Once this works, you have full matching capabilities with human-in-the-loop validation.

**Time Estimate:** 2-3 days (simplified from 5-7 days - no Power BI report template, no translytical flows)

---

## Decision Framework

### When to Add Complexity

1. **Current phase works** (meets all success criteria)
2. **Real need exists** (not hypothetical - you have actual data/use case)
3. **Performance/data quality issues** justify the complexity
4. **Can measure improvement** (before/after metrics)

### When to Stop

- Current phase meets all needs
- Adding complexity doesn't improve outcomes
- Performance is acceptable
- Match quality is acceptable

### When to Simplify

- Current approach is too complex for the problem
- Simpler solution works just as well
- Maintenance burden too high

---

## Output Types

### Match Results (Data)

**Match Pairs (Default):**
```python
results.matches  # Polars DataFrame
# Columns: left_id, right_id, match_fields, confidence
```

**For Human Review:**
```python
results.matches_for_review()  # Includes full records
results.export_for_review("review.parquet")  # Power BI format
```

**Match Groups (Deduplication):**
```python
groups = results.to_groups()
# Columns: group_id, record_ids, canonical_record_id
```

### Execution Results (Operational)

**Match Statistics:**
```python
results.stats
# {
#   "count": 123,
#   "avg_confidence": 0.92,
#   "confidence_distribution": {...},
#   "performance": {"duration_seconds": 2.5}
# }
```

**Review Statistics:**
```python
review_stats = results.analyze_review_results(reviewed_matches)
# {
#   "precision": 0.95,
#   "false_positive_rate": 0.05,
#   "reviewed_count": 100,
#   "total_matches": 123
# }
```

---

## Implementation Notes

### Architecture

**Where matching lives:**
- New `matcher/` package (standalone library)
- **Phase 1:** Single file `matcher/core.py` (~100 lines)
- **Later:** Split into modules only when single file gets too large (>500 lines)
  - `matcher/core.py` (Matcher class)
  - `matcher/results.py` (MatchResults class)
  - `matcher/algorithms.py` (fuzzy algorithms - wraps rapidfuzz, Phase 3+)
  - `matcher/review.py` (human review workflows, Phase 4+)

**Dependencies (Minimal - Add Only When Needed):**
- **Polars** (data processing) - Foundation layer, Phase 1+
- **rapidfuzz** (fuzzy matching, Phase 3+) - Don't reinvent algorithms

**Add Later If Needed:**
- **Pydantic** (configuration & validation) - Only if config files are needed
- **nameparser** (name standardization) - Only if simple normalization doesn't work
- **usaddress-scourgify** (address standardization) - Only if simple normalization doesn't work
- **jellyfish** (phonetic matching) - Only if fuzzy matching misses important matches
- **recordlinkage** (evaluate for blocking strategies) - Only if custom blocking needed
- Power BI API - Only if automated export/import needed

**What We Build (Orchestration Layer):**
- Lightweight API that orchestrates Polars + rapidfuzz
- Simple blocking strategies optimized for Polars
- Simple human-in-the-loop workflows (file-based, Phase 4+)
- Simple result objects (start minimal, add methods when needed)
- Python API (no config files initially - add later if needed)

**What We Don't Build:**
- Fuzzy matching algorithms (use rapidfuzz)
- DataFrame operations (use Polars)
- Configuration validation (use Pydantic)
- Low-level string similarity (use rapidfuzz)

### Testing Strategy

**Unit Tests:**
- Matching algorithms
- Result object methods
- Error handling
- Review analysis

**Integration Tests:**
- End-to-end matching with test datasets
- Ground truth validation
- Power BI export/import
- Review workflow

**Test Data:**
- Version control test datasets
- Synthetic data generator for edge cases
- Real data samples (anonymized)

---

## Summary

**KISS/YAGNI Principles Applied:**
- **Phase 1:** Absolute minimum - single field exact matching only (~100 lines, 1-2 days)
- **Phase 2+:** Add features only when you have a real, current use case
- **Dependencies:** Start with Polars only, add others when needed
- **API:** One way to do things, add options later when requested
- **Time Savings:** 15-20 days → 6 days (if all phases needed)

**Start with Phase 1 (exact matching).** Don't add complexity until:
1. Current phase works
2. You have a real need (not hypothetical)
3. You can measure improvement

**Key Architectural Decision:** Build a Python library first, optimized for exploration. Keep it simple - add complexity only when proven necessary.

**Human-in-the-loop from Phase 4:** Simple file-based workflow. Users build their own Power BI reports. Add automation later if needed.

**Stop when you have something that works for your use case.** Don't build features you don't need.

**Next step:** Create test dataset with known matches, then build Phase 1 (absolute minimum).
