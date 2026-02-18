---
title: PR Summary - Phase 3 Fuzzy Matching
tags: [enhancement, feature]
---

## Overview

- **Fuzzy matching API**: `Matcher.match_fuzzy(field=..., threshold=0.85)` and `Deduplicator.match_fuzzy(...)` for typo-tolerant matching on a single string column using Jaro–Winkler similarity.
- **Vectorized pipeline**: Polars → Arrow → rapidfuzz `cdist` (no row-by-row Python loops); single batch similarity matrix, multi-core via `workers=-1`.
- **Same result shape as exact match**: Full joined rows plus `confidence` (0–1); `evaluate()` and `refine()` work unchanged.

## Key Changes

### Dependencies

- `pyproject.toml`: Added `rapidfuzz>=3.0.0`, `pyarrow>=14.0.0`, `numpy>=1.24.0` for fuzzy matching and the Polars–rapidfuzz bridge.

### Matcher

- `matcher/matcher.py`:
  - New `match_fuzzy(field, threshold=0.85)`: validates field and threshold; drops nulls on field; normalizes (lowercase, strip) in Polars; exports via Arrow to lists; runs `rapidfuzz.process.cdist` with `JaroWinkler.similarity`; builds (left_id, right_id, confidence) pairs and rejoins to full left/right. Returns `MatchResults` with `original_left` for refine/evaluate.
  - **DRY**: Extracted `_empty_fuzzy_result()` so both "no valid rows" and "no pairs above threshold" use one helper instead of duplicating empty-pairs + join logic.
  - Handles empty inputs and "no pairs above threshold" with explicit schema so joins don't fail. Uses temp column `_right_id_val` when left_id/right_id share the same name (e.g. both `"id"`).
  - Module docstring updated with fuzzy usage and dependencies.

### Deduplicator

- `matcher/deduplicator.py`:
  - New `match_fuzzy(field, threshold=0.85)`: delegates to `_matcher.match_fuzzy()`, then filters self-matches (`id != id_right`) and returns `MatchResults` with same `original_left`. Docstring and module docs updated.

### Tests

- `tests/test_core.py`: Seven fuzzy tests—basic match, typos, missing field (left/right), threshold validation, high threshold fewer matches, empty when no matches, and Deduplicator fuzzy (including no self-matches).
- `tests/test_with_sample_data.py`: Two sample-data tests—fuzzy entity resolution on `first_name` (≥20 matches, confidence in [0.85, 1]) and fuzzy deduplication (≥50 pairs, no self-matches).

## Testing

- All tests passing: `pytest` (55 tests). Fuzzy coverage: unit tests for API, validation, and edge cases; integration tests against generated entity-resolution and deduplication sample data.

## Principles

- **KISS/YAGNI**: Single algorithm (Jaro–Winkler), single threshold, one field; no new algorithm class.
- **Convention over configuration**: Sensible default threshold 0.85; same `MatchResults` contract as `match()`.
- **Backward compatibility**: No changes to existing `match()` or `MatchResults`/evaluator contracts.

---

**Reminder:** Add GitHub labels to the PR (e.g. `enhancement`, `feature`) so release notes can categorize this change.
