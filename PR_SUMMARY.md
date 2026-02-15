---
title: PR Summary - Technical debt cleanup (max_workers removal, null handling docs)
tags: [breaking, documentation]
---

## Overview

- **Removed unused `max_workers`** from ExactMatcher, Matcher, and Deduplicator so the API matches reality (Polars handles parallelization; the parameter was never used).
- **Documented null handling** for exact matching: Polars inner joins exclude nulls (including null-to-null); docstring and README updated so behavior is explicit.
- **ROADMAP** updated: technical debt items marked done and Next Steps checkboxes updated.

## Key Changes

### Matching Algorithm

- `matcher/algorithms.py`:
  - ExactMatcher no longer accepts `max_workers`; class docstring notes Polars handles parallelization.
  - ExactMatcher.match() docstring documents null handling: join keys with nulls (including null-to-null) are excluded; suggests filling/dropping nulls if different behavior is needed.

### Matcher and Deduplicator

- `matcher/matcher.py`:
  - Matcher no longer accepts `max_workers`; algorithm initialization simplified (no hasattr/pass-through).
- `matcher/deduplicator.py`:
  - Deduplicator no longer accepts or passes `max_workers` to Matcher.

### Documentation

- `README.md`:
  - New **Null handling** subsection after Quick Start describing exact-match null behavior and when to preprocess nulls.
- `ROADMAP.md`:
  - Technical debt items 1 (Remove max_workers) and 2 (Document null handling) marked ✅ Done; corresponding Next Steps checkboxes checked.

## Testing

- All existing tests passing: `pytest` (47 tests). No test code changes; removal of `max_workers` is backward-incompatible for callers who passed it (they will get `TypeError: unexpected keyword argument 'max_workers'`).

---

**Note:** Add GitHub labels to the PR (e.g. `breaking`, `documentation`) so release notes can categorize this correctly.
