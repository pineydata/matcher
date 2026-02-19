---
title: PR Summary - Phase 2 Blocking (optional blocking_key)
tags: [enhancement, feature]
---

## Overview

- **Optional blocking** on `match()` and `match_fuzzy()` for Matcher and Deduplicator: pass `blocking_key="zip_code"` (or any column) to restrict candidate pairs to rows that share the same value, reducing comparisons and memory on large datasets.
- **Exact match**: blocks are derived from common values of the blocking key; matching runs per block then results are combined (same matches as no blocking when blocks align).
- **Fuzzy match**: similarity matrix is built per block so memory stays bounded; pairs are merged and deduplicated across blocks.
- **Backward compatible**: all new parameters are optional; existing call sites unchanged.

## Key Changes

### Matcher (blocking)

- `matcher/matcher.py`:
  - `match(rules, blocking_key=None)`: when `blocking_key` is set, iterates over common block values, runs algorithm per block, combines with existing OR logic.
  - `match_fuzzy(field, threshold, blocking_key=None)`: when set, runs fuzzy pipeline per block via `_fuzzy_block_pairs()` and `_fuzzy_pairs_for_blocks()`, then concat + unique + single rejoin.
  - `_block_pairs()` and `_fuzzy_block_pairs()`: nulls in blocking key form one block (filter by `is_null()`). Block key must exist in both left and right (validated with existing `_validate_fields()` for blocking_key).

### Deduplicator (pass-through)

- `matcher/deduplicator.py`:
  - `match(rules, blocking_key=None)` and `match_fuzzy(field, threshold, blocking_key=None)` accept `blocking_key` and pass it to the internal Matcher; self-match filtering unchanged.

### Documentation

- `ROADMAP.md`: Phase 2 marked complete; API and success criteria updated.
- `matcher/matcher.py`, `matcher/deduplicator.py`: module and method docstrings updated for blocking.
- `README.md`, `CLAUDE.md`: one-line mention and common-pattern example for `blocking_key`.

### Tests

- `tests/test_core.py`:
  - Blocking: same results with/without blocking when blocks align; no matches when no common blocks; missing `blocking_key` column raises; fuzzy blocking keeps matches within same block; one-block fuzzy equals no-block count; Deduplicator blocking and fuzzy blocking (self-matches filtered). Seven new tests.

### Refactor (post-review)

- `matcher/matcher.py`:
  - **DRY**: Shared block-pair logic extracted into `_paired_blocks_by_key()`; `_block_pairs()` and `_fuzzy_block_pairs()` delegate to it.
  - **Empty-result path**: When there are no matches, join key is explicit (use `left_id` when same in both frames, else require first left column in right) and raises a clear `ValueError` if the column is missing in right instead of relying on an implicit assumption.

## Testing

- All tests passing: `pytest` (79 tests).
- New coverage: exact and fuzzy blocking for Matcher and Deduplicator, validation and edge cases (no common blocks, one block).

---

**Principles**: KISS (single blocking key, no auto-suggestions), YAGNI (no multiple blocking keys), backward compatible, library-first (optional arg, notebook-friendly). **Reminder:** Add GitHub label `enhancement` or `feature` to this PR for release notes.
