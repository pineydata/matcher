---
title: PR Summary - Per-rule blocking keys on match()
tags: [enhancement, feature]
---

## Overview

- **Per-rule blocking**: `blocking_key` on `Matcher.match()` and `Deduplicator.match()` can be a list (one entry per rule) so each rule uses its own blocking column—e.g. email within zip, name within state.
- **Backward compatible**: Passing a single string for `blocking_key` is unchanged; all rules share that key.
- **Optional no-blocking per rule**: List entries can be `None` so that rule runs unblocked (full candidate set) while others use blocking.
- **Partially addresses**: GitHub issue #6 (multiple blocking keys), #7 (nulls in blocking_key). This PR adds per-rule blocking (different key per rule); #6’s composite key (e.g. `["state", "zip_code"]`) and #7’s null semantics are separate follow-ups.

## Key Changes

### Blocking API

- `matcher/matcher.py`:
  - `blocking_key` type: `Optional[Union[str, List[Optional[str]]]]`. String = one key for all rules. List = length must match number of rules; `blocking_key[i]` is the column for rule `i`, or `None` for no blocking.
  - Execution: loop per rule; for each rule resolve blocks from that rule’s key (or full pair if `None`), run only that rule over those blocks, combine matches with OR.
  - Validation: list length vs. rule count; distinct non-`None` keys validated once on both frames (DRY). Module docstring updated for per-rule blocking.
- `matcher/deduplicator.py`:
  - `match()` signature and docstring updated to accept same `blocking_key` type; passes through to `Matcher.match()`. Docstring example added for list form.

### Tests

- `tests/test_core.py`:
  - **Per-rule**: Two rules with different blocking keys (email by zip, name by state); per-rule finds (1,3), single-key by zip finds 0.
  - **List length**: `blocking_key` list length must equal number of rules (ValueError).
  - **Same key for two rules**: `blocking_key=["zip_code", "zip_code"]` gives same result as `blocking_key="zip_code"`.
  - **None for one rule**: One rule with blocking, one with `None`; both run, matches combined.

## Testing

- All tests passing: `pytest` (89 tests).
- New: 4 tests for per-rule blocking (behavior, list length, same key for two rules, None entry).

---

**Principles**: KISS (reuse existing block logic per rule), backward compatibility (string unchanged), incremental (addresses “blocks per rule” before composite multi-column keys). **Reminder:** Add GitHub labels (e.g. `enhancement`, `feature`) to this PR for release notes.
