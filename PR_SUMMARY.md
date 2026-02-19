---
title: PR Summary - Phase 4: Human in the Loop, Evaluation Workflow & Threshold Tuning
tags: [enhancement, feature, documentation]
---

## Overview

- **Phase 4 complete**: Export matches for human review (CSV + optional sample) and a documented improvement loop so users can improve matching over time.
- **evaluate(ground_truth)** takes a **DataFrame** only (you load CSV/Parquet); API stays "you load data, matcher operates on it."
- **find_best_threshold()** sweeps confidence thresholds on fuzzy results and returns the threshold that maximizes F1, plus a curve for plotting—data-driven tuning instead of guessing (e.g. 0.85).
- **README** documents the improvement loop and export-for-review with `sample()`; evaluation tests use known pairs and precision/recall.

## Key Changes

### Export for review & evaluation (Phase 4)

- `matcher/results.py`:
  - **export_for_review(path)** writes **CSV** for Excel or any tool; **sample(n=..., fraction=..., seed=...)** for a manageable review sample (e.g. `results.sample(n=50, seed=42).export_for_review("sample.csv")`).
  - **evaluate(ground_truth)** accepts a **DataFrame** only; docstring notes loading from file in user code. **sample()** docstring notes that when `n` exceeds row count, all rows are returned.

### Threshold tuning (fuzzy)

- `matcher/evaluators.py`:
  - **find_best_threshold(matches, ground_truth, ...)** for fuzzy match results with a `confidence` column. Sweeps a default grid (0.50–1.00, step 0.05), at each step keeps pairs with confidence ≥ threshold and evaluates with SimpleEvaluator. Returns **best_threshold**, **best_f1**, **best_precision**, **best_recall**, and **curve** (list of threshold/precision/recall/f1 for plotting). Optional **thresholds** and **evaluator** args. Raises if `matches` has no `confidence` column.
- `matcher/__init__.py`: **find_best_threshold** exported.

### Documentation

- `README.md`: Improvement loop (six steps), evaluate with DataFrame and "load from CSV if needed," export-for-review with CSV and `sample()`, threshold-comparison example.
- `ROADMAP.md` / `INVESTMENT_PLAN.md`: Phase 4 wording and completion status.

### Tests

- `tests/test_core.py`: export_for_review (CSV round-trip), sample (n, fraction, validation, empty).
- `tests/test_evaluation.py`: evaluate with DataFrame (including after loading from CSV/Parquet); **find_best_threshold** (structure, best_f1 is max over curve, curve length); **find_best_threshold_requires_confidence** (clear error when no confidence column).
- `tests/test_with_sample_data.py`: entity resolution and deduplication with evaluate() and known ground truth (precision/recall assertions).

## Testing

- All tests passing: `pytest` (68 tests).
- Coverage: export-for-review, sample(), evaluate(DataFrame), find_best_threshold (sweep + error path), sample-data evaluation.

---

**PR labels**: Add `enhancement` or `feature` and `documentation` on GitHub so release notes group this under new features and docs.
