---
title: PR Summary - Phase 4: Human in the Loop, Evaluation Workflow & Threshold Tuning
tags: [enhancement, feature, documentation]
---

## Overview

- **Phase 4 complete**: Export matches for human review (CSV + optional sample) and a documented improvement loop so users can improve matching over time.
- **evaluate(ground_truth)** takes a **DataFrame** only (you load CSV/Parquet); API stays "you load data, matcher operates on it."
- **find_best_threshold()** sweeps confidence thresholds on fuzzy results and returns the threshold that maximizes F1, plus a curve for plotting—data-driven tuning instead of guessing.
- **PR feedback addressed**: Docs aligned to DataFrame-only API; evaluator right-id resolution when left/right id share the same name; fail-fast validation for `sample()` (n, fraction) and `find_best_threshold(thresholds)`.

## Key Changes

### Export for review & evaluation (Phase 4)

- `matcher/results.py`:
  - **export_for_review(path)** writes CSV for Excel or any tool; **sample(n=..., fraction=..., seed=...)** for a manageable review sample.
  - **evaluate(ground_truth)** accepts a DataFrame only; docstring notes loading from file in user code.
  - **sample()** validates `n >= 0` and `0 < fraction <= 1`; clear ValueError for invalid inputs.

### Evaluation & threshold tuning

- `matcher/evaluators.py`:
  - **find_best_threshold(matches, ground_truth, ...)** for fuzzy results with a `confidence` column; returns best_threshold, best_f1, best_precision, best_recall, curve. Validates custom **thresholds**: non-empty, numeric, in [0, 1]; raises clear errors instead of returning None.
  - **SimpleEvaluator**: When `right_id_col == left_id_col` and a suffixed column (e.g. `id_right`) exists, use the suffixed column so entity-resolution results with default `id` resolve correctly.
- `matcher/__init__.py`: **find_best_threshold** exported.

### Documentation

- **README.md**: Ground truth as DataFrame only (load with `pl.read_csv`/`pl.read_parquet`); improvement loop step 3 notes `right_id_col="id_right"` for both deduplication and entity resolution when left/right id share the same name.
- **ROADMAP.md**: Phase 4 success criteria, metrics, timeline, and next steps updated to "load ground truth from CSV/Parquet and pass DataFrame to evaluate()" (no path support).
- **PR_REVIEW.md**: Summary and strengths updated to reflect DataFrame-only evaluate() API.

### Tests

- `tests/test_core.py`: export_for_review, sample (n, fraction, validation, empty, **n negative**, **fraction out of range**).
- `tests/test_evaluation.py`: evaluate with DataFrame (CSV/Parquet load); find_best_threshold (structure, curve, **empty thresholds**, **invalid threshold value**); **ground_truth_30_pairs** fixture for DRY.
- `tests/test_with_sample_data.py`: entity resolution and deduplication with evaluate() and known ground truth.

## Testing

- All tests passing: `pytest` (72 tests).
- Coverage: export-for-review, sample() (including validation), evaluate(DataFrame), find_best_threshold (sweep + confidence error + threshold validation), sample-data evaluation.

---

**PR labels**: Add `enhancement` or `feature` and `documentation` on GitHub so release notes group this under new features and docs.
