# Code Review: Phase 4 – Human in the Loop & User Evaluation Workflow

**PR Summary:** Export matches for human review (CSV + optional sample), evaluate() scores matches against a ground-truth DataFrame, README improvement loop and comparison examples, sample-data tests use evaluate() with known pairs.

---

## Strengths

- **Clear scope**: Phase 4 delivers exactly what it describes: CSV export, `sample()`, DataFrame-based `evaluate()`, docs, and tests. No scope creep.
- **Comfortable API**: `results.sample(n=50, seed=42).export_for_review("sample.csv")` is readable and chainable. Matches the “flow” and “convention over configuration” goals.
- **Fail-fast on invalid input**: `sample()` requires exactly one of `n` or `fraction` and raises `ValueError` for both/neither. No silent fallbacks.
- **Backward compatible**: `evaluate(ground_truth)` accepts a ground-truth DataFrame, and existing callers that pass DataFrames are unchanged.
- **Docs**: README improvement loop (six steps), export-for-review with `sample()`, and threshold-comparison example are clear and actionable.
- **Tests**: `test_export_for_review` checks CSV round-trip; `test_sample_*` cover n, fraction, validation, and empty; sample-data tests use `evaluate()` with known pairs and assert precision/recall. Evaluation is the standard way to judge quality in tests.
- **Code quality**: Polars-only, type hints, f-strings; module docstring in `results.py` updated with `sample()` and `export_for_review()`.

---

## Issues

### 1. `evaluate()` does not accept `Path` (API inconsistency)

- **Where:** `matcher/results.py` – `evaluate(ground_truth: Union[DataFrame, str], ...)`.
- **Fact:** `export_for_review(path: Union[str, Path])` accepts `Path`; `evaluate(ground_truth)` does not. Passing `Path("ground_truth.csv")` does not hit the file-load branch (`isinstance(path, str)` is false), so the evaluator receives a `Path` and fails later with a less clear error.
- **Suggestion:** Accept `Union[DataFrame, str, Path]` and normalize to a string path before the `isinstance(ground_truth, str)` check, e.g. `path_str = str(ground_truth)` when it’s a path-like, then use `path_str` for `endswith` and for `read_csv`/`read_parquet`. This keeps behavior the same for strings and makes `Path` work as users would expect.

### 2. Unsupported file extensions fail via Polars, not up front

- **Where:** `matcher/results.py` – branch that loads from path (lines 286–292).
- **Fact:** Only `.csv` is treated specially; everything else is passed to `pl.read_parquet()`. So `.xlsx`, no extension, or typo (e.g. `.csv `) lead to a Polars error instead of a clear “ground truth path must be .csv or .parquet” style message.
- **Suggestion:** After deciding “this is a path”, require `.csv` or `.parquet` (case-insensitive) and raise `ValueError` with a short message for any other extension. Then call `read_csv` or `read_parquet`. Keeps behavior explicit and avoids second-guessing.

---

## Fallback Warnings

- **Path branch in `evaluate()`:** There are two explicit branches (`.csv` vs else → parquet). There is no silent fallback to a third format; unknown extensions simply fail in Polars. The only improvement is to fail fast with a clear message (see Issue 2).
- **`sample(n)` when `n > count`:** Using `min(n, self.matches.height)` is deterministic and tested; it’s “cap at available rows,” not a hidden fallback. **Recommendation:** Document it in the docstring (e.g. “If `n` is greater than the number of rows, all rows are returned”) so the behavior is explicit and not surprising.

---

## Checklist Summary

| Area | Notes |
|------|--------|
| **DRY** | No new duplication in library code. Test helpers for “first 30 records” ground truth in `test_evaluation.py` could be a shared fixture later; optional. |
| **KISS** | Straightforward: CSV write, Polars `sample`, path → load by extension. |
| **YAGNI** | Features (CSV export, sample, CSV ground truth, improvement loop docs) match the stated Phase 4 need. |
| **Second-guess fallbacks** | Path handling has two explicit cases; suggest explicit .csv/.parquet check and doc for `sample(n)` when n > count. |
| **hygge/Rails** | API is comfortable and consistent with “improvement loop”; export + sample support flow. |
| **matcher principles** | Library-first, incremental, evaluation as the standard way to judge quality. |
| **Architecture** | MatchResults gains `sample()` and path in `evaluate()`; export stays CSV. Fits existing design. |
| **Backward compatibility** | Preserved; path support is additive. |
| **Code quality** | Polars, type hints, clear errors where validation exists. |
| **Pythonic** | f-strings, type hints, `Path` already used for `export_for_review`; extending to `evaluate` would align with “one obvious way” for paths. |

---

## Suggestions

1. **`evaluate(ground_truth)`:** Add `Path` to the union type and normalize to `str` when loading from file; then add an explicit check that the path has a `.csv` or `.parquet` extension (case-insensitive) and raise `ValueError` otherwise before calling Polars.
2. **`sample()` docstring:** Add one line that when `n` is provided and greater than the number of rows, all rows are returned (no error).
3. **Tests:** Consider a small shared fixture or helper in `test_evaluation.py` for the “first 30 eval pairs” ground truth to reduce repetition; low priority.

---

## Questions

- Do you want to support `Path` in `evaluate()` in this PR, or leave it for a follow-up?
- Do you want to add the explicit “.csv or .parquet only” check and message in this PR, or rely on Polars for now and tighten in a later change?

---

**Verdict:** Phase 4 is coherent, backward compatible, and aligned with DRY/KISS/YAGNI and the matcher/hygge principles. The main improvements are API consistency (`Path` in `evaluate`) and failing fast with a clear message for unsupported ground-truth file extensions; both are optional refinements. Ready to merge from a principles and checklist perspective; address Path and extension check as you prefer (this PR or follow-up).
