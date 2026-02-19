# Technical Review & Stress-Testing Game Plan

**Date:** 2026-02-18  
**Scope:** matcher MVP post–PR #1–5 (pineydata/matcher). Phases 1–4 complete: exact match, blocking, fuzzy, human-in-the-loop.  
**Perspective:** Principal data engineer + product + design (outcomes, UX, technical excellence).

A single pass through the technical review, then the stress-testing game plan. The review reflects the state of the repo after the five merged PRs and is written so the library can be read as a user or new maintainer would see it.

---

## Part 1: Technical Review

### 1.1 PRs in scope (pineydata/matcher)

| PR | Title | Delivered |
|----|--------|-----------|
| 1 | Technical debt: remove max_workers, document null handling | Clean API; null handling documented in README + algorithm docstrings. |
| 2 | Phase 3: Fuzzy matching (Jaro-Winkler) | `match_fuzzy()`, vectorized pipeline, confidence column, 7 fuzzy tests. |
| 3 | Phase 4: Human in the loop & evaluation workflow | `export_for_review()`, `sample()`, `evaluate(DataFrame)`, `find_best_threshold()`, improvement loop docs, 72 tests. |
| 4 | Phase 2 Blocking | Optional `blocking_key` on match/match_fuzzy; nulls-one-block; `_paired_blocks_by_key` DRY; 7 blocking tests including **blocking with nulls**. |
| 5 | refine() test coverage and schema fix | 5 refine() tests (entity resolution, dedup, all-matched, 2 error paths); combine-by-IDs + rejoin for consistent schema; 85 tests total. |

The two previously identified critical gaps—**refine() tests** and **blocking-with-nulls test**—are now covered by PR #5 and PR #4.

---

### 1.2 Strengths

**Architecture & design**
- **Component-based design:** `MatchingAlgorithm`, `Evaluator` swappable; Matcher orchestrates rules and blocking; ExactMatcher does joins; Deduplicator wraps Matcher and filters self-matches. Good for experimentation and data-driven tuning.
- **Deduplicator is a thin wrapper:** Clones the source, calls Matcher, filters self-matches. No duplicated matching logic; easy to extend (e.g. when `blocking_key` becomes a list).
- **Polars-only, in-memory:** No pandas, no hidden I/O. Single blocking key and no auto-blocking keep the API small (KISS, YAGNI).

**API & UX**
- **One obvious path:** Entity resolution → `Matcher`; dedup → `Deduplicator`. `match(rules="email")` and `match_fuzzy(field="name", threshold=0.85)` are the main entry points. Convention over configuration.
- **Fluent usage:** `matcher.match(rules="email").evaluate(ground_truth)`, `results.sample(n=50, seed=42).export_for_review("sample.csv")` read well and support the improvement loop.
- **Errors are loud and clear:** Empty left/right, missing ID column, missing rule/blocking field, invalid threshold, non-Utf8 for fuzzy all raise `ValueError` with column names and what’s available. Fixable without guessing.
- **Refine is a single extra rule:** `refine(matcher, rule=["first_name", "last_name"])` applies one rule to unmatched rows and merges. Combine-by-IDs then rejoin keeps schema consistent when the refine rule returns different columns.
- **Evaluation is first-class:** `results.evaluate(ground_truth)` and `find_best_threshold()`; improvement loop (match → evaluate → tune → re-run) is documented and testable. DataFrame-only `evaluate()` keeps “you load data, matcher operates on it.”

**Correctness & tests**
- **Exact:** Single/multi-field, multi-rule (OR), blocking (same/different blocks, missing key, nulls form one block), empty rules.
- **Fuzzy:** Threshold validation, null exclusion, empty result schema, dedup self-match filter, blocking within blocks.
- **Refine:** Entity resolution, deduplication, all-matched, and error paths (original_left None, missing id structure) covered.
- **Evaluation:** Perfect/partial/empty predictions and ground truth; column resolution (`id_right` / `id_match`); `find_best_threshold` structure and validation.
- **Sample data:** Entity resolution and dedup use generated data with known matches and `evaluate()` for precision/recall.

**Documentation**
- Module docstrings explain purpose, concepts, usage, and dependencies. README has Quick Start, null handling, and improvement loop; ROADMAP aligns with KISS/YAGNI.

---

### 1.3 Critical issues — status

Two issues were previously identified. Both are **resolved**:

| Issue | Status | Where fixed |
|-------|--------|-------------|
| **refine() had no test coverage** — cascading match (anti-join, combine, self-match filter) was untested. | **Done** | PR #5: five tests in `test_core.py`. |
| **Blocking with all-null blocking key undertested** — nulls form one block; subtle, easy to break. | **Done** | PR #4: `test_match_blocking_key_nulls_form_one_block` in `test_core.py`. |

**Fuzzy empty result:** Correct (zero rows, schema preserved); partially covered by existing test. No code change required.

No **new** critical issues for MVP scope. Remaining work is documentation, optional API polish, and stress testing.

---

### 1.4 Concerns & questions

**Data engineering**
- **Scale:** No benchmarks yet. ROADMAP’s “1M records in <30 minutes” and “blocking reduces comparisons >90%” are unmeasured. For MVP this is acceptable; run the stress-test plan before claiming production-ready for large data.
- **Memory & block size:** Fuzzy builds a similarity matrix per block. One very large block (e.g. 100K×100K) can cause high memory or OOM. A single coarse blocking key (e.g. **state**) often yields one huge block—users need a way to **further partition**. Document that `blocking_key` should keep block sizes bounded; if one key gives huge blocks, suggest pre-partitioning (composite column) or support for **multiple blocking keys** (see issues). Add stress test for one big block vs many small blocks (Phase C).
- **Nulls in blocking_key:** Null is **missing data**—semantically a row could belong to any block. **Current behavior:** nulls form one isolated block (nulls only match nulls). Conservative and avoids cross-block explosion. **Alternative:** null matches against all blocks (expensive). Document current behavior and this trade-off; consider whether to change or add an option (see issue #7).
- **Nulls in match fields:** Exact and fuzzy exclude nulls in match fields; README documents this. Optional: one README bullet that nulls in `blocking_key` form one block and the missing-data caveat.

**Product & API**
- **evaluate(ground_truth):** Accepts only a DataFrame. Accepting `Path`/path would be a small UX improvement; defer until someone asks (YAGNI).
- **Right ID in evaluate:** SimpleEvaluator fallback documented; for dedup, `right_id_col="id_right"` is the obvious choice. No change needed.
- **Rules vs rule:** `match(rules=...)` (plural) vs `refine(..., rule=...)` (singular) is intentional but slightly inconsistent. Minor; docstrings make it clear.

**Design & docs**
- **sample(n) when n > count:** Returns all rows; docstring already states this. Empty rules / empty left or right both raise; correct.
- **README examples and real API:** The “Component-Based Architecture” snippet shows `MyCustomMatcher` with `def match(self, left, right, rule)` but the real interface is `match(self, left, right, rule, left_id, right_id)`. Copy-paste would cause a signature error. Worth a pass: ensure every README (and CLAUDE.md) code snippet is runnable and matches the current API.
- **Normal-sized definition:** §1.6 below defines it for this review; users won’t see it unless they open this file. Issue #8 (define normal-sized in user-facing docs) addresses that.

---

### 1.5 Suggestions

1. **Document blocking and block size** (README or Matcher docstring): Recommend bounded block sizes; if one key gives huge blocks (e.g. state), suggest pre-partitioning or multiple blocking keys. Document nulls in `blocking_key` (one block) and the missing-data trade-off.
2. **Optional:** In README “Null handling,” add: “Nulls in `blocking_key` form a single block; matches only within that block. Null here means missing data—current behavior is conservative.”
3. **README code audit:** Go through every code block in README and CLAUDE.md; ensure signatures and imports match the current code. Fix the custom-algorithm example to show `match(self, left, right, rule, left_id, right_id)`.
4. **Defer:** Path/str for `evaluate(ground_truth)` and “.csv or .parquet only” path validation until there’s a real ask (YAGNI).
5. **Backlog:** Stress plan (Part 2) and GitHub issues #6–11 (multiple blocking keys, nulls in blocking_key, normal-sized docs, stress phases A/B/C) are the right next steps. Execute the plan; avoid scope creep until stress and docs are in a good place.

---

### 1.6 What “normal-sized data” means (for this review)

**Normal-sized data** means: volumes where the current implementation is expected to run without tuning, on typical hardware, **if** you use blocking when needed:

- **Entity resolution:** Left and right each under roughly **~10K–50K rows** when using a blocking key that yields manageable block sizes (e.g. no single block >>10K rows); or under **~1K–5K rows** without blocking.
- **Deduplication:** Single source under roughly **~50K rows** with blocking; smaller without blocking.

Exact limits depend on hardware, blocking key, and fuzzy vs exact. Phase B benchmarks should define numbers for your environment. “Large” or “production at scale” means beyond these ballpark ranges until benchmarks exist.

---

### 1.7 Overall assessment

**APPROVE for MVP and typical (normal-sized) use cases.**

The MVP is coherent, well-structured, and aligned with KISS, YAGNI, and the improvement loop. Critical gaps (refine() tests, blocking-with-nulls test) are closed. No blocking issues for production use at normal-sized scale **if** you run the test suite and accept that performance/limits are not yet documented by benchmarks. The library does one thing well: entity resolution and deduplication with exact and fuzzy rules, optional blocking, evaluation, and a simple refine/sample/export flow. Main risks are scale (unmeasured) and coarse blocking keys (one huge block)—both are tracked in issues and in the stress plan.

**Recommendation:** Treat the library as ready to use for normal-sized data (see 1.6). Next step is **stress testing** (Part 2) to document performance and limits and close ROADMAP Phase 2 success criteria.

---

## Part 2: Stress-Testing Game Plan

### 2.1 Goals

1. **Correctness under load:** Same matches with and without blocking at scale; no silent wrong results or crashes.
2. **Performance baseline:** Time and memory for 1K, 10K, 100K (and optionally 1M) rows for entity resolution and deduplication, with and without blocking.
3. **Fuzzy limits:** Identify block sizes and thresholds where memory or runtime becomes unacceptable.
4. **Documentation:** Capture results so users and future work (e.g. Phase 2 success criteria) have numbers.

### 2.2 Principles

- **Reproducible:** Fixed seeds and script that regenerates data or uses checked-in datasets.
- **Layered:** Start small (1K), then 10K, 100K; only add 1M if 100K is stable.
- **Scoped:** Focus on the main paths (exact match, blocking, fuzzy with blocking); no need to stress every combination.

---

### 2.3 Phase A: Correctness at scale (no performance target)

**A1. Exact match with and without blocking**
- Generate (or use) left/right with known ground truth (e.g. 100 known pairs, 10K rows each side). Run `match(rules="email")` with and without `blocking_key="zip_code"` (or similar so blocks partition the known pairs).
- **Assert:** Match counts identical; set of (left_id, right_id) identical. Run at 1K and 10K to catch any size-dependent bug.

**A2. Deduplication at scale**
- Same idea: one source, known duplicate pairs, with and without blocking. Assert same duplicate pair set.

**A3. Fuzzy with blocking**
- Data with known fuzzy pairs and a blocking key that separates them into blocks. Assert that with blocking you get the same (left_id, right_id) set as without blocking (and optionally same confidence values).

**Deliverable:** Script or pytest that runs A1–A3; run as part of CI or manually before releases.

---

### 2.4 Phase B: Performance benchmarks

**B1. Entity resolution – exact**
- Sizes: 1K, 10K, 100K (and 1M if desired) rows per side.
- Operations: `Matcher(left, right, left_id, right_id); matcher.match(rules="email")` and same with `blocking_key="zip_code"` (or a column that yields ~10–50 blocks).
- Measure: Wall-clock time and peak memory (e.g. `tracemalloc` or external tool).
- Output: Table (rows = size, cols = no blocking / blocking; cells = time and optionally memory).

**B2. Deduplication – exact**
- Same sizes, single source; `Deduplicator.match(rules="email")` with and without blocking.
- Same metrics and table.

**B3. Fuzzy – with blocking only**
- Sizes: 1K, 10K (and 100K if B1/B2 are fine). One blocking key so block sizes are bounded (e.g. 500–2K rows per block).
- Measure: Time and memory for `match_fuzzy(field="name", threshold=0.85, blocking_key="zip_code")`.
- Optional: One run with no blocking and 10K×10K to document “without blocking, fuzzy can be heavy.”

**Deliverable:** Script (e.g. `scripts/benchmark.py` or `tests/benchmark_perf.py`) that prints or writes the table; optional markdown in repo (e.g. `docs/PERFORMANCE.md` or a section in README).

---

### 2.5 Phase C: Edge cases and failure modes

**C1. One huge block**
- Left/right with same `blocking_key` for 90% of rows (one big block). Run exact and fuzzy with that blocking key. Measure time/memory and assert no wrong results. Document that blocking key should avoid one giant block—and that users with coarse keys (e.g. state) should further partition (composite column or future multiple blocking keys).

**C2. Multiple / finer blocks (e.g. state then zip)**
- If supporting or documenting multiple blocking keys: stress test with a composite or hierarchical block (e.g. block by state, then zip within state) so one big “state” block is split into many smaller blocks. Compare memory and time vs one huge block. Otherwise, document the “pre-partition with a composite column” pattern.

**C3. Many tiny blocks**
- Blocking key with many distinct values (e.g. unique or near-unique). Measure overhead; assert correctness.

**C4. Empty and single-row**
- Already partly covered; add explicit “left has 1 row, right has 10K” and “left 10K, right 1 row” for match and match_fuzzy to ensure no broadcast/join bugs.

**Deliverable:** Tests or benchmark script entries for C1–C4; short note in docs if you document recommended block sizes and the “further partition” pattern (or multiple blocking keys).

---

### 2.6 Implementation outline

1. **Data**
   - Option A: Extend `scripts/generate_test_data.py` to emit “stress” datasets (e.g. 10K, 100K) with known matches and blocking key.
   - Option B: Generate in the benchmark script with a fixed seed (e.g. random emails/names, inject known pairs).

2. **Runner**
   - Single script, e.g. `scripts/benchmark.py`, with modes: `correctness` (Phase A), `perf` (Phase B), `edge` (Phase C). Or separate pytest files for A/C and a script for B.

3. **CI**
   - Phase A (correctness at 1K/10K) can run in CI if fast (<2–3 min). Phase B/C can be manual or nightly; document “run `uv run python scripts/benchmark.py perf` before release.”

4. **Docs**
   - README or ROADMAP: “For performance numbers, see docs/PERFORMANCE.md” or “Run scripts/benchmark.py”. In ROADMAP, update Phase 2 success criteria when benchmarks exist (e.g. “1M in <30 min” checked or updated).

---

### 2.7 Priority order (updated post–PR #4 & #5)

With refine() and blocking-null tests in place, the order of work shifts to proving behavior and performance at scale:

| Priority | Item | Effort | Impact |
|----------|------|--------|--------|
| 1 | Phase A: Correctness at scale (script/tests) | Medium | High |
| 2 | Phase B: Performance benchmark script | Medium | High |
| 3 | Phase C: Edge cases (one huge block, multiple/finer blocks, many tiny blocks, single-row) | Low | Medium |
| 4 | PERFORMANCE.md + ROADMAP Phase 2 criteria | Low | Medium |

Do Phase A–B to get numbers and confidence at scale. Then Phase C and docs to document limits and close ROADMAP criteria.

---

## Summary

- **Review:** MVP is in good shape. Critical test gaps (refine(), blocking nulls) are closed. API is consistent, fail-fast, and documented. **APPROVE for MVP and typical (normal-sized) use cases** (see §1.6). Optional: README code audit so every snippet matches current API.
- **Stress plan:** Correctness at scale (Phase A) → performance benchmarks (Phase B) → edge cases (Phase C), with a single benchmark script and optional PERFORMANCE.md. Ready to kick the tires in production for normal-sized data; run stress tests before relying on it for large data or before claiming “production-ready at scale.”
- **Backlog:** Issues #6–11 (multiple blocking keys, nulls in blocking_key, normal-sized docs, stress phases A/B/C) are the right next steps. Execute the plan; avoid scope creep.
