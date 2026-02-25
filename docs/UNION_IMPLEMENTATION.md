# Implementation: Union of MatchResults with score/on columns

## 1. Goal

- **Union:** Combine two or more `MatchResults` (e.g. from exact cascades and fuzzy cascades) into one `MatchResults` whose pair set is the union of all input pair sets, deduplicated by `(left_id, right_id_right)`.
- **Preserve provenance and scores:** For each pair, record which algorithm(s) produced it and the score, using a fixed set of columns so refinement is expressed in values, not column names.

## 2. Column design

**Per algorithm type (exact, fuzzy), two columns:**

| Column         | Type        | Meaning |
|----------------|-------------|--------|
| `exact_score`  | Float       | Score for exact match; always `1.0` when this pair came from an exact rule. Null if pair did not come from exact. |
| `exact_on` | str or list[str] | The rule that produced this pair (the `on` passed to match/refine). Null if pair did not come from exact. |
| `fuzzy_score`   | Float       | Similarity score in [0, 1] when pair came from a fuzzy rule. Null otherwise. |
| `fuzzy_on` | str or list[str] | The rule that produced this pair (the `on` passed to match/refine). Null otherwise. |

- **Rule in values, not column names:** Refinement and multiple rules are represented by the **value** of `exact_on` / `fuzzy_on` (the rule as given: e.g. `"email"` or `["first_name", "last_name"]`), not by extra columns. Column names stay fixed.
- **Format of exact_on / fuzzy_on:** Filled with the rule as **str** (single field) or **list[str]** (multiple fields), i.e. the same type as passed to `match(on=...)` / `refine(on=...)`.

## 3. Behavior

- **Inputs:** Two or more `MatchResults` from the same "world" (same `original_left`, same logical right / matcher). Each result may come from:
  - A single `match(on=...)` (exact or fuzzy), or
  - A cascade `match(...).refine(...).refine(...)` (each step is one rule).
- **Per input result we know:**
  - Algorithm type: exact vs fuzzy (from the matcher's algorithm at the time of that step; if refine doesn't support per-call algorithm yet, all steps in that run share one type).
  - The rule used at each step: the `on` argument (str or list of str).
  - For fuzzy: the `confidence` column from the algorithm output. For exact: no score today; we assign `1.0`.
- **Union of pairs:** Collect all `(left_id, right_id_right)` from each input, concat, then `.unique()`.
- **Building the combined table:**
  - Start from the union of pairs; rejoin to `original_left` and right to get left + right columns (canonical schema).
  - For each pair, set `exact_score` / `exact_on` and `fuzzy_score` / `fuzzy_on` from the input result(s) that contain that pair:
    - If the pair appears in an **exact** result: set `exact_score = 1.0` and `exact_on` to the rule that produced it (see "Which rule to store in exact_on / fuzzy_on" below).
    - If the pair appears in a **fuzzy** result: set `fuzzy_score` from that result's confidence and `fuzzy_on` to the rule that produced it.
  - If the same pair appears in multiple runs of the **same** algorithm (e.g. two exact cascades with different rules), define a rule: e.g. keep first, or prefer the row that has a non-null score, or take max score and choose exact_on / fuzzy_on (e.g. "first match wins").

## 4. Which rule to store in exact_on / fuzzy_on

- **exact_on** and **fuzzy_on** are filled with the rule as **str** or **list[str]** (the same type as passed to `match(on=...)` / `refine(on=...)`).
- **Single-step result:** One `match(on=...)` → the corresponding on column is that rule (e.g. `"email"` or `["first_name", "last_name"]`).
- **Cascade result:** The pair was first produced in one of the steps (match or a refine). Store the **rule of the step that produced this pair** (the step whose match added this (left_id, right_id)). So we need, when producing matches, to record "which rule produced this row" (e.g. add an internal column or track it in the pipeline so that when we later union we can set exact_on / fuzzy_on correctly).

**Implementation note:** Today, MatchResults don't carry "which rule produced this pair." So either:

- **Option A:** When building matches (in `match()` and in `refine()`), add an internal column (e.g. `_on`) holding the rule for that step as str or list[str]. Then when unioning we can map algorithm type + that value to `exact_on` / `fuzzy_on`. Option A requires adding and carrying it through the matching pipeline.
- **Option B:** Union only supports single-step results (no refine), so each result has one rule and we use that for exact_on / fuzzy_on. Cascades would need to be unioned per step or we add Option A later.

**Recommendation:** Option A so that union works with cascades and refinement; document that exact_on / fuzzy_on hold the rule (str or list[str]) of the step that produced the pair.

## 5. API

**Primary:**

- **`MatchResults.union(*others) -> MatchResults`**
  Instance method: `combined = results_exact.union(results_fuzzy)` or `combined = results_a.union(results_b, results_c)`.
- **Preconditions:** All arguments share the same `original_left` and the same right (same matcher/source). Same ID column names (left_id, right_id_right). Optional: require same `_source` (matcher/deduplicator).
- **Return:** New `MatchResults` with:
  - Rows = union of pair sets, deduplicated by (left_id, right_id_right).
  - Columns = canonical (left + right with `_right`) plus `exact_score`, `exact_on`, `fuzzy_score`, `fuzzy_on`. Nulls where a pair didn't come from that algorithm. exact_on and fuzzy_on hold the rule as str or list[str].

**Alternative:**

- **`MatchResults.union_all(results_iterable)`** class method or module-level `union_match_results(*results)` if you prefer a functional form.

## 6. Edge cases and rules

- **Same pair in multiple inputs:** Deduplicate by (left_id, right_id_right). For score/on: when the same pair appears in more than one input with the same algorithm type (e.g. two exact runs), decide policy (e.g. first non-null wins, or max score; same for exact_on / fuzzy_on). Document the chosen policy.
- **Same pair, exact and fuzzy:** Both `exact_*` and `fuzzy_*` columns can be non-null for that row.
- **Schema mismatch:** If one result has columns the other doesn't (e.g. one from fuzzy with confidence), the combined schema is canonical (left + right) plus the fixed score/on columns; algorithm-specific columns beyond score/on are not preserved unless we explicitly add more.
- **Empty inputs:** Union with empty MatchResults yields the other; union of all empty yields empty MatchResults with the same canonical + score/on columns.

## 7. Implementation steps (suggested order)

1. **Add score/on to single-run output (exact):** When returning from `match()` (and later from `refine()`), add `exact_score`/`exact_on` or `fuzzy_score`/`fuzzy_on` to the matches DataFrame (exact: 1.0 and rule as str or list[str]; fuzzy: existing confidence + rule as str or list[str]). This may require passing the current `on` and algorithm type through the match/refine pipeline.
2. **Carry "which rule" in refine:** In `refine()`, when producing new matches, tag each row with the rule used for that step (e.g. `_on` as str or list[str]). Use that when building `exact_on`/`fuzzy_on` so refinement is reflected in the value of the on columns.
3. **Implement `MatchResults.union(*others)`:** Union pair sets; rejoin to get canonical rows; for each pair, set exact_score/exact_on and fuzzy_score/fuzzy_on from the input(s) that contain it; apply policy for duplicates (same pair, same alg).
4. **Tests:** Union of two exact results; union of two fuzzy results; union of exact + fuzzy; same pair in both (both column sets populated); cascade result union with single-step result; empty inputs; ID column name consistency.
5. **Docs and examples:** Document column semantics (exact_on / fuzzy_on as str or list[str]), and duplicate policy; add a short example (exact cascade + fuzzy cascade, then union and filter by `exact_score` / `fuzzy_score`).

## 8. Open decisions

- **Naming:** `exact_score` / `exact_on` and `fuzzy_score` / `fuzzy_on` vs. a single `score` + `on` + `algorithm` (one row per pair per algorithm). Current design: one row per pair, multiple score/on columns. exact_on and fuzzy_on hold the rule as str or list[str].
- **More algorithms:** If we add another algorithm type later (e.g. "phonetic"), add `phonetic_score` and `phonetic_on` in the same way.
- **Duplicate policy:** When the same pair appears in multiple runs of the same algorithm, document whether we take first, max score, or something else for score and exact_on / fuzzy_on.

## 9. Implemented

- **Duplicate policy (implemented):** Preserve all runs; no coalescing. When the same algorithm type (e.g. exact) appears in multiple inputs, the first run keeps exact_score/exact_on, the second gets exact_score_2/exact_on_2, the third exact_score_3/exact_on_3, and so on. You see every run’s values and can apply your own rule (e.g. prefer highest score).
- **Union:** `MatchResults.union(*others)` implemented in matcher.results. Score/on columns are discovered dynamically via `*_score` / `*_on` naming (see matcher.algorithms.is_score_on_column, kind_of_score_on_column).
