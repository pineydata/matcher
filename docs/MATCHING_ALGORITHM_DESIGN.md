# Designing a Matching Algorithm

A practical guide to deciding the combination of exact and fuzzy matching, assessing a blocking strategy, and evaluating your algorithm using the matcher library.

---

## 1. Choosing Exact vs Fuzzy (and Combining Them)

**When to use exact matching**

- The field is a stable identifier or is well-normalized (e.g. email, ID, cleaned postcode).
- You want zero false positives on that field (e.g. `on="email"`).

**When to use fuzzy matching**

- The field has typos or spelling variants (names, addresses, product names).
- You're willing to trade some precision for recall and will tune via threshold.

**How to combine them in matcher**

- **Cascading exact rules** (recommended): Use `match(on=...)` then `refine(on=...)` for each additional rule. The first rule runs on the full set; later rules run only on left rows that didn't match yet (cascading). Example:

  ```python
  results = (
      matcher.match(on="email")
      .refine(on=["first_name", "last_name"])
      .refine(on=["address"], blocking_key="zip_code")
  )
  ```

- **Exact then exact refine**: Same idea: `matcher.match(on="email").refine(on=["first_name", "last_name"])` — still all exact.

- **Exact vs fuzzy as separate strategies**: The library has `match(on=...)` (exact, or fuzzy when you pass `matching_algorithm=FuzzyMatcher(...)`). There is no separate `match_fuzzy()` method. To combine "exact first, then fuzzy on unmatched" you'd run both (e.g. `match(on="email")` and `match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))`), then merge/deduplicate results yourself (e.g. via `MatchResults.union()`), or run fuzzy only on a subset of data you derive from "unmatched" left rows.

**Practical design steps**

1. Start with **exact rules only** on the cleanest identifiers (e.g. email, then name, then address).
2. **Evaluate** (see §3). If recall is too low and you see missed matches due to typos/variants, add **fuzzy** on the relevant field (e.g. name) via `match(on=["name"], matching_algorithm=FuzzyMatcher(...))` and compare metrics.
3. For fuzzy, **tune the threshold** with `find_best_threshold()` so you don't guess (e.g. 0.85); the library is built for this data-driven choice.

---

## 2. Designing and Assessing a Blocking Strategy

**What blocking does**

Restricts candidate pairs to rows that share the same value in a blocking key (e.g. `zip_code`). So comparisons only happen within blocks. That cuts work and memory; it does **not** change the matching logic inside each block.

**How to choose a blocking key**

- Use a field that **true pairs usually share** (e.g. same zip, same area code, same first letter of surname). If true pairs often have different values (e.g. different zips), blocking will **drop those pairs** and **reduce recall**.
- Prefer keys that **split the data** into many small blocks rather than a few huge ones (so you get both speed and fewer pairs per block).

**Assessing blocking**

1. **Correctness (recall)**  
   With blocking, you only see pairs that fall in the same block. So:
   - Run **with** blocking: `matcher.match(on="email", blocking_key="zip_code")`.
   - Run **without** blocking: `matcher.match(on="email")`.
   - Compare **recall** (and match counts) on the same ground truth. If recall drops with blocking, some true pairs lie in different blocks — either fix the key (e.g. use a field that aligns better with how matches actually occur) or use no blocking / a different key.

2. **Performance**  
   Compare runtime and, if you can, number of comparisons (or block sizes). Blocking is successful when you get a big reduction in work with little or no loss in recall.

**Caveat**

The library only considers pairs that share a **common** blocking key value (inner join on block values). So if a true pair has left in block A and right in block B, they will **never** be compared. Your assessment must be "with vs without blocking + same ground truth".

**In your codebase**

Per-rule blocking is supported: you can use one key for the first rule and another for the next (e.g. email within zip, then name within state for unmatched), so you can align blocking with each rule's logic.

---

## 3. Evaluating the Algorithm

The library is built for a **measure → tune → compare** workflow.

**Get ground truth**

- A Polars DataFrame with at least `left_id` and `right_id` (the known true pairs). Load from CSV/Parquet if needed.

**Run and evaluate**

```python
# Run your chosen algorithm (exact, fuzzy, blocking, etc.)
results = matcher.match(on="email").refine(on=["first_name", "last_name"], blocking_key="zip_code")
# Or: results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))

metrics = results.evaluate(
    ground_truth,
    left_id_col="id",
    right_id_col="id_right"
)
# precision, recall, f1, true_positives, false_positives, false_negatives
```

**Improvement loop (from README)**

1. Get ground truth.
2. Run matcher (rules + optional blocking).
3. `metrics = results.evaluate(ground_truth, ...)`.
4. Change something (rules, threshold, blocking key).
5. Re-run and compare metrics.
6. Repeat until precision/recall are good enough.

**For fuzzy only**

Use `find_best_threshold()` so you don't pick the threshold by hand:

```python
from matcher import Matcher, FuzzyMatcher, find_best_threshold

results = matcher.match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))
best = find_best_threshold(
    results.matches,
    ground_truth,
    right_id_col="id_right"
)
# best["best_threshold"], best["best_f1"], best["curve"]
```

Run `match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=...))` with a low threshold so you have enough scored pairs; then `find_best_threshold` picks the best cutoff by F1 (and you can inspect precision/recall on the curve).

**What to compare**

- **Exact vs fuzzy:** e.g. `match(on="email")` vs `match(on=["name"], matching_algorithm=FuzzyMatcher(threshold=0.85))` on the same data and ground truth.
- **Blocking:** same rules, with vs without `blocking_key`, same ground truth — check recall and runtime.
- **Thresholds:** multiple `match(on=[...], matching_algorithm=FuzzyMatcher(threshold=t))` or `find_best_threshold(..., thresholds=[...])` and compare F1/precision/recall.

---

## Summary

| Decision           | How to decide                                                                 | How matcher helps                                                                 |
|--------------------|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| **Exact vs fuzzy** | Use exact on clean IDs; add fuzzy where typos/variants hurt recall.          | `match(on=...)` for exact (and cascading); `match(on=[...], matching_algorithm=FuzzyMatcher(...))` for fuzzy; evaluate both. |
| **Combining rules**| Order by confidence (e.g. email → name → address); use cascading.             | One rule per `match(on=...)`; chain with `refine(on=...)` for extra rules.      |
| **Blocking**       | Choose a key that true pairs usually share; avoid keys that split true pairs. | `blocking_key` (single or per-rule); compare recall with vs without blocking.    |
| **Evaluation**     | Ground truth + precision/recall/F1; iterate by changing rules, threshold, or blocking. | `results.evaluate(ground_truth)`; `find_best_threshold()` for fuzzy; improvement loop. |

Design by **data and ground truth** (what fields are reliable, where do matches live?), add **blocking** only when you need speed and you've checked recall, and **evaluate every change** with `evaluate()` (and `find_best_threshold()` for fuzzy) so the algorithm is driven by measured precision/recall rather than guesswork.
