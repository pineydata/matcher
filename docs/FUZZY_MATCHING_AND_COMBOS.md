# Fuzzy Matching and Combos

How fuzzy matching works in matcher (single rule), how to build combos (exact + fuzzy, or multiple fuzzy), and how to evaluate them.

---

## 1. Is Fuzzy a Single Rule or a Combo?

**Fuzzy is a single rule.** In matcher, fuzzy matching is always one rule:

- **One field:** `match_fuzzy(field="name", ...)` — you pass a single column name.
- **One similarity:** Jaro-Winkler (via rapidfuzz), with a single `threshold`.
- **No multiple fuzzy rules:** There is no built-in “fuzzy rule 1 OR fuzzy rule 2” or “fuzzy on field A and fuzzy on field B” in one call.

So “fuzzy” in this codebase = one field, one threshold, one similarity measure.

**Combos are things you build yourself:**

- **Exact + exact:** Built-in — use `match(rules=["email", ["first_name", "last_name"]])` or `match(...).refine(rule=[...])`. Multiple exact rules, cascading.
- **Exact + fuzzy:** Not a single API. You run both and merge:
  - `exact_results = matcher.match(rules="email")`
  - `fuzzy_results = matcher.match_fuzzy(field="name", threshold=0.85)`
  - Then combine the two sets of pairs (and deduplicate by `(left_id, right_id)` if a pair appears in both).
- **Fuzzy + fuzzy:** Also not built-in. You’d run `match_fuzzy(field="name", ...)` and `match_fuzzy(field="address", ...)` and merge/dedupe the pair sets yourself.

So: **fuzzy is a single rule**; a “combo” is when you **combine** that with other match outputs (exact and/or more fuzzy) in your own code.

---

## 2. How to Evaluate a Combo

Evaluation only needs a **single DataFrame of predicted pairs** with left and right IDs. So you can evaluate any combo by building that DataFrame and calling the evaluator on it.

### Build one set of pairs for the combo

- Get pairs from exact: `exact_results.matches` (columns include your left id and right id, e.g. `id`, `id_right`).
- Get pairs from fuzzy: `fuzzy_results.matches` (same id columns, plus `confidence`).
- Merge and deduplicate by `(left_id, right_id)` so each pair is counted once.

Example:

```python
# Exact + fuzzy combo
exact_pairs = exact_results.matches.select(["id", "id_right"]).unique()
fuzzy_pairs = fuzzy_results.matches.select(["id", "id_right"]).unique()
combo_pairs = pl.concat([exact_pairs, fuzzy_pairs]).unique(subset=["id", "id_right"])
```

If you want to keep confidence for thresholding, add a column (e.g. `confidence = 1.0` for exact and keep the fuzzy score for fuzzy-only rows when merging).

### Evaluate that combo

**Option A — Using MatchResults**

Build a DataFrame `combined_matches` that has at least your left id and right id columns (and optionally `confidence`). Wrap it in MatchResults and call `evaluate()`:

```python
from matcher.results import MatchResults

combo_results = MatchResults(
    combined_matches,
    original_left=matcher.left,
    source=matcher
)
metrics = combo_results.evaluate(
    ground_truth,
    left_id_col="id",
    right_id_col="id_right"
)
```

**Option B — Using the evaluator directly**

The evaluator only needs a “predicted” DataFrame with the same ID columns:

```python
from matcher.evaluators import SimpleEvaluator

evaluator = SimpleEvaluator()
metrics = evaluator.evaluate(
    combined_matches,
    ground_truth,
    left_id_col="id",
    right_id_col="id_right"
)
```

Same ground truth, same precision/recall/F1 interpretation.

### Optional: evaluate pieces and then the combo

To see how much each part contributes:

- Evaluate exact-only: `exact_results.evaluate(ground_truth, ...)`.
- Evaluate fuzzy-only: `fuzzy_results.evaluate(ground_truth, ...)`.
- Build the combo (exact + fuzzy, deduped), then evaluate that.

That gives you three metrics (exact, fuzzy, combo) so you can see whether the combo improves recall and how much it costs in precision.

### If the combo has a confidence column

- For a **single** fuzzy run, use `find_best_threshold(results.matches, ground_truth, ...)` to pick a threshold.
- For a **combo** (exact + fuzzy), you can still call `find_best_threshold(combined_matches, ground_truth, ...)` if `combined_matches` has a `confidence` column (e.g. 1.0 for exact, score for fuzzy). The sweep then treats the whole set as “fuzzy with confidence” and finds the best cutoff; exact pairs (confidence=1.0) are always included at every threshold. So you’re really tuning “how much of the fuzzy part to keep.”

---

## 3. Best Practices: Blended Algorithms (Several Exact + Several Fuzzy + Score)

Having a blended algorithm — several exact rules, several fuzzy rules, and a single score per pair — is a common and good approach. Matcher does not build that score for you; you run each rule, collect pairs, then combine and score in your own code. Below are best practices that work with matcher’s API.

### 3.1 Why blend

- **Exact rules** give high precision and are cheap (joins).
- **Fuzzy rules** recover pairs that exact miss (typos, variants), at the cost of more false positives if the threshold is low.
- **Multiple signals** (e.g. email exact + name fuzzy + address fuzzy) make the final decision more stable: a pair that matches on several rules is more likely to be correct than one that matches on one weak rule only.
- A **single blended score** lets you apply one accept/reject threshold and compare strategies (e.g. different weights) using the same evaluation pipeline.

### 3.2 Run each rule separately

- **Exact:** Use `matcher.match(rules=[...])` with all your exact rules (cascading), or `match(rules="email").refine(rule=["first_name", "last_name"])` etc. You get one DataFrame of pairs (no per-rule score from matcher).
- **Fuzzy:** Call `matcher.match_fuzzy(field=..., threshold=...)` once per field (e.g. name, address). Use a **low** threshold (e.g. 0.5–0.6) so you get many candidates; you will filter later with the blended score. Each result has a `confidence` column for that field.
- **Blocking:** Use the same `blocking_key` where it makes sense (e.g. zip or region) so all rules run within the same blocks and performance stays manageable. Blocking key must be present in both sources with the same name.

### 3.3 Collect candidate pairs and per-rule scores

- For each exact run, add a column to mark the rule and a score, e.g. `rule="email"`, `score=1.0` (exact = full confidence).
- For each fuzzy run, keep `id`, `id_right`, and `confidence`; optionally add `rule="name"` (or the field name).
- Concatenate all these DataFrames so you have one long list of **(left_id, right_id, rule, score)** rows. The same pair can appear multiple times (once per rule that fired).

### 3.4 Deduplicate by pair and build a blended score

- Group by `(left_id, right_id)` and combine the per-rule scores into one number. Common strategies:
  - **Max:** `blended_score = max(scores)`. Simple; any strong signal (e.g. exact email) gives 1.0.
  - **Weighted average:** `blended_score = sum(weight[rule] * score) / sum(weight[rule])` for rules that fired. Lets you emphasize email over name, etc.
  - **Any-exact-else-max-fuzzy:** If any rule was exact (score 1.0), set blended to 1.0; else use the max fuzzy score. Good when exact matches are fully trusted.
- After grouping, you have one row per pair with a single `blended_score` (and optionally which rules fired, for debugging).

### 3.5 Apply one threshold to the blended score

- Decide a cutoff (e.g. 0.85): keep pairs with `blended_score >= cutoff`. That is your final predicted set.
- Tune the cutoff using ground truth: sweep cutoffs (or use `find_best_threshold` if you expose the blended score as `confidence`) and pick the one that maximizes F1 or matches your precision/recall target.

### 3.6 Tune weights and thresholds with ground truth

- Start with simple weights (e.g. all 1.0, or exact=1.0 and fuzzy=max). Evaluate the blended result with `evaluate(ground_truth)`.
- If precision is low, raise the blended cutoff or give more weight to exact rules. If recall is low, lower the cutoff or add more fuzzy rules / lower per-fuzzy thresholds (so more candidates get a score).
- Compare: (1) exact-only, (2) each fuzzy-only, (3) blended with a few weight schemes. Use the same ground truth so you can see how much the blend improves over any single rule.

### 3.7 Keep the pipeline reproducible

- Use fixed per-fuzzy thresholds when you run `match_fuzzy` (even if low); then use the **blended** score and **one** cutoff for accept/reject. That way the same inputs and weights always give the same outputs.
- Optionally store the rule list and weights in config or code so you can re-run and audit.

### 3.8 Minimal code sketch (blended, no weights)

```python
import polars as pl
from matcher import Matcher, MatchResults, SimpleEvaluator

# 1) Exact
exact = matcher.match(rules=["email", ["first_name", "last_name"]])
exact_df = exact.matches.select(["id", "id_right"]).with_columns(
    pl.lit(1.0).alias("score")
)

# 2) Fuzzy (low threshold to get candidates)
fuzzy_name = matcher.match_fuzzy(field="name", threshold=0.5)
fuzzy_name_df = fuzzy_name.matches.select(["id", "id_right", "confidence"]).with_columns(
    pl.col("confidence").alias("score")
)

# 3) Combine and take max score per pair
all_pairs = pl.concat([exact_df.select(["id", "id_right", "score"]),
                       fuzzy_name_df.select(["id", "id_right", "score"])])
blended = all_pairs.group_by(["id", "id_right"]).agg(pl.col("score").max().alias("blended_score"))

# 4) Threshold
final = blended.filter(pl.col("blended_score") >= 0.85)

# 5) Evaluate (evaluator needs predicted pairs with left and right ID columns)
metrics = SimpleEvaluator().evaluate(final, ground_truth, left_id_col="id", right_id_col="id_right")
```

(You can add more fuzzy runs and weighted aggregation in the `group_by` step. For export or review, rejoin `final` to left/right to get full rows; for evaluation, the pairs DataFrame with `id` and `id_right` is enough.)

---

## 4. Summary

| Question | Answer |
|----------|--------|
| **Single or combo?** | Fuzzy in matcher is a **single rule** (one field, one threshold). A “combo” is when you combine that with other match outputs (exact and/or more fuzzy) in your own code. |
| **How to evaluate a combo?** | Merge all predicted pairs (exact + fuzzy, dedupe by left/right id), then call `evaluate()` on that merged set (via `MatchResults` or `SimpleEvaluator.evaluate()`). Optionally evaluate exact-only and fuzzy-only to compare. If the merged set has `confidence`, you can use `find_best_threshold` to tune how much of the fuzzy part to keep. |
| **Blended best practices?** | Run several exact and several fuzzy rules separately; collect (left_id, right_id, rule, score); dedupe by pair and compute a blended score (max, weighted average, or any-exact-else-max-fuzzy); apply one threshold; tune weights and cutoff with ground truth; keep the pipeline reproducible. |
