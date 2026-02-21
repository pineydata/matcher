# Tutorial review: datasets and variety (DX/education lens)

**Role:** Former educator, now developer experience / relations.  
**Scope:** Suitability of tutorial datasets, coverage of bases, and whether to leverage existing ecosystem datasets.

---

## What works well

- **Single narrative:** One story (prep → exact → measurement loop → fuzzy → blocking → design → dedup) with a clear habit: measure, change one thing, compare. That’s strong pedagogy.
- **Stable evaluation set:** The 50×50 evaluation set with 30 known pairs and hardcoded `eval_left_1..30` / `eval_right_1..30` gives reproducible numbers across notebooks. Good for “change one thing and see the delta.”
- **Small, fast runs:** Sizes (50, 500, 1000 rows) keep runs quick and lower cognitive load while concepts are introduced.
- **Ground truth is explicit:** Learners see how ground truth is built (07) or documented (01), which reinforces that matching is evaluated, not guessed.

---

## Dataset suitability: concerns

### 1. **Two different “worlds” of data**

| Dataset | Used in | Ground truth | Match variety |
|--------|---------|--------------|----------------|
| **Entity resolution (500×500, 40 pairs)** | 01 (prep), 04 (fuzzy demo) | `ground_truth.md` (left_1–right_1, etc.) | Email-only, name-only, address+zip, mixed |
| **Evaluation (50×50, 30 pairs)** | 02, 03, 05, 06 | Hardcoded `eval_left_{i}` / `eval_right_{i}` | **Only email** (identical pairs) |

The **evaluation** set is “perfect”: 30 pairs that are identical on email (and name, address, zip). So:

- Exact match on email gets all 30; cascading adds no *true* extra pairs, only risk of false positives.
- Blocking on `zip_code` doesn’t split true pairs (same zip per pair), so blocking never hurts recall in the tutorial.
- The **measurement loop** is demonstrated on data where the “change one thing” (e.g. add name rule, add blocking) doesn’t create interesting trade-offs—learners see numbers change, but the *reason* (e.g. “blocking put true pairs in different blocks”) never appears.

So: the evaluation set is great for **stability and speed**, but weak for **teaching when and why** rules or blocking help or hurt.

### 2. **Main ER set is underused in the narrative**

`generate_test_data.py` deliberately creates four match types:

- Email-only (10)
- Name-only (10)
- Multi-field: address + zip (10)
- Mixed: email or name (10)

The tutorial almost never uses the **multi-field (address+zip)** scenario. Matching on `["address", "zip_code"]` isn’t in the notebooks; only email, name, and zip (for blocking) are. So:

- The “exotic” design of the main ER data isn’t fully reflected in the learning path.
- Learners who read `ground_truth.md` may wonder why address isn’t used.

Either the narrative should briefly show multi-field rules (and possibly one exercise), or the docs should state that this set is for “deeper experimentation” and that the tutorial focuses on email/name/blocking.

### 3. **Fuzzy matching on “clean” data**

Fuzzy is demonstrated on:

- Entity resolution (500×500): names are **exact** across pairs (e.g. “Kevin Bacon” left and right).
- Evaluation (50×50): again identical names for the 30 pairs.

So there are **no typos, no “Jon” vs “John”, no nickname/abbreviation**. Learners see confidence scores and threshold tuning, but not the situation fuzzy is meant to fix: similar-but-not-identical strings. The lesson is “how to run fuzzy and tune a threshold,” not “when fuzzy beats exact.”

To cover the bases, the tutorial would benefit from at least one small dataset (or subset) where true pairs have **intentional name variation** (typos, spacing, casing) so that:

- Exact on name misses some pairs.
- Fuzzy recovers them and the measurement loop shows recall gain.

### 4. **Deduplication set is very regular**

Dedup data: 50 duplicate pairs (same email), 900 uniques. Duplicates are exact on email and very regular (no fuzzy-needed duplicates). That’s fine for “same API, one table,” but doesn’t show dedup-specific challenges (e.g. same person, different emails; or fuzzy duplicates).

---

## Do we need more variety?

**Yes, in a targeted way:**

1. **Blocking:** At least one scenario where true pairs **don’t** share the blocking key (e.g. different zip), so that “with blocking → recall drops” is visible and the choice of key is meaningful.
2. **Fuzzy:** At least one scenario where true pairs have **name variation** (typos/similar names), so fuzzy clearly adds recall over exact.
3. **Optional:** One place (e.g. 01 or an “advanced” note) that shows or references **multi-field** rules (e.g. address+zip) so the main ER set’s design isn’t orphaned.

You don’t need many new datasets: one or two **small, purpose-built** variants (e.g. “evaluation-with-name-variation”, “evaluation-with-split-zips”) could be generated alongside the current ones and used in specific notebooks.

---

## Existing ecosystem datasets: reuse vs reinvent

### Good candidates

| Source | What it is | Pros | Cons for matcher tutorial |
|--------|-------------|------|----------------------------|
| **recordlinkage (FEBRL)** | `load_febrl1()` (dedup), `load_febrl4()` (link two tables); 500–5k records; ground truth via `return_links=True` | Standard in record linkage; real-ish person data; typos/variation in FEBRL duplicates | Pandas + MultiIndex; column names differ (given_name, surname, postcode, etc.). Would need a thin loader → Polars and column mapping to matcher’s expected names (or tutorial uses FEBRL names). |
| **Leipzig / DBLP-ACM, Abt-Buy** | Bibliographic / product tables; 2k–2.6k rows; CSV + ground truth | Well-known benchmarks; different domain (titles, authors, products) | Schema and domain differ a lot from “customers”; larger; need download + normalization. Better for “benchmark” or “advanced” than core tutorial. |
| **Zenodo “Datasets for Supervised Matching”** | 13 datasets; clean and dirty; multiple formats | Rich variety, dirty data | Heavier integration; multiple formats; may be overkill for “cover the bases” in the first tutorial. |

### Recommendation

- **Don’t replace** the current tutorial data entirely. It’s simple, fast, and aligned with the narrative. Keeping it preserves reproducibility and avoids “run this notebook after downloading X from the internet.”
- **Optional “use a standard benchmark” step:** Add an **optional** section (e.g. in the preamble or a short “08_optional_febrl” notebook) that:
  - Uses `recordlinkage.datasets.load_febrl4(return_links=True)` (or FEBRL1 for dedup).
  - Converts to Polars and maps columns to a consistent schema (e.g. `given_name` → `first_name`, `surname` → `last_name`, `postcode` → `zip_code`), then runs the same matcher flow.
  - Positions this as “same ideas, different data; you can try other benchmarks (Leipzig, Zenodo) the same way.”
- That way you **leverage the ecosphere** (FEBRL) without reinventing it, and you don’t depend on it for the main path. No breaking change to existing notebooks.

---

## Concrete next steps (prioritized)

1. **Document current trade-offs** (in preamble or README): State that the evaluation set is “perfect” (identical pairs, same zip) so that blocking and cascading don’t show recall loss; add one sentence that the main ER set includes more match types (e.g. address+zip) for self-directed experimentation.
2. **Improve fuzzy pedagogy:** Extend `generate_test_data.py` with a small “evaluation-with-name-variation” set (e.g. 20–30 pairs where right table has typos/variants like “Jon”/“John”, “St.”/“Street”). Use it in 04 (and optionally 06) so that “exact misses, fuzzy catches” is visible.
3. **Optional blocking lesson:** Add a tiny “evaluation-with-split-zip” variant where some true pairs have different zip codes, and use it in 05 so that blocking-on-zip can reduce recall and the choice of key is teachable.
4. **Optional FEBRL integration:** One optional notebook or subsection that loads FEBRL4 (and maybe FEBRL1), converts to Polars + standard column names, and runs the same measurement loop; link to Leipzig/Zenodo for “further benchmarks.”

If you want to minimize change: do (1) only. For “covering the bases” on fuzzy and blocking: add (2) and (3) with small generated variants. For “ecosystem without reinventing”: add (4) as optional.

---

## Summary

| Question | Answer |
|----------|--------|
| Are the datasets suitable? | Yes for stability and flow; **no** for teaching blocking trade-offs and fuzzy’s value (data is too “perfect”). |
| Do we need more variety? | **Yes:** at least name variation for fuzzy, and optionally split blocking keys for blocking. |
| Use existing datasets? | **Optional:** FEBRL via recordlinkage is a solid, reusable choice for an optional “standard benchmark” step; keep current data as the default path. |
| Breaking changes? | **None** required; improvements can be additive (new generated subsets + optional FEBRL). |
