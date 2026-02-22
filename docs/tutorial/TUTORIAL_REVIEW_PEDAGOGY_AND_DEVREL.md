# Tutorial review: pedagogy and developer relations

**Scope:** Consistency, pedagogical flow, and dev-rel quality of `docs/tutorial/` (notebooks 00–07, README, data loaders).  
**Lens:** Teaching effectiveness, narrative clarity, and first-run experience.

---

## Executive summary

The tutorial has a **strong core**: a single narrative (prep → exact → measurement loop → fuzzy → design → blocking → dedup), a clear habit (measure → change one thing → compare), and small, fast runs. A few inconsistencies and one factual typo weaken the experience; fixing them is low effort and high impact.

**Fixed in this pass:** 03 — prose said "25% recall" for Run A; table shows 33.33%. Corrected to 33.33%.

**Later pass (recommendations implemented):** 00 prerequisites and dataset table aligned; 02 added evaluate() column-name explanation; 05 added blended-cutoff tuning sentence; 01 Section 4/5 full_name boundary cleaned; path block format standardized across 04–07.

---

## What works well

### Pedagogy

- **Single story:** One path from "I have two tables" to a tunable pipeline. No branching "choose your adventure" that loses learners.
- **Habit over mechanics:** The measurement loop is introduced early (02/03) and reused everywhere (fuzzy thresholds, blocking, blend). Learners get a repeatable decision process, not just API steps.
- **Concepts before features:** Preparation (01) establishes schema, nulls, cleaning, and ground truth before any matching. Exact (02) before fuzzy (04). Blocking (06) is clearly optional.
- **Data matches the lesson:** Entity resolution data has exact-name, email-adds-value, and fuzzy-name pairs; blocking_evaluation has split-zip pairs so blocking-on-zip visibly hurts recall. The generator docstring in `_generate.py` explains the design.

### Dev rel

- **Tone:** Direct, concise, no fluff. "No magic—just measure, tune, compare" is memorable and accurate.
- **Navigation:** Each notebook has Back/Next (and 07 points to TOC). Preamble has a clear table of contents.
- **Run from anywhere:** Path logic supports repo root or `docs/tutorial`; sanity check in 00 confirms environment.
- **No hidden steps:** Data is generated on the fly; README states no data files needed. 02 explicitly does raw → standardize → match so nothing is implicit.

---

## Consistency issues

### 1. Prerequisites: preamble vs README

| Location | Instruction |
|--------|-------------|
| **00 preamble** | "From the repository root: `uv sync` (or `pip install -e .`) so matcher is installed." |
| **README** | "Jupyter is not in the main dependency path. Create a dedicated venv … `uv sync --group tutorial`" |

**Issue:** Someone following only the preamble may run `uv sync` from root and have matcher but no Jupyter; they then open a notebook and get a kernel/import failure. README is correct (tutorial group includes Jupyter).

**Recommendation:** In 00 Prerequisites, add one sentence: "To run notebooks, use a venv with the tutorial deps: from repo root, `uv venv .venv-tutorial && source .venv-tutorial/bin/activate && uv sync --group tutorial`. See [README](README.md) for details."

### 2. Data loaders: preamble table vs actual notebook usage

| Loader | Preamble says | Actually used in |
|--------|----------------|------------------|
| `load_evaluation()` | "50×50, 30 perfect pairs (optional / alternate for smaller demos)" | Not used in any notebook |
| `load_fuzzy_evaluation()` | "50×50, 30 pairs (15 identical, 15 name variants) **for 04**" | Not used in 04; 04 uses `load_entity_resolution` + `standardize_for_matching` |
| `load_blocking_evaluation()` | "50×50, 30 pairs (15 same zip, 15 split zip) **for 06**" | Used in 06 ✓ |

**Issue:** 04 uses the 500×500 entity resolution data (which already includes 10 fuzzy-name pairs). The preamble implies 04 uses the smaller fuzzy_evaluation set. Either 04 could optionally mention "alternatively, use `load_fuzzy_evaluation()` for a smaller 50×50 run" or the preamble table should say "for 04 we use entity resolution data (which includes name variants); `load_fuzzy_evaluation()` is an optional smaller alternate."

**Recommendation:** Update the preamble "Sample Datasets Used" table so the "Used in" column matches reality: 02–05 use entity resolution (standardized); 06 uses blocking_evaluation; 07 uses deduplication. Mark load_evaluation and load_fuzzy_evaluation as "optional / alternate" without tying them to a specific notebook number, or add one line: "04 uses ER data (has name variants); load_fuzzy_evaluation is an optional smaller set."

### 3. Repetition of path/setup block

Every notebook (01–07) repeats the same `_tutorial` path detection and `sys.path.insert` + imports. Minor formatting differences (e.g. 02/03 use multi-line `_tutorial` assignment; 04/05/06/07 use one-liner). Not wrong, but a bit noisy.

**Recommendation:** Low priority. Optionally add a one-line helper in `tutorial_data` (e.g. `ensure_tutorial_path()`) that notebooks call so the block is one line; only if you want less copy-paste and a single place to change path logic.

---

## Pedagogy: small improvements

### 1. Why evaluate() uses left_id_col / right_id_col

Notebooks consistently pass `left_id_col="id", right_id_col="id_right"`. 04 briefly says "Ground truth keeps left_id/right_id (evaluator requires those names); predicted matches use id/id_right." That’s the only explanation. A single sentence in 02 or 03 when `evaluate()` is first used would help: e.g. "Ground truth has columns `left_id` and `right_id`; the match table has `id` and `id_right`. We pass these so the evaluator can join them."

### 2. 03 Run C: "25% recall" typo

**Fixed:** Prose said "A finds only the pairs with identical names (25% recall)"; the table shows 33.33%. Updated to 33.33% so the text matches the table.

### 3. 01: Section 4 (full_name) vs Section 5 (feature engineering)

Section 4 is "Value-level standardization (cleaning)" and adds `email_clean`. Section 5 is "Feature engineering for matching and blocking" and adds `full_name`. There’s a small duplicate: in Section 4, Step 2 in the narrative says "Add a single **full_name** column" but the code that actually adds `full_name` is in Section 5. Cell 18 (Section 4) shows "messy" rows but doesn’t add full_name; cell 20 (Section 5) adds it. Consider moving the "we'll add full_name for fuzzy" mention to Section 5 only, or adding full_name in Section 4 and having Section 5 focus on "other derived columns (e.g. blocking key)." Minor—flow is still understandable.

### 4. 05: Blended example outcome

05 shows "Exact only: 33.33% recall; Exact+fuzzy: 96.67% recall" and "Blended (max score, cutoff 0.85): 9.39% precision, 96.67% recall." The blended run has very low precision; the narrative doesn’t say "this cutoff is loose; tune it with the measurement loop." One sentence would help: "Here we used 0.85 as a single cutoff; in practice you’d sweep cutoffs and compare precision/recall as in 03."

---

## Dev rel: small improvements

### 1. First cell in each notebook

Notebooks 01–07 start with a markdown cell: title, one short paragraph, and "Back: … · Next: …". That’s good. 07 ends with "End of tutorial" and links to README and design docs—also good. No change needed; just noting the pattern is consistent.

### 2. README "Notebooks" table

README table matches the preamble TOC and the actual order (00–07). The only nuance: README says "05 Design: exact+fuzzy combo, … 06 Blocking: when it helps and when it hurts recall." 06 correctly uses blocking_evaluation (split zip), so "when it hurts recall" is actually shown. Good.

### 3. Optional: time estimates

For dev rel, optional "~5 min" or "~10 min" per notebook could help; not required for consistency.

---

## Relation to DX_REVIEW_DATASETS_AND_VARIETY.md

That review focused on **dataset design** (evaluation set too "perfect," blocking/fuzzy pedagogy). The current state:

- **Blocking:** 06 uses `load_blocking_evaluation()` (15 same zip, 15 split zip), so "blocking can hurt recall" is now taught. No change needed for this review.
- **Fuzzy:** 04 uses entity resolution data, which includes 10 fuzzy-name pairs (Jonathan/Jon, etc.) in `_generate.py`. So "exact misses, fuzzy recovers" is already in the narrative. The DX review’s concern about "no name variation" applied to an older or alternate evaluation set; the main path (04) is aligned.
- **Multi-field (address+zip):** DX review suggested either showing multi-field rules once or documenting that the ER set’s address+zip design is for self-directed experimentation. This pedagogy review doesn’t require that; if you do it, a short note in 01 or the preamble is enough.

---

## Summary: what to do

| Priority | Action |
|----------|--------|
| **Done** | Fix 03: "25% recall" → "33.33% recall" so prose matches table. |
| **Done** | Align prerequisites: in 00, point to README and mention `uv sync --group tutorial` (or equivalent) for running notebooks. |
| **Done** | Align preamble "Sample Datasets Used" with actual usage: which notebooks use which loader; clarify that 04 uses ER data (with name variants), not load_fuzzy_evaluation. |
| **Done** | 02: one sentence explaining why we pass left_id_col/right_id_col (ground truth vs match column names). |
| **Done** | 05: one sentence that the blended cutoff 0.85 is illustrative and should be tuned via the measurement loop. |
| **Done** | 01: cleanup Section 4/5 boundary—full_name introduced only in Section 5; Section 4 transition points to Section 5. |
| **Done** | Path block: standardized to the same multi-line format in all notebooks (04–07 now match 01–03). |

Overall the tutorial is in good shape: clear narrative, strong habit, and data that supports the lessons. The fixes above are about consistency and one typo, not a redesign.
