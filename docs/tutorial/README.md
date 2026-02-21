# matcher Tutorial (Notebooks)

A hands-on path from "I have two tables" (or one for dedup) to a matching pipeline you can tune and trust. Work through the notebooks in order; they use sample data from `data/ExactMatcher/` so runs are fast and outputs are reproducible. The focus is on designing, measuring, and comparing—not just running steps.

**Start here:** [00_preamble_and_toc.ipynb](00_preamble_and_toc.ipynb) — preamble, table of contents, and links to all notebooks.

## Prerequisites

- **Tutorial env (recommended):** Jupyter is not in the main dependency path. Create a dedicated venv and sync the `tutorial` group from the **repository root**:
  ```bash
  uv venv .venv-tutorial
  source .venv-tutorial/bin/activate   # Windows: .venv-tutorial\Scripts\activate
  uv sync --group tutorial
  ```
- Run notebooks from the **repository root** (or from `docs/tutorial`). Data is **loaded on the fly** by the `tutorial_data` package—no need to generate or commit data files.

## Sample data (on the fly)

The `tutorial_data` package under `docs/tutorial/tutorial_data/` generates all sample data in memory. Each notebook adds `docs/tutorial` to `sys.path` and imports e.g. `load_evaluation`, `load_fuzzy_evaluation`, then calls the loader to get `(left, right, ground_truth)` or `(df, ground_truth)`. No `data/` directory is required.

Optional: from repo root run `uv run python scripts/generate_test_data.py` to write the same data to `data/` for inspection or for tests that read from disk.

## Notebooks

| Notebook | Content |
|----------|---------|
| [00_preamble_and_toc](00_preamble_and_toc.ipynb) | Preamble, table of contents, sanity check |
| [01_preparation](01_preparation.ipynb) | Schema, nulls, standardization, feature engineering, ground truth |
| [02_exact_matching](02_exact_matching.ipynb) | Exact matching: single rule, cascading rules, evaluate API |
| [03_measurement_loop](03_measurement_loop.ipynb) | The measurement loop: measure → change one thing → compare |
| [04_fuzzy_matching](04_fuzzy_matching.ipynb) | Fuzzy matching: one field, threshold, confidence (uses fuzzy_evaluation) |
| [05_blocking](05_blocking.ipynb) | Blocking: when it helps and when it hurts recall (uses blocking_evaluation) |
| [06_design_algorithm](06_design_algorithm.ipynb) | Exact+fuzzy combo, blended score |
| [07_deduplication](07_deduplication.ipynb) | Deduplication on a single table |

## Related docs

- [BEFORE_YOU_BEGIN.md](../BEFORE_YOU_BEGIN.md) — data prep reference
- [MATCHING_ALGORITHM_DESIGN.md](../MATCHING_ALGORITHM_DESIGN.md) — design and evaluation reference
- [FUZZY_MATCHING_AND_COMBOS.md](../FUZZY_MATCHING_AND_COMBOS.md) — fuzzy and blended reference
