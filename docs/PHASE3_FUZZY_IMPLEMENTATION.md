# Phase 3: Fuzzy Matching — Implementation Plan

This document captures the implementation approach for Phase 3 (fuzzy matching), including **vectorization** and the **Arrow bridge** for the Polars–NumPy boundary. It extends the scope in [ROADMAP.md](../ROADMAP.md#phase-3-fuzzy-matching) with concrete technical decisions.

---

## Goals (from ROADMAP)

- Handle typos and variations ("John Smith" vs "J. Smith").
- Single algorithm (Jaro-Winkler), single threshold, `rapidfuzz` dependency.
- API: `matcher.match_fuzzy(field="name", threshold=0.85)`.
- Performance: acceptable for 10K records; combinable with blocking later.

---

## 1. Vectorization — No Row-by-Row Loops

**Decision:** Use **batch, vectorized** similarity computation. Do **not** implement fuzzy matching with row-by-row Python loops over pairs.

**Mechanism:** Use **`rapidfuzz.process.cdist`**:

- **Input:** Two collections of strings (left column, right column).
- **Output:** One similarity matrix (NumPy array) for all pairs in a single call.
- **Execution:** Implemented in C, with **`workers=-1`** for multi-core use.

**Rationale:** Row-by-row Python loops over 10K×10K pairs are slow and don’t match the rest of the stack (Polars/columnar). `cdist` gives one batch call, C-level work, and parallel execution. The archive sketch in `docs/archive/MATCHING_PLAN_V2.md` that used nested Python loops is **superseded** by this approach.

**Usage shape:**

```python
from rapidfuzz import process, fuzz

# queries = left column (e.g. from NumPy/Arrow); choices = right column
matrix = process.cdist(
    queries, choices,
    scorer=fuzz.JaroWinkler.similarity,
    workers=-1,
    score_cutoff=threshold_0_to_100,
)
# matrix is ndarray of shape (len(queries), len(choices))
```

Then: from the matrix, derive `(left_idx, right_idx, score)` pairs (see §3).

---

## 2. Arrow as the Polars–NumPy Bridge

**Decision:** Use **Arrow** as the standard interchange step when moving data from Polars to NumPy (and back where needed). Do not treat Polars and NumPy as two unrelated worlds.

**Data path:**

1. **Polars** holds all tabular data: left/right DataFrames, IDs, normalized string column(s).
2. **Polars → Arrow:** Export the string column(s) needed for similarity via `df.to_arrow()` (or equivalent). Polars is Arrow-format internally, so this can be zero-copy where possible.
3. **Arrow → NumPy:** Convert the Arrow column(s) to the form rapidfuzz expects (e.g. via PyArrow’s `.to_numpy()` or buffer interface). This is the only copy at the boundary; it is explicit and consistent.
4. **rapidfuzz:** Consumes the NumPy/list of strings and returns the similarity matrix (NumPy).
5. **Matrix → Polars:** Turn the matrix into match pairs (left_id, right_id, confidence) and build a Polars DataFrame for the result.

**Rationale:** Data is already in Arrow format in Polars. Using Arrow as the bridge (Polars → Arrow → NumPy) keeps the contract clear, avoids ad-hoc `.to_list()`/`.to_numpy()` without a defined interchange, and aligns with ecosystem practice. Polars remains the single source of truth for tables; NumPy is used only at the rapidfuzz interface (input strings and output matrix).

**Note on PyArrow:** Polars uses its own Arrow implementation (Rust); it does not use PyArrow as its engine. PyArrow is used as the **interop layer** when we call `to_arrow()` and then convert Arrow → NumPy in Python.

---

## 3. End-to-End Data Flow

| Step | Where | What |
|------|--------|------|
| 1. Normalize | Polars | Columnar normalization (e.g. lowercase, strip) on the match field(s). |
| 2. Export for similarity | Polars → Arrow → NumPy | Extract normalized string column(s) via Arrow into the form needed for `cdist` (e.g. two 1D arrays). |
| 3. Similarity matrix | rapidfuzz | Single `process.cdist(left_col, right_col, scorer=..., workers=-1)` call. |
| 4. Matrix → pairs | NumPy / Polars | Find `(i, j)` where `matrix[i, j] >= threshold`. Map indices back to `left_id`, `right_id` using the original Polars DataFrames; attach `confidence = matrix[i, j]`. |
| 5. Result | Polars | Build `MatchResults` (or equivalent) from a Polars DataFrame of (left_id, right_id, confidence). |

**Polars is the table layer throughout.** NumPy is used only for: (a) the string vectors passed into `cdist`, and (b) the similarity matrix and index-finding step. Final matches and IDs live in Polars.

---

## 4. Memory and Scaling

- **Full matrix:** `cdist` returns a dense matrix of shape `(len(left), len(right))`. For 10K×10K with float32, that is on the order of hundreds of MB, which is acceptable for the target scale.
- **Larger data:** When combining with blocking (Phase 2), run fuzzy only **within blocks**. Then `cdist` is called per block with smaller vectors, so matrix size stays bounded. Chunking left (or right) and processing chunk-by-chunk is another option if needed.

---

## 5. Dependencies and References

- **rapidfuzz:** Required; provides `process.cdist` and `fuzz.JaroWinkler.similarity`.
- **Polars:** Already in use; remains the primary data container and table API.
- **Arrow (PyArrow):** Used for the Polars → NumPy bridge when exporting/importing at the rapidfuzz boundary.

**References:**

- [ROADMAP.md](../ROADMAP.md) — Phase 3 scope, API, success criteria.
- [rapidfuzz process.cdist](https://rapidfuzz.github.io/RapidFuzz/Usage/process.html) — batch similarity API.
- Polars Arrow interop: `to_arrow()`, `from_arrow()` and related docs.

---

## 6. Summary

- **Vectorization:** Use `rapidfuzz.process.cdist` for all-pairs similarity in one batch call; no row-by-row Python loops.
- **Arrow:** Use Arrow as the bridge from Polars to NumPy at the rapidfuzz boundary (Polars → Arrow → NumPy); keep Polars as the table layer and NumPy only at the interface.
- **Implementation:** Normalize in Polars → export via Arrow to NumPy → cdist → matrix to (left_id, right_id, confidence) in Polars. Design for optional blocking/chunking when scaling.
