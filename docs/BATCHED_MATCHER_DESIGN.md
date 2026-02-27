# BatchedMatcher: Design Document

## Purpose

Enable entity resolution when left and/or right data do not fit in memory. The existing `Matcher` holds full `left` and `right` DataFrames; `BatchedMatcher` accepts **chunked** sources (iterators of DataFrames, or LazyFrames that are collected in batches), runs the same matching algorithms on each (left_batch, right_batch) pair, and returns a single `MatchResults` with combined matches. Refine (cascade) is supported by re-streaming and filtering to unmatched left IDs.

**Scope**: Bounded memory for inputs and for refine; same semantics as eager Matcher. No new algorithms—reuse `MatchingAlgorithm` (ExactMatcher, FuzzyMatcher, etc.).

---

## Goals

- **Memory**: Never hold full left or full right in memory; only one left batch and one right batch at a time (plus accumulated match pairs/results as needed).
- **API**: Clear, batched-first entry point; encourage LazyFrame/iterators; same `match_on=`, `block_on=`, `matching_algorithm` as Matcher.
- **Semantics**: Same as Matcher: one rule per `match()`; cascade via `refine(match_on=...)` (second rule on unmatched left only; union of pairs).
- **Compatibility**: Same `MatchResults` surface (count, pipe, evaluate, export_for_review); refine works via re-stream + filter by `matched_left_ids`.

---

## Non-goals (for now)

- True lazy evaluation (we materialize batches).
- Changing `MatchingAlgorithm` interface or adding BatchedMatchingAlgorithm.
- **BatchedDeduplicator**: For deduplication at scale, use BatchedMatcher with the same source for both sides: e.g. `BatchedMatcher(batches(source), batches(source), left_id="id", right_id="id")` or pass the same LazyFrame twice. Filter self-matches from the result (e.g. `results.matches.filter(pl.col("id") != pl.col("id_right"))`). A dedicated BatchedDeduplicator can be added later if needed.

---

## API

### 1. Batch source

A batch source is one of:

- **Iterator[DataFrame]**: User provides an iterator that yields Polars DataFrames (e.g. from a DB cursor, or from a custom generator).
- **LazyFrame**: We chunk by row range (see `batches()` below) and collect each chunk; user can pass `pl.scan_parquet(...)` etc.

We do **not** accept a single large DataFrame and slice it internally for the initial API (user can slice into an iterator themselves). We may add a convenience later.

**Iterator re-use**: When you pass one-shot iterators (e.g. `iter([df1, df2])`), `match()` consumes them. A second call to `match()` on the same BatchedMatcher would see exhausted iterators and return empty or partial results. For multiple passes (e.g. `refine()`), use LazyFrame sources so we can re-stream, or pass a fresh iterator for each `match()` if you only run once.

**Preprocessing before the double loop**

If you need to normalize or transform data before matching (e.g. lowercase emails, derive blocking keys, fill nulls), do it **before** passing sources to BatchedMatcher—there is no built-in preprocess hook.

- **LazyFrame**: Build preprocessing into the plan. Each chunk collected by `batches(lf, batch_size)` will already be transformed.
  ```python
  left_lf = (
      pl.scan_parquet("left.parquet")
      .with_columns(pl.col("email").str.to_lowercase().str.strip_chars())
      .with_columns(pl.col("zip").cast(pl.Utf8).alias("zip_code"))  # blocking_key
  )
  batcher = BatchedMatcher(left_lf, right_lf, left_id="id", right_id="id")
  results = batcher.match(match_on="email", block_on="zip_code")
  ```
- **Iterator**: Wrap the iterator so each yielded batch is preprocessed before BatchedMatcher sees it.
  ```python
  def left_batches():
      for batch in batches(pl.scan_parquet("left.parquet"), batch_size=50_000):
          yield batch.with_columns(pl.col("email").str.to_lowercase())
  batcher = BatchedMatcher(left_batches(), right_batches(), left_id="id", right_id="id")
  ```

If you need global preprocessing (e.g. a statistic computed over the full dataset), do a first pass in the LazyFrame or a separate streaming pass, then run BatchedMatcher on the transformed source. We do not add a `preprocess=` callback unless a concrete use case requires it.

### 2. `batches(source, batch_size=50_000)`

Helper to turn a LazyFrame or DataFrame into an iterator of DataFrames:

- **LazyFrame**: Add a row index (or use `slice` by range), collect in chunks of `batch_size` rows, yield each chunk. Default `batch_size=50_000`.
- **DataFrame**: Yield `source.slice(i, batch_size)` for i = 0, batch_size, 2*batch_size, ... (convenience for testing; user already has data in memory).

This encourages LazyFrame usage in docs and examples.

### 3. BatchedMatcher

**Constructor**

```python
BatchedMatcher(
    left_batches: Iterator[DataFrame] | LazyFrame,
    right_batches: Iterator[DataFrame] | LazyFrame,
    left_id: str,
    right_id: str,
    *,
    matching_algorithm: MatchingAlgorithm | None = None,
    batch_size: int = 50_000,  # used when left/right are LazyFrame
)
```

- If `left_batches` or `right_batches` is a LazyFrame, we use `batches(lf, batch_size)` internally. Otherwise we assume it's an iterator and consume it.
- We do not validate schema until first batch is pulled (or we require first batch up front for validation). Design: on first `match()` we start iterating and validate first left and first right batch have `left_id`/`right_id`.

**Method: match(match_on=..., block_on=..., matching_algorithm=...)**

- Normalize `on` to a single rule (same as Matcher); validate fields on first batches.
- Double loop: for each left_batch, for each right_batch, run `algo.match(left_batch, right_batch, rule, left_id, right_id)`. If `block_on` is set, we compute blocks **within** each (left_batch, right_batch) pair (so blocking is per batch, not global). Combine all batch results: concatenate match DataFrames, then deduplicate on (left_id, right_id_right) and rejoin to get one combined DataFrame (we need to rejoin with left/right to get full rows—see below).
- **Rejoin for full rows**: We only have batch-sized left/right at a time. To produce a `matches` DataFrame with the same schema as Matcher (full left + full right columns), we either:
  - **Option A**: Store only (left_id, right_id_right) + provenance from each batch; then do one more pass over (left_batches, right_batches) to materialize full rows for those pairs (filter pairs to pairs in current batch, join with left_batch and right_batch). That second pass is the "re-stream to build result" step.
  - **Option B**: As we run the double loop, for each batch we get a matches DataFrame with full rows for that batch; we concat all of them and then `.unique(subset=[left_id, right_id_right])` to dedupe. That gives full rows without a second pass, but we might accumulate a large `matches` DataFrame if there are many matches. We accept that the **result** can be large; we only bound **input** memory.

Design choice: **Option B** for simplicity—concat batch results and dedupe. We document that if the number of matches is huge, result memory can grow; user can pipe to export or sample. Option A can be a later optimization.

- Return `MatchResults(matches=combined, original_left=None, source=batched_matcher)`.

**Stored for refine**

- We store a reference to the BatchedMatcher (so we can re-stream). We do **not** store `original_left`. We store `matched_left_ids` from the first match (a DataFrame or set of left_id values) so refine can filter "unmatched" on the fly.

### 4. Refine (batched)

When the user calls `results.refine(match_on=...)` and `results._source` is a BatchedMatcher:

- **matched_left_ids**: We have this from the first match (stored on BatchedMatcher or on a thin wrapper in MatchResults).
- **Unmatched left**: Re-stream left_batches; for each left_batch, filter to rows where `left_id` is not in `matched_left_ids`. Yield unmatched_left_batch.
- **Second rule**: For each (unmatched_left_batch, right_batch) run `algo.match(...)` with the new rule. Collect all new pairs. Union with first-pass pairs: `combined_pairs = first_pairs ∪ new_pairs`.
- **Full result**: We now have combined_pairs (left_id, right_id_right). To build the same full-row schema as eager refine, we re-stream (left_batches, right_batches) once: for each (left_batch, right_batch), filter combined_pairs to pairs whose left_id is in left_batch and right_id is in right_batch, join with left_batch and right_batch to get full rows, concat. Dedupe on (left_id, right_id_right). Return `MatchResults(combined, original_left=None, source=batched_matcher)` and update stored matched_left_ids to the union of left_ids in combined_pairs (so further refine() can run).

**Provenance**: Same as eager: add score/on columns from the algorithm; merge with existing score/on when combining first + refined (we have the same provenance columns in batch results).

### 5. MatchResults from BatchedMatcher

- `matches`: Same schema as eager (full left + right columns, left_id, right_id_right, provenance).
- `_original_left`: None (we don't have it).
- `_source`: BatchedMatcher instance. When `refine()` is called, we dispatch to BatchedMatcher.refine(results, match_on=..., block_on=...) which does the re-stream logic above.

So `MatchResults.refine()` in results.py will need to detect `source is BatchedMatcher` and call `source.refine(self, match_on=..., block_on=...)` instead of the current eager path. That keeps the API one place (`results.refine(match_on=...)`) and the batched implementation in BatchedMatcher.

---

## Implementation summary (done)

1. **matcher/batched.py**
   - `batches(source: DataFrame | LazyFrame, batch_size: int = 50_000) -> Iterator[DataFrame]`: DataFrame sliced in memory; LazyFrame chunked via `with_row_index` + filter range + collect per chunk.
   - `BatchedMatcher(left_batches, right_batches, left_id, right_id, *, matching_algorithm=None, batch_size=50_000)`:
     - Accepts `Iterator[DataFrame]` or `LazyFrame` for each side; LazyFrame is turned into an iterator via `batches()` so it can be re-iterated for refine.
     - `match(match_on=..., block_on=..., matching_algorithm=...)`: double loop over left then right batches; blocking applied per (left_batch, right_batch); concat + dedupe on (left_id, right_id_right); add provenance; return `MatchResults(..., source=self)`.
     - `refine(results, match_on=..., block_on=..., matching_algorithm=...)`: requires LazyFrame sources (re-iterable). Re-stream left, anti-join with matched_left_ids; double loop for new rule; union pairs; re-stream to materialize full rows for combined_pairs (with left id preserved after join); merge provenance; return new MatchResults.
   - One-shot iterators: refine() raises if sources are not re-iterable (LazyFrame).

2. **matcher/results.py**
   - At start of `refine()`, if `isinstance(source, BatchedMatcher)` then `return source.refine(self, match_on=match_on, block_on=block_on)`; else run eager path.

3. **matcher/__init__.py**
   - Exports `BatchedMatcher`, `batches`.

4. **tests/test_batched.py**
   - batches(DataFrame), batches(LazyFrame), batch_size validation; BatchedMatcher single/multiple batches (LazyFrame for full double loop); refine requires LazyFrame; refine LazyFrame; empty batches.

---

## Edge cases

- **Empty batches**: Skip (left_batch or right_batch height 0); no match call.
- **Empty result**: Same as Matcher—return MatchResults with empty matches and same schema (from algo.match(empty, empty, ...)).
- **Blocking**: Applied per (left_batch, right_batch). So a block can span batches (same block key in different batches); we do not merge blocks across batches. Document that blocking reduces work within each batch.
- **ID columns**: Required in every batch; validate on first batch (or on first pull).
- **LazyFrame chunking**: Polars LazyFrame doesn't have a built-in "collect N rows at a time". We use row_index or range filters: e.g. collect with row_index, then filter row_index.is_between(lo, hi), collect; or slice. See implementation for exact approach (some LazyFrames may not support row_index; fallback to collect and slice in memory if needed).

---

## Documentation

- Add "Working with large data" to docs: use `pl.scan_parquet` + `batches()` + BatchedMatcher; default batch_size 50_000; refine supported via re-stream.
- Docstring for BatchedMatcher: "Use when left and/or right do not fit in memory. Pass iterators of DataFrames or LazyFrames; we process in batches and return MatchResults. Refine is supported by re-streaming and filtering to unmatched left IDs."

---

## Future

- Optional: yield or stream results per (left_batch, right_batch) to avoid holding full result in memory (e.g. write to Parquet/DB).
- BatchedDeduplicator if needed (same pattern; self-matches filtered).
- LazyFrame chunking improvements (e.g. use Polars streaming collect in chunks when available).
