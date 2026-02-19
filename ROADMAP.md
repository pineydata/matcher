# hygge-match Roadmap

**Last Updated:** 2026
**Current Phase:** Phase 1, Phase 3 & Phase 4 Complete ✅
**Next Phase:** Phase 2 Blocking or Phase 4 load-back as needed

---

## Executive Summary

**Current State:** Phase 1 (Exact Matching) and Phase 3 (Fuzzy Matching) are complete and production-ready. The codebase is clean, well-tested, and ready for continued development.

**Strategic Direction:**
- **Immediate (Next)**: Complete Phase 4—document user evaluation workflow (get GT → match → evaluate → tune → re-run), use evaluate() in sample-data tests
- **Short-term (Next 1-3 months)**: Phase 2 (Blocking) or Phase 4 load-back based on user needs
- **Medium-term (3-6 months)**: Complete remaining phases, add advanced features as needed
- **Long-term (6+ months)**: Human-in-the-loop workflows, Power BI integration

**Decision Framework:**
- Add features only when there's a real, current use case (YAGNI)
- Measure before committing (data-driven decisions)
- Keep it simple (KISS)
- Optimize for programmer happiness (hygge philosophy)

---

## Phase 1: Exact Matching ✅ COMPLETE

**Status:** Complete and production-ready

**What Was Built:**
- ✅ Entity resolution (cross-source matching)
- ✅ Deduplication (single-source matching)
- ✅ Multi-field matching (AND logic within rules)
- ✅ Multiple rules (OR logic between rules)
- ✅ Component-based architecture (MatchingAlgorithm, Evaluator)
- ✅ Evaluation capabilities (precision, recall, F1)
- ✅ Clean module structure (refactored from core.py)

**Success Criteria:**
- ✅ Finds all known matches (100% recall on test data)
- ✅ Zero false positives (100% precision on test data)
- ✅ Runs in <30 seconds for test dataset
- ✅ Works in notebooks
- ✅ Clear error messages
- ✅ Auditable results

**What's Next:**
- Technical debt (max_workers, null handling docs) addressed ✅
- Review roadmap with stakeholders; plan for Phase 2 or Phase 3 based on user needs

---

## Technical Debt & Immediate Improvements

### Priority: Medium

#### 1. Remove `max_workers` Parameter ✅ Done

**Issue:** Parameter exists but is not used. Polars handles parallelization internally.

**Impact:** Creates confusion and false expectations.

**Solution:**
- Remove `max_workers` from `ExactMatcher.__init__()`
- Remove `max_workers` from `Matcher.__init__()`
- Remove `max_workers` from `Deduplicator.__init__()`
- Update docstrings to note that Polars handles parallelization internally

**Effort:** Low (1-2 hours)
**Risk:** Low (parameter is not used, removal is safe)

**Rationale:** YAGNI - Don't add features until needed. When custom algorithms need parallelization control, they can add it.

---

### Priority: Low

#### 2. Document Null Handling ✅ Done

**Issue:** Polars inner joins exclude null values by default, but this behavior is not documented.

**Impact:** Users might not understand why null-to-null records don't match.

**Solution:**
- Add note about null handling in `ExactMatcher.match()` docstring
- Add note in README about null handling behavior
- Consider adding example showing null handling

**Effort:** Low (1 hour)
**Risk:** None (documentation only)

**Rationale:** Documentation helps users understand behavior. Polars default behavior is reasonable.

---

#### 3. Add Performance Benchmarks (Future)

**Issue:** No explicit performance benchmarks or scalability testing.

**Impact:** Can't quantify performance characteristics or justify Phase 2 (blocking).

**Solution:**
- Create benchmark suite with different data sizes (1K, 10K, 100K, 1M rows)
- Measure performance for entity resolution and deduplication
- Document results in README or separate performance guide

**Effort:** Medium (4-8 hours)
**Risk:** None (additive only)

**Rationale:** Would help justify Phase 2 (blocking) implementation and help users understand performance characteristics.

**When:** Add when planning Phase 2 (blocking)

---

## Phase 2: Blocking (Performance Optimization) ✅ COMPLETE

**Status:** Complete

**Goal:** Handle larger datasets efficiently by reducing comparisons.

**What Was Built:**
- ✅ Single blocking key on `Matcher.match(rules=..., blocking_key="zip_code")` and `Deduplicator.match(rules=..., blocking_key=...)`
- ✅ Blocking on `Matcher.match_fuzzy(..., blocking_key=...)` and `Deduplicator.match_fuzzy(..., blocking_key=...)` (fuzzy runs per block to bound matrix size)
- ✅ Generate candidate pairs within blocks only (common block values only)
- ✅ Nulls in blocking_key form one block (matched within null block)

**API:**
```python
# Blocking for performance - simple, one key
results = matcher.match(rules="email", blocking_key="zip_code")
results = matcher.match_fuzzy(field="name", threshold=0.85, blocking_key="zip_code")
results = deduplicator.match(rules="email", blocking_key="zip_code")
results = deduplicator.match_fuzzy(field="name", blocking_key="zip_code")
```

**Success Criteria:**
- [x] Optional blocking_key on match and match_fuzzy (Matcher and Deduplicator)
- [x] Same matches as without blocking when blocks align with match keys
- [ ] Can process 1M records in <30 minutes (add benchmarks when needed)
- [ ] Blocking reduces comparisons by >90% (measure with benchmarks)

**YAGNI (unchanged):**
- No automatic blocking suggestions (users know their data)
- No multiple blocking keys (add later if users request)
- No auto-blocking (add later if users request)

---

## Phase 3: Fuzzy Matching ✅ COMPLETE

**Status:** Complete and production-ready

**Goal:** Handle typos and variations ("John Smith" vs "J. Smith").

**What Was Built:**
- ✅ `Matcher.match_fuzzy(field=..., threshold=0.85)` and `Deduplicator.match_fuzzy()`
- ✅ Single algorithm (Jaro-Winkler via rapidfuzz), single threshold
- ✅ Vectorized pipeline: Polars → Arrow → rapidfuzz `cdist` (no row loops); multi-core
- ✅ `rapidfuzz`, `pyarrow`, `numpy` dependencies; simple normalization (lowercase, trim)
- ✅ String (Utf8) validation; nulls excluded (same semantics as exact match)
- ✅ MatchResults with `confidence` column; works with `evaluate()` and `refine()`

**API:**
```python
# Single-field fuzzy matching - simple
results = matcher.match_fuzzy(
    field="name",
    threshold=0.85  # That's it
)
```

**Success Criteria:**
- ✅ Can match "John Smith" to "J. Smith" with confidence >0.8
- ✅ Can match addresses with typos
- ✅ False positive rate acceptable (threshold controls precision/recall tradeoff)
- ✅ Performance acceptable (vectorized; <5 minutes for 10K records target)
- ✅ Confidence scores auditable (0–1 in results)
- ✅ Designed to combine with blocking later (run fuzzy within blocks)

**Implementation:** See [docs/PHASE3_FUZZY_IMPLEMENTATION.md](docs/PHASE3_FUZZY_IMPLEMENTATION.md) for vectorization (rapidfuzz `cdist`), Arrow bridge (Polars → Arrow → NumPy), and data flow.

---

## Phase 4: Human in the Loop & User Evaluation Workflow

**Status:** Complete ✅

**Goal:** (1) Enable human review by exporting matches in an informative, focused format (CSV, optional sample). (2) Make evaluate() the standard way for the user to improve: get ground truth → match → evaluate → tune → re-run → compare until good enough.

**When to Build:**
- ✅ Phase 1 and Phase 3 work
- ✅ Need human review and/or measurable iteration on quality

**What Was Built (export for review):**
- ✅ Export matches to CSV (human-friendly; opens in Excel, etc.)
- ✅ `sample(n=...)` / `sample(fraction=...)` for manageable review samples
- ✅ Focused export via `pipe`/select before export

**API (export):**
```python
results = matcher.match_fuzzy(field="name", threshold=0.85)
results.export_for_review("matches_for_review.csv")
results.sample(n=50, seed=42).export_for_review("sample_for_review.csv")
```

**What to Build (user evaluation workflow):**
- **Document the improvement loop** in README or "Evaluation & improvement": get ground truth (known pairs or from reviewed sample) → run matcher → `metrics = results.evaluate(ground_truth)` → change rules/threshold → re-run → compare metrics → repeat.
- **Use evaluate() in sample-data tests** with known pairs; assert on precision/recall where appropriate.
- **Optional:** accept CSV path for `ground_truth` in `evaluate(ground_truth)`; optional ground truth files for sample data.

**Success Criteria:**
- [x] Can export matches to CSV for review
- [x] Can export a sample via `sample(n=...)` or `sample(fraction=...)`
- [x] Exported file includes identifiers and match context
- [x] Users can load exported file into Excel or any tool
- [x] Improvement loop documented (get GT → match → evaluate → tune → re-run → compare)
- [x] Sample-data tests use evaluate() with known pairs
- [x] Users can load ground truth from CSV/Parquet and pass the DataFrame to evaluate()

**Deferred (consider later):** Load-back of reviewed data, approval/rejection tracking, analyze_review(). Sets up path: reviewed sample → ground truth → evaluate.

**YAGNI:** No Power BI template, no translytical flows, no auto-tuning, no dashboard or run history.

**Estimated Effort:** 0.5–1 day (done)

---

## Future Considerations (Not Planned)

These features are not planned but may be considered if users request them:

### Advanced Features
- **Different field names**: Match `left.email` to `right.email_address` (add when users request)
- **Field mapping suggestions**: Auto-suggest field mappings (add when users request)
- **Complex normalization**: Use libraries like `nameparser`, `usaddress-scourgify` (add when simple normalization doesn't work)
- **Phonetic matching**: Use `jellyfish` for phonetic matching (add when fuzzy matching misses important matches)
- **Multiple blocking keys**: Block on multiple fields (add when users request)
- **Auto-blocking**: Automatically suggest blocking keys (add when users request)

### Integration Features
- **Power BI report template**: Pre-built report for review (add when multiple users need the same report)
- **Translytical flows**: Automated export/import (add when file export/import is too time-consuming)
- **Automatic tuning**: Auto-tune thresholds based on review results (add when manual tuning is too time-consuming)

### Performance Features
- **Streaming API**: Out-of-core processing for very large datasets (add when blocking isn't enough)
- **Distributed processing**: Multi-machine processing (add when single-machine isn't enough)

**Decision Framework:**
- Add only when there's a real, current use case (YAGNI)
- Measure before committing (data-driven decisions)
- Keep it simple (KISS)

---

## Decision Framework

### When to Add Complexity

Add a feature only when:
1. **Current phase works** (meets all success criteria)
2. **Real need exists** (not hypothetical - you have actual data/use case)
3. **Performance/data quality issues** justify the complexity
4. **Can measure improvement** (before/after metrics)

### When to Stop

Stop adding features when:
- Current phase meets all needs
- Adding complexity doesn't improve outcomes
- Performance is acceptable
- Match quality is acceptable

### When to Simplify

Simplify when:
- Current approach is too complex for the problem
- Simpler solution works just as well
- Maintenance burden too high

---

## Success Metrics

### Phase 1 Metrics ✅
- ✅ Finds all known matches (100% recall on test data)
- ✅ Zero false positives (100% precision on test data)
- ✅ Runs in <30 seconds for test dataset
- ✅ Works in notebooks
- ✅ Clear error messages
- ✅ Auditable results

### Phase 2 Metrics (When Built)
- Can process 1M records in <30 minutes
- Blocking reduces comparisons by >90%
- Match quality unchanged (same matches found)
- Performance scales linearly with data size

### Phase 3 Metrics ✅
- ✅ Can match "John Smith" to "J. Smith" with confidence >0.8
- ✅ Can match addresses with typos
- ✅ False positive rate controllable via threshold
- ✅ Performance acceptable (vectorized; target <5 minutes for 10K records)

### Phase 4 Metrics (When Complete)
- ✅ Export matches to CSV; sample via sample(n=...) or sample(fraction=...)
- ✅ Exported file includes identifiers and match context; Excel or any tool
- Improvement loop documented (get GT → match → evaluate → tune → re-run → compare)
- Sample-data tests use evaluate() with known pairs
- Users can load ground truth from CSV/Parquet and pass the DataFrame to evaluate()

---

## Timeline Estimates

### Immediate (Next 1-2 weeks)
- ~~Technical debt~~ ✅ Done. ~~Phase 4 export + sample~~ ✅ Done.
- **Phase 4 completion**: Document improvement loop, use evaluate() in sample-data tests; user loads CSV/Parquet and passes DataFrame to evaluate()
- **Effort:** 0.5–1 day

### Short-term (Next 1-3 months)
- Phase 2 (Blocking) or Phase 4 load-back based on user needs
- **Effort:** 2-3 days each

### Medium-term (3-6 months)
- Complete remaining phases
- Add advanced features as needed
- **Effort:** TBD based on user needs

### Long-term (6+ months)
- Human-in-the-loop workflows
- Power BI integration
- **Effort:** TBD based on user needs

---

## Risk Assessment

### Low Risk
- **Technical debt cleanup**: Removing unused parameters, adding documentation
- **Phase 2 (Blocking)**: Well-understood problem, Polars supports it well
- **Phase 3 (Fuzzy Matching)**: `rapidfuzz` is battle-tested, integration is straightforward

### Medium Risk
- **Phase 4 (Human-in-the-loop & evaluation workflow)**: Export/sample done; workflow docs + tests low risk; load-back deferred, may need iteration when added

### Mitigation Strategies
- **Measure before committing**: Use benchmarks and metrics to justify features
- **YAGNI**: Don't add features until there's a real need
- **KISS**: Keep it simple, add complexity only when proven necessary
- **Data-driven decisions**: Make decisions based on evidence, not assumptions

---

## Next Steps

1. **Immediate (Next)**
   - [x] Remove `max_workers` parameter
   - [x] Document null handling behavior
   - [x] Phase 4 export + sample (CSV, sample())
   - [x] **Phase 4 completion**: Document user evaluation workflow; use evaluate() in sample-data tests; document ground_truth input format
   - [ ] Review roadmap with stakeholders

2. **Short-term (Next 1-3 months)**
   - [ ] Gather user feedback on Phase 1 and Phase 3
   - [ ] Decide on Phase 2 (Blocking) vs Phase 4 load-back based on user needs
   - [ ] Add performance benchmarks if planning Phase 2
   - [ ] Implement chosen phase

3. **Medium-term (3-6 months)**
   - [ ] Complete remaining phases based on user needs
   - [ ] Add advanced features as requested
   - [ ] Monitor performance and user feedback

4. **Long-term (6+ months)**
   - [ ] Human-in-the-loop workflows
   - [ ] Power BI integration
   - [ ] Advanced features as needed

---

## Notes

- **Keep it simple**: Add complexity only when proven necessary (KISS, YAGNI)
- **Measure before committing**: Use benchmarks and metrics to justify features
- **Data-driven decisions**: Make decisions based on evidence, not assumptions
- **Optimize for programmer happiness**: APIs should feel natural and comfortable (hygge philosophy)

---

**Roadmap Complete**
