# hygge-match Roadmap

**Last Updated:** 2024
**Current Phase:** Phase 1 Complete ✅
**Next Phase:** TBD (Phase 2 Blocking or Phase 3 Fuzzy Matching)

---

## Executive Summary

**Current State:** Phase 1 (Exact Matching) is complete and production-ready. The codebase is clean, well-tested, and ready for continued development.

**Strategic Direction:**
- **Immediate (Next 1-2 weeks)**: Address technical debt, improve documentation
- **Short-term (Next 1-3 months)**: Phase 2 (Blocking) or Phase 3 (Fuzzy Matching) based on user needs
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
- Address technical debt (see below)
- Plan for Phase 2 or Phase 3 based on user needs

---

## Technical Debt & Immediate Improvements

### Priority: Medium

#### 1. Remove `max_workers` Parameter

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

#### 2. Document Null Handling

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

## Phase 2: Blocking (Performance Optimization)

**Status:** Not Started

**Goal:** Handle larger datasets efficiently by reducing comparisons.

**When to Build:**
- ✅ Phase 1 works
- ✅ Have datasets >100K records
- ✅ Performance is too slow for production datasets
- ✅ Can measure improvement (before/after benchmarks)

**What to Build:**
- Single blocking key (zip code, area code, etc.)
- Generate candidate pairs within blocks only
- Reduce O(n²) comparisons dramatically

**API Design:**
```python
# Blocking for performance - simple, one key
results = matcher.match(
    rules="email",
    blocking_key="zip_code"  # Optional, user-specified
)
```

**Success Criteria:**
- [ ] Can process 1M records in <30 minutes
- [ ] Blocking reduces comparisons by >90%
- [ ] Match quality unchanged (same matches found)
- [ ] Performance scales linearly with data size

**YAGNI Decisions:**
- No automatic blocking suggestions (users know their data)
- No multiple blocking keys (add later if users request)
- No auto-blocking (add later if users request)

**Estimated Effort:** 2-3 days

**Dependencies:**
- Performance benchmarks (to justify and measure improvement)

---

## Phase 3: Fuzzy Matching

**Status:** Not Started

**Goal:** Handle typos and variations ("John Smith" vs "J. Smith").

**When to Build:**
- ✅ Phase 1 works
- ✅ Real data has typos/variations that exact matching misses
- ✅ Need fuzzy matching to find important matches
- ✅ Can measure improvement (before/after metrics)

**What to Build:**
- New `match_fuzzy()` method (or extend `match()` with `threshold` parameter)
- Single algorithm (Jaro-Winkler - good default)
- Single threshold (no per-field thresholds initially)
- Add `rapidfuzz` dependency
- Simple normalization (lowercase, trim) if needed

**API Design:**
```python
# Single-field fuzzy matching - simple
results = matcher.match_fuzzy(
    field="name",
    threshold=0.85  # That's it
)
```

**Success Criteria:**
- [ ] Can match "John Smith" to "J. Smith" with confidence >0.8
- [ ] Can match addresses with typos
- [ ] False positive rate acceptable (<5% on test dataset)
- [ ] Performance acceptable (<5 minutes for 10K records)
- [ ] Confidence scores make sense (can audit matches)
- [ ] Can combine with blocking for performance

**YAGNI Decisions:**
- No algorithm selection (use Jaro-Winkler only)
- No per-field thresholds (use one threshold for all fields)
- No weights (use simple averaging if multi-field added later)
- No complex normalization (add libraries only if simple normalization doesn't work)

**Estimated Effort:** 2-3 days

**Dependencies:**
- `rapidfuzz` library (add to dependencies)

---

## Phase 4: Human in the Loop

**Status:** Not Started

**Goal:** Enable human review and scoring of matches using Power BI Reports and translytical flows.

**When to Build:**
- ✅ Phase 1 works (or Phase 2/3 if needed)
- ✅ Need human review workflow
- ✅ Have matches that need validation

**What to Build:**
- Export matches with source data for review
- Load reviewed matches back
- Simple statistics (approved/rejected counts)

**API Design:**
```python
# Export matches for review
results = matcher.match_fuzzy(field="name", threshold=0.85)
results.export_for_review("matches_for_review.parquet")

# Users review in Power BI (or Excel, or any tool)
# They add columns: human_score (True/False), human_notes

# Load reviewed matches back
reviewed = pl.read_parquet("reviewed_matches.parquet")
stats = results.analyze_review(reviewed)
print(f"Total: {stats['total']}")
print(f"Approved: {stats['approved']}")
print(f"Rejected: {stats['rejected']}")
```

**Success Criteria:**
- [ ] Can export matches with source data for review
- [ ] Users can load exported file into their tool of choice
- [ ] Can load reviewed matches back
- [ ] Can analyze review results (simple counts)
- [ ] Users can manually adjust thresholds based on results

**YAGNI Decisions:**
- No Power BI report template (users can build their own)
- No translytical flows (file export/import is enough initially)
- No automatic tuning (users can tune manually)
- No complex statistics (precision/recall when you have ground truth)

**Estimated Effort:** 2-3 days

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

### Phase 3 Metrics (When Built)
- Can match "John Smith" to "J. Smith" with confidence >0.8
- Can match addresses with typos
- False positive rate acceptable (<5% on test dataset)
- Performance acceptable (<5 minutes for 10K records)

### Phase 4 Metrics (When Built)
- Can export matches with source data for review
- Users can load exported file into their tool of choice
- Can load reviewed matches back
- Can analyze review results (simple counts)

---

## Timeline Estimates

### Immediate (Next 1-2 weeks)
- Address technical debt (remove `max_workers`, document null handling)
- **Effort:** 2-3 hours

### Short-term (Next 1-3 months)
- Phase 2 (Blocking) or Phase 3 (Fuzzy Matching) based on user needs
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
- **Phase 4 (Human-in-the-loop)**: Requires understanding user workflows, may need iteration

### Mitigation Strategies
- **Measure before committing**: Use benchmarks and metrics to justify features
- **YAGNI**: Don't add features until there's a real need
- **KISS**: Keep it simple, add complexity only when proven necessary
- **Data-driven decisions**: Make decisions based on evidence, not assumptions

---

## Next Steps

1. **Immediate (This Week)**
   - [ ] Remove `max_workers` parameter
   - [ ] Document null handling behavior
   - [ ] Review roadmap with stakeholders

2. **Short-term (Next 1-3 months)**
   - [ ] Gather user feedback on Phase 1
   - [ ] Decide on Phase 2 (Blocking) vs Phase 3 (Fuzzy Matching) based on user needs
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
