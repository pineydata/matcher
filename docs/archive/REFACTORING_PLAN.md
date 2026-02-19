# Refactoring Plan: Split core.py into Logical Modules

**Date:** 2024
**Goal:** Split `matcher/core.py` (736 lines) into logically organized modules for better maintainability and scalability.

---

## Current State

- **File:** `matcher/core.py` (736 lines)
- **Classes:** 7 classes in a single file
  - `MatchingAlgorithm` (abstract base)
  - `ExactMatcher` (concrete implementation)
  - `Matcher` (entity resolution)
  - `Deduplicator` (deduplication wrapper)
  - `Evaluator` (abstract base)
  - `SimpleEvaluator` (concrete implementation)
  - `MatchResults` (results class)

---

## Proposed File Structure

### 1. `matcher/algorithms.py` (~100 lines)
**Purpose:** Matching algorithm components

**Classes:**
- `MatchingAlgorithm` (abstract base class)
- `ExactMatcher` (concrete implementation)

**Dependencies:**
- `polars`
- `abc` (ABC, abstractmethod)

---

### 2. `matcher/matcher.py` (~240 lines)
**Purpose:** Entity resolution matching

**Classes:**
- `Matcher` (main entity resolution class)

**Dependencies:**
- `matcher.algorithms` (MatchingAlgorithm, ExactMatcher)
- `matcher.results` (MatchResults)
- `polars`

---

### 3. `matcher/deduplicator.py` (~85 lines)
**Purpose:** Deduplication wrapper

**Classes:**
- `Deduplicator` (convenience wrapper around Matcher)

**Dependencies:**
- `matcher.matcher` (Matcher)
- `matcher.algorithms` (MatchingAlgorithm)
- `matcher.results` (MatchResults)
- `polars`

---

### 4. `matcher/evaluators.py` (~90 lines)
**Purpose:** Evaluation components

**Classes:**
- `Evaluator` (abstract base class)
- `SimpleEvaluator` (concrete implementation)

**Dependencies:**
- `polars`
- `abc` (ABC, abstractmethod)

---

### 5. `matcher/results.py` (~220 lines)
**Purpose:** Match results and operations

**Classes:**
- `MatchResults` (results class with pipe, refine, evaluate methods)

**Dependencies:**
- `matcher.matcher` (Matcher)
- `matcher.deduplicator` (Deduplicator)
- `matcher.evaluators` (Evaluator, SimpleEvaluator)
- `polars`

---

## Import Strategy

### Update `matcher/__init__.py`

Re-export all public classes to maintain backward compatibility:

```python
"""matcher: A cozy, comfortable library for entity resolution and deduplication."""

from matcher.algorithms import MatchingAlgorithm, ExactMatcher
from matcher.matcher import Matcher
from matcher.deduplicator import Deduplicator
from matcher.evaluators import Evaluator, SimpleEvaluator
from matcher.results import MatchResults

__all__ = [
    "Matcher",
    "Deduplicator",
    "MatchResults",
    "MatchingAlgorithm",
    "ExactMatcher",
    "Evaluator",
    "SimpleEvaluator",
]

__version__ = "0.1.0"
```

**Result:** Existing imports continue to work:
```python
from matcher import Matcher, Deduplicator, MatchResults
# Still works! No breaking changes.
```

---

## Benefits

1. **Clear Separation of Concerns**
   - Each file has a single, well-defined responsibility
   - Algorithms, matchers, evaluators, and results are clearly separated

2. **Easier Navigation**
   - Developers can quickly find what they need
   - No more scrolling through 736 lines

3. **Better Scalability**
   - Easy to add new algorithms without bloating a single file
   - New evaluators can be added to `evaluators.py`
   - New result operations can be added to `results.py`

4. **Improved Maintainability**
   - Smaller files are easier to understand and modify
   - Changes to one component don't require navigating a large file
   - Better for code reviews

5. **Backward Compatible**
   - No breaking changes to the public API
   - All existing code continues to work

---

## Implementation Steps

1. **Create new module files**
   - `matcher/algorithms.py`
   - `matcher/matcher.py`
   - `matcher/deduplicator.py`
   - `matcher/evaluators.py`
   - `matcher/results.py`

2. **Move classes to appropriate files**
   - Copy classes with all their methods
   - Ensure all imports are correct
   - Handle circular dependencies if needed

3. **Update `matcher/__init__.py`**
   - Add imports from new modules
   - Update `__all__` list

4. **Run tests**
   - Verify all tests still pass
   - Ensure no import errors

5. **Remove `matcher/core.py`**
   - Delete the old file after verification

6. **Update documentation**
   - Update any references to `core.py` in docs
   - Update README if needed

---

## Dependency Graph

```
algorithms.py
    ↓
matcher.py ──→ results.py
    ↑              ↓
deduplicator.py ──┘
    ↓
evaluators.py
```

**Note:** `results.py` depends on both `matcher.py` and `deduplicator.py` for the `refine()` method. This is acceptable as it's a clear dependency.

---

## Testing Strategy

After refactoring:
1. Run full test suite: `uv run python -m pytest tests/ -v`
2. Verify all 47 tests pass
3. Check for any import warnings or errors
4. Test that existing code examples still work

---

## Notes

- Keep the same docstrings and comments
- Maintain the same code style and formatting
- No functional changes - this is purely a structural refactoring
- Consider adding `__init__.py` files if needed for proper package structure

---

**Status:** Ready to implement
