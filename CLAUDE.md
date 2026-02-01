# CLAUDE.md - Project Context for AI Assistants

This file provides context and guidelines for AI assistants working on the matcher project.  See [](README.md)

---

## Project Overview

**Matcher** is a Python library for entity resolution and deduplication, built with a focus on simplicity and incremental development.

**Core Purpose:**
- Match records across data sources (entity resolution)
- Identify duplicate records within a single source (deduplication)
- Provide a simple, library-first API optimized for exploration in notebooks
- Enable data-driven decision making through component comparison and evaluation

**Key Design Principles:**
- **KISS (Keep It Simple, Stupid)**: Start with the absolute minimum, add complexity only when proven necessary
- **YAGNI (You Aren't Gonna Need It)**: Don't build features until you have a real, current use case
- **Library-First**: Python API optimized for notebooks, not CLI-first
- **Data-Driven Decisions**: Compare approaches, measure results, and make decisions based on evidence, not assumptions

---

## Philosophy & Principles

### hygge Core Values
- **Comfort**: APIs should feel natural and comfortable to use
- **Simplicity**: Clean, intuitive APIs that feel natural
- **Reliability**: Robust, predictable behavior without surprises
- **Flow**: Smooth, efficient matching without friction

### Rails-Inspired Development Principles

#### Core Principles
1. **Convention over Configuration**: Smart defaults, minimal setup
2. **Programmer Happiness**: APIs should feel natural and comfortable
3. **Flow Over Force**: Matching should work smoothly between data sources
4. **Reliability Over Speed**: Prefer robust, predictable behavior
5. **Clarity Over Cleverness**: Simple, clear code over complex optimizations
6. **Progress over Perfection**: Ship working solutions that solve real problems

#### Detailed Rails Philosophy for matcher
1. **Optimize for Programmer Happiness**
   - Code should make developers smile
   - **matcher application**: Make matching feel natural, not forced

2. **Convention over Configuration**
   - Smart defaults eliminate repetitive decisions
   - "You're not a beautiful and unique snowflake" - embrace conventions
   - **matcher application**: `field="email"` instead of complex configs

3. **The Menu is Omakase**
   - Curated stack decisions, not endless choice paralysis
   - **matcher application**: Polars by default

4. **No One Paradigm**
   - Use the right tool for each job, not one-size-fits-all
   - **matcher application**: Component-based architecture (DataLoader, MatchingAlgorithm) for flexibility

5. **Exalt Beautiful Code**
   - Aesthetically pleasing code is valuable
   - **matcher application**: `matcher.match_exact()` feels natural, not `MatcherExecutor.execute_match()`

6. **Provide Sharp Knives**
   - Trust programmers with powerful tools
   - **matcher application**: Allow custom DataLoader/MatchingAlgorithm implementations

7. **Value Integrated Systems**
   - Majestic monoliths over premature microservices
   - **matcher application**: Single library for matching, not scattered tools

8. **Progress over Stability**
   - Evolution keeps libraries relevant
   - **matcher application**: Keep improving the API and error handling

9. **Push up a Big Tent**
   - Welcome disagreement and diversity of thought
   - **matcher application**: Support different data sources and use cases

### Pythonic Development Principles

matcher code should be **Pythonic** - following Python's idioms, conventions, and philosophy. Pythonic code feels natural to Python developers and reads like well-written English.

#### Core Zen of Python Principles

1. **Beautiful is Better Than Ugly**: Code should be aesthetically pleasing (`matcher.match_exact()` not `MatcherExecutor.execute()`)
2. **Explicit is Better Than Implicit**: Make intentions clear (`field="email"` explicitly declares matching field)
3. **Simple is Better Than Complex**: Prefer straightforward solutions with smart defaults
4. **Complex is Better Than Complicated**: Some problems require complexity, but keep it organized
5. **Flat is Better Than Nested**: Keep structures flat (simple module structure, not deep nesting)
6. **Sparse is Better Than Dense**: Use whitespace and clear formatting
7. **Readability Counts**: Code is read more often than written (`Matcher`, `MatchResults` are self-documenting)
8. **Special Cases Aren't Special Enough**: Unified interfaces work for all types, not special cases
9. **Practicality Beats Purity**: Use what works, not dogmatic approaches
10. **Errors Should Never Pass Silently**: Fail fast with clear messages
11. **In the Face of Ambiguity, Refuse to Guess**: Clear validation rejects ambiguous inputs
12. **One Obvious Way**: `matcher.match_exact()` is the obvious way, not multiple execution methods
13. **If Hard to Explain, it's a Bad Idea**: Core logic should be explainable
14. **Namespaces are One Honking Great Idea**: `matcher.core` provides clear namespaces

#### Pythonic Patterns for matcher

- **Type Hints**: Full type hints on all public APIs
- **f-strings**: Use f-strings for all string formatting (not `.format()` or `%`)
- **pathlib.Path**: Use `Path` objects for file paths, not string paths
- **EAFP (Easier to Ask for Forgiveness)**: Try operations, handle specific exceptions, provide helpful messages
- **Duck Typing**: Use protocols for interfaces (`DataLoader`, `MatchingAlgorithm`), not `isinstance()` checks
- **Comprehensions & Generators**: Use for transformations and data processing
- **Enums**: Use for constants (matching types, etc.)
- **Property Decorators**: Use `@property` for computed attributes (`results.count`)

## Data-Driven Development Philosophy

**Matcher is built for experimentation and comparison.** The component-based architecture and evaluation capabilities are foundational, not premature optimization. They enable data-driven decision making.

**Core Philosophy**: Measure before committing. Swap components, test approaches, measure results, then decide based on evidence (precision, recall, F1), not assumptions.

**Why Components?** Enables experimentation, side-by-side comparison, measurement, and informed decisions. Try different algorithms without rewriting core logic. Component architecture and evaluation are foundational - they enable measurement and comparison.

**Example Workflow:**
```python
# Test and compare approaches
matcher_exact = Matcher(..., matching_algorithm=ExactMatcher())
metrics_exact = matcher_exact.match_exact(field="email").evaluate(ground_truth)

matcher_ci = Matcher(..., matching_algorithm=CaseInsensitiveExactMatcher())
metrics_ci = matcher_ci.match_exact(field="email").evaluate(ground_truth)

# Choose based on evidence
if metrics_ci['recall'] > metrics_exact['recall']:
    # Case-insensitive performs better
    pass
```

---

## Code Quality & Review Principles

**DO NOT RUSH CHANGES** - Always consider the greater codebase and adhere to strong development standards:

1. **DRY**: Extract duplicated logic, check for existing solutions before adding code
2. **KISS**: Prefer simple, clear solutions. Code should be readable by someone who didn't write it
3. **YAGNI**: Don't add functionality "just in case". Solve the current problem, not hypothetical future problems
4. **Backward Compatibility**: Maintain backward compatibility unless there is a clear discussion about breaking changes

**Development Style:**
- Be direct and candid (not deferential)
- Prioritize user experience over technical perfection
- Think like a data engineer: outcomes and impact matter more than perfection
- Make progress, not perfection: ship working solutions

---

## Development Guidelines

### When Adding Features

**Before adding any feature, ask:**
1. Do I have a real, current use case? (Not hypothetical)
2. Is it blocking progress?
3. Can I add it later easily?
4. Do users actually need this?
5. Can I measure the value?

**If any answer is "no" or "maybe", don't build it yet.**

## Decision Framework

### When to Add Complexity

Add a feature only when:
1. Current phase works (meets all success criteria)
2. Real need exists (not hypothetical - you have actual data/use case)
3. Performance/data quality issues justify the complexity
4. Can measure improvement (before/after metrics)

### When to Stop

Stop adding features when:
- Current phase meets all needs
- Adding complexity doesn't improve outcomes
- Performance is acceptable
- Match quality is acceptable

---

## Common Patterns

### Loading Data
```python
# From file paths
matcher = Matcher(left_source="data/a.parquet", right_source="data/b.parquet")

# From Polars DataFrames
matcher = Matcher(left_source=df1, right_source=df2)

# Deduplication (single source)
matcher = Matcher(left_source="data/customers.parquet")
```

### Matching & Evaluation
```python
# Exact matching
results = matcher.match_exact(field="email")

# Evaluate against ground truth
metrics = results.evaluate(ground_truth)
print(f"Precision: {metrics['precision']:.2%}, Recall: {metrics['recall']:.2%}")
```

---

## References

- **Main Plan:** `MATCHING_PLAN_V2.md`

**Remember:** The goal is to build something that works for the use case, not something that has every possible feature.
