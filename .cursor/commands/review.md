# Code Review

Review code for adherence to matcher's development principles: DRY, KISS, YAGNI, hygge/Rails-inspired philosophy, Pythonic patterns, and matcher-specific guidelines.

## Review Checklist

### DRY (Don't Repeat Yourself)
- [ ] Is similar code duplicated elsewhere in the codebase?
- [ ] Can duplicated logic be extracted into helper methods or base classes?
- [ ] When overriding methods, does it call `super()` instead of duplicating logic?
- [ ] Are there opportunities to reuse existing methods?

### KISS (Keep It Simple, Stupid)
- [ ] Is this the simplest solution that solves the problem?
- [ ] Can complex logic be broken into smaller, understandable pieces?
- [ ] Is the code readable by someone who didn't write it?
- [ ] Are there unnecessary abstractions or over-engineering?

### YAGNI (You Aren't Gonna Need It)
- [ ] Is functionality added "just in case" rather than for a concrete need?
- [ ] Does this solve the current problem, not hypothetical future problems?
- [ ] Is there premature optimization or unnecessary complexity?

### Second-Guess Fallbacks (CRITICAL)
**Second-guess fallbacks are the lurking missing commas of implementations** - hard to find and debug. Only allow them in the strictest of circumstances.

- [ ] Are there fallback behaviors that try to "second guess" what should happen?
- [ ] Does code silently fall back to alternative behavior instead of failing clearly?
- [ ] Are there multiple code paths that could execute based on implicit assumptions?
- [ ] Would a fallback behavior be hard to debug if it triggered unexpectedly?
- [ ] Is the fallback behavior explicitly documented and justified?
- [ ] Could the code fail fast and clearly instead of using a fallback?

**Red flags:**
- Silent fallbacks that mask real problems
- "Try this, but if it fails, try that" patterns without clear error handling
- Implicit assumptions about what the user "probably meant"
- Multiple layers of fallback logic
- Fallbacks that make debugging harder

**Acceptable fallbacks (strict circumstances only):**
- Explicit, documented fallbacks with clear error logging
- Fallbacks that are part of the public API contract
- Fallbacks that fail fast after attempting the fallback
- Fallbacks that are testable and predictable

**When in doubt: FAIL FAST. Let the error surface clearly rather than hiding it behind a fallback.**

### hygge/Rails-Inspired Principles
- [ ] **Comfort**: Does the API feel natural and comfortable to use?
- [ ] **Simplicity**: Is the code clean and intuitive?
- [ ] **Reliability**: Is behavior robust and predictable without surprises?
- [ ] **Flow**: Does matching work smoothly without friction?
- [ ] **Convention over Configuration**: Are smart defaults used instead of complex configs?
- [ ] **Programmer Happiness**: Does this make developers' lives better?
- [ ] **Clarity Over Cleverness**: Is code simple and clear, not overly clever?
- [ ] **Progress over Perfection**: Is this good enough to ship and iterate?

### matcher-Specific Principles
- [ ] **KISS (Keep It Simple)**: Is this the simplest solution?
- [ ] **YAGNI (You Aren't Gonna Need It)**: Is this solving a real, current need?
- [ ] **Library-First**: Is the API optimized for notebook usage?
- [ ] **Incremental Development**: Is complexity added only when proven necessary?

### Architecture Consistency
- [ ] Does this maintain consistency with existing patterns?
- [ ] Does this fit into the broader architecture?
- [ ] Is component-based architecture used appropriately (DataLoader, MatchingAlgorithm)?
- [ ] Does this follow matcher's simple, single-file approach (until >500 lines)?

### Backward Compatibility
- [ ] **Maintain backward compatibility** unless there is a clear discussion about breaking changes
- [ ] Are existing APIs preserved?
- [ ] Will this change break existing user code or configurations?
- [ ] If breaking changes are proposed, is there explicit discussion and justification?
- [ ] Are breaking changes clearly documented and communicated?

### Code Quality
- [ ] Is Polars used for all data operations (not pandas)?
- [ ] Are type hints used appropriately?
- [ ] Is error handling clear and fails fast?
- [ ] Are custom exceptions used where appropriate?

### Pythonic Code Standards
- [ ] **Beautiful is Better Than Ugly**: Is the code aesthetically pleasing and readable?
- [ ] **Explicit is Better Than Implicit**: Are intentions clear, not hidden in magic?
- [ ] **Simple is Better Than Complex**: Is this the simplest solution?
- [ ] **Complex is Better Than Complicated**: If complex, is it well-organized and elegant?
- [ ] **Flat is Better Than Nested**: Are structures flat and shallow?
- [ ] **Sparse is Better Than Dense**: Is code readable with proper whitespace?
- [ ] **Readability Counts**: Can someone else understand this code?
- [ ] **Special Cases Aren't Special Enough**: Is there consistent, general handling?
- [ ] **Practicality Beats Purity**: Is this pragmatic, not dogmatic?
- [ ] **Errors Should Never Pass Silently**: Do errors fail fast and clearly?
- [ ] **In the Face of Ambiguity, Refuse to Guess**: Does Pydantic validation reject ambiguous configs?
- [ ] **One Obvious Way**: Is `matcher.match_exact()` the clear, obvious pattern?
- [ ] **If Hard to Explain, it's a Bad Idea**: Can this implementation be easily explained?
- [ ] **Namespaces**: Are modules organized logically (`matcher.core`, etc.)?

### Pythonic Patterns
- [ ] **Type Hints**: Are full type hints used on public APIs?
- [ ] **f-strings**: Are f-strings used for string formatting (not `.format()` or `%`)?
- [ ] **pathlib.Path**: Are `Path` objects used instead of string paths?
- [ ] **Context Managers**: Are `async with` statements used for resource management?
- [ ] **EAFP**: Does code try operations and handle exceptions (not check preconditions excessively)?
- [ ] **Duck Typing**: Are protocols used for interfaces (`DataLoader`, `MatchingAlgorithm`), not `isinstance()` checks?
- [ ] **Comprehensions**: Are comprehensions/generators used for transformations?
- [ ] **Dataclasses/Pydantic**: Are modern data structures used (Pydantic for configs, dataclasses for simple containers)?
- [ ] **Enums**: Are enums used for constants (not magic strings)?
- [ ] **Property Decorators**: Are `@property` decorators used for computed attributes?

### Pythonic Anti-Patterns to Avoid
- [ ] **Overusing `isinstance()`**: Is duck typing used instead of type checking?
- [ ] **Manual iteration with indices**: Is `enumerate()` or direct iteration used?
- [ ] **String concatenation with `+`**: Are f-strings or `.join()` used?
- [ ] **Catching bare `Exception`**: Are specific exceptions caught?
- [ ] **Using `== None`**: Is `is None` or `is not None` used?
- [ ] **Mutable default arguments**: Are `None` defaults with assignment in body used?
- [ ] **Importing with `*`**: Are explicit imports used?
- [ ] **Not using context managers**: Are `with` statements used for resources?

## Review Process

1. **Search for similar patterns** in the codebase before suggesting changes
2. **Identify duplication** and suggest extraction opportunities
3. **Question complexity** - is there a simpler way?
4. **Hunt for second-guess fallbacks** - these are dangerous and hard to debug
5. **Verify matcher principles** - does this follow KISS and YAGNI?
6. **Check architecture** - does this fit the existing structure?

## Review Output

1. **Write to `.dev/PR_REVIEW.md`**: Save the generated review to `.dev/PR_REVIEW.md`, then display it in your response.

2. **Content to provide:**
   - **Strengths**: What's working well
   - **Issues**: Specific violations of DRY/KISS/YAGNI or matcher principles
   - **Fallback Warnings**: Any second-guess fallbacks that should be removed or made explicit
   - **Suggestions**: Concrete improvements with code examples
   - **Questions**: Areas that need clarification

Be direct and candid (not deferential), focusing on matcher's ethos of simplicity, clarity, and incremental development, guided by hygge's comfort and Rails-inspired programmer happiness.
