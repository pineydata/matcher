# Fix Linting Issues

Fix linting and formatting issues using ruff, following matcher's code quality standards.

## Linting Tool
- **Primary tool**: ruff (configured in `pyproject.toml`)
- **Line length**: 88 characters (Black-compatible)
- **Target Python**: 3.10+
- **Enabled rules**: pycodestyle (E), Pyflakes (F), isort (I)

## Fix Process

**Note: This command provides guidance and fixes code directly. Do NOT run terminal commands.**

1. **Identify linting issues** by analyzing the code against ruff rules
2. **Apply fixes directly** to the code files (imports, formatting, style)
3. **Preserve functionality** - only fix linting, never change logic

## Common Fixes

### Import Organization (isort)
- Group imports: stdlib, third-party, first-party (matcher)
- Sort imports alphabetically within groups
- Use `known-first-party = ["matcher"]` configuration

### Code Style (pycodestyle)
- Fix line length violations (88 chars)
- Fix whitespace issues
- Fix indentation problems

### Code Quality (Pyflakes)
- Remove unused imports
- Fix undefined names
- Fix unused variables (allow `_` prefix)

## matcher-Specific Considerations

- **Don't break functionality**: Only fix linting, not logic
- **Maintain backward compatibility**: Preserve existing APIs and behavior unless there's a clear discussion about breaking changes
- **Preserve matcher patterns**: Maintain existing code structure
- **Maintain Polars usage**: Don't change Polars API calls
- **Preserve type hints**: Keep type annotations
- **Follow KISS/YAGNI**: Don't add complexity while fixing linting
- **Follow hygge/Rails philosophy**: Keep code comfortable, simple, reliable, and flowing
- **Be Pythonic**: Maintain Pythonic patterns and idioms

## Output

After fixing:
1. List all issues that were fixed
2. List any issues that need manual attention
3. Verify code still follows matcher principles (KISS, YAGNI, hygge/Rails philosophy)
4. Ensure code remains Pythonic and comfortable to use
5. Ensure tests still pass (if applicable)

## Manual Review Needed

Some issues may require manual review:
- Complex refactoring opportunities
- Logic changes (not just formatting)
- Architecture decisions
- **Breaking changes or backward compatibility concerns** - These require explicit discussion before implementation

Flag these for the developer to review rather than auto-fixing.

