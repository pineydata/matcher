# Generate PR Summary

Generate a concise, well-structured summary of pull request changes following matcher's principles and the project's PR format.

## Context
- matcher emphasizes simplicity, clarity, and incremental development (KISS, YAGNI)
- **PR summaries should be concise** - aim for 40-60 lines total, not exhaustive documentation
- Focus on **what changed and why**, not every implementation detail
- **Outcomes over details**: What matters to users and reviewers, not every line changed

## Summary Structure

### Overview Section
- 2-4 bullet points summarizing the main changes
- Focus on user-facing impact and key improvements
- Use clear, natural language (matcher style: simple and direct)

### Key Changes Section
Organize changes by component/feature area:
- **Group related changes together** - combine similar modifications into single bullet points
- Use descriptive section headers (e.g., "Matching Algorithm Updates", "DataLoader Improvements")
- **Limit to 3-5 major feature areas** - if more, consider grouping or summarizing
- **Per file: 2-4 bullet points max** - summarize multiple related changes into one point
- Focus on **behavior changes**, not code structure or refactoring details
- **Skip minor changes**: Typo fixes, import reordering, formatting-only changes
- **Combine similar changes**: "Updated X, Y, and Z components to implement new feature()" instead of listing each separately

### Testing Section
- **Summarize test changes** - group test files by what they test, not list every file
- Note test coverage improvements (e.g., "Added 15 new tests covering retry scenarios")
- Mention integration tests or end-to-end verification if significant
- **Keep brief**: "All tests passing: `pytest` (X tests)" is usually sufficient

## hygge/Rails-Inspired Guidelines

- **Comfort**: Does this make the API more comfortable to use?
- **Simplicity**: Is the code clean and intuitive?
- **Reliability**: Is behavior robust and predictable?
- **Flow**: Does matching work smoothly without friction?
- **Convention over Configuration**: Do minimal configs "just work"?
- **Programmer Happiness**: Does this make developers' lives better?
- **Clarity over Cleverness**: Is code simple and clear?

## matcher-Specific Guidelines

- **KISS (Keep It Simple)**: Use clear, natural language
- **YAGNI (You Aren't Gonna Need It)**: Focus on what was actually needed
- **Library-First**: Note any improvements to the notebook-friendly API
- **Incremental Development**: Is complexity added only when proven necessary?

## Output Location

Display the PR summary in a markdown format. Optionally save to a file if requested.

## Output Format

```markdown
---
title: PR Summary - [Brief Description]
tags: [bug, fix]  # or [enhancement, feature], [breaking], [documentation] as appropriate
---

## Overview

- [Main change 1 with user impact]
- [Main change 2 with user impact]
- [Main change 3 if applicable]

## Key Changes

### [Feature/Component Name]

- `path/to/file.py`:
  - [What was changed and why]
  - [Key implementation detail if relevant]

### [Another Feature/Component]

- `path/to/file.py`:
  - [Changes...]

### Tests

- `tests/path/to/test_file.py`:
  - [What is tested]
  - [Coverage improvements]

## Testing

- All tests passing: `pytest` ([X] tests collected, all passing)
- [Any specific test coverage notes]
```

## Analysis Process

1. **Review changed files**: Identify all modified, added, and deleted files
2. **Group by feature**: Organize changes into logical feature areas
3. **Identify user impact**: What does this change enable or fix?
4. **Note testing**: What tests were added or modified?
5. **Check matcher principles**: Does this align with KISS, YAGNI, and simplicity?
6. **Check PR labels**: Ensure appropriate labels are present for release notes categorization

## hygge/Rails Principles to Highlight

- **Comfort**: Did this make the API more comfortable to use?
- **Simplicity**: Did this keep things simple?
- **Reliability**: Did this improve error handling or robustness?
- **Flow**: Did this make matching smoother?
- **Convention over Configuration**: Did this add smart defaults?
- **Programmer Happiness**: Did this make developers' lives better?
- **Backward Compatibility**: Were existing APIs preserved?
- **Progress over Perfection**: Is this good enough to ship and iterate?

## matcher Principles to Highlight

- **KISS (Keep It Simple)**: Did this keep things simple?
- **YAGNI (You Aren't Gonna Need It)**: Did this add only what was needed?
- **Library-First**: Did this improve the notebook-friendly API?
- **Incremental Development**: Did this add complexity only when proven necessary?

## PR Labels for Release Notes

**Important**: Add appropriate labels to PRs to help GitHub automatically generate release notes.

When creating or reviewing PRs, ensure they have appropriate labels:
- `enhancement` or `feature` - for new features or improvements
- `bug` or `fix` - for bug fixes
- `breaking` - for breaking changes (API changes, config changes, etc.)
- `documentation` - for documentation updates

**Why this matters**: GitHub uses these labels to automatically categorize changes in release notes. Without labels, all changes appear in a single "What's Changed" section. With labels, changes are grouped into "New Features", "Bug Fixes", "Breaking Changes", etc.

**Action**: When generating PR summaries:
1. **Add tags to the frontmatter** - Include appropriate tags in the YAML frontmatter (e.g., `tags: [bug, fix]`)
2. **Remind in summary** - Include a note at the end of the summary reminding to add GitHub labels to the PR

## Conciseness Guidelines

### What to Include
- ✅ **User-facing changes**: What does this enable or fix?
- ✅ **Architectural changes**: New patterns, abstractions, or significant refactors
- ✅ **Error handling improvements**: Better reliability or robustness
- ✅ **Breaking changes**: API changes that affect users
- ✅ **Major test additions**: Significant new test coverage

### What to Exclude or Summarize
- ❌ **Implementation details**: How something was implemented (unless architecturally significant)
- ❌ **Minor refactors**: Code cleanup, variable renaming, small reorganizations
- ❌ **Formatting changes**: Import sorting, whitespace, style-only changes
- ❌ **Every file changed**: Group related files together
- ❌ **Every method added**: Summarize functionality, don't list every method

### Length Targets
- **Overview**: 2-4 bullet points (one sentence each)
- **Key Changes**: 3-5 feature sections, 2-4 bullet points per file
- **Testing**: 1-3 lines summarizing test coverage
- **Total**: Aim for 40-60 lines - if longer, you're including too much detail

### Summarization Strategies
1. **Combine similar changes**: "Updated all MatchingAlgorithm implementations to add new method()" instead of listing each algorithm
2. **Focus on outcomes**: "Improved error messages" instead of "Updated 5 exception classes to include context"
3. **Group by feature**: Multiple files for one feature? One section with grouped file list
4. **Skip the obvious**: Don't mention "Added tests" if it's obvious from context

## Output Requirements

- **Concise**: 40-60 lines total - if longer, summarize more aggressively
- **Clear**: Use plain language, avoid jargon
- **Organized**: Group related changes together
- **Selective**: Cover major changes only, skip minor details
- **matcher-style**: Simple, clear, direct language

**Remember**: A PR summary should help reviewers quickly understand what changed and why. If it's too long, reviewers won't read it. Better to be concise and clear than comprehensive and overwhelming.

Generate a summary that helps reviewers quickly understand what changed and why, following matcher's principles of simplicity and clarity - **keep it simple, not comprehensive**.

