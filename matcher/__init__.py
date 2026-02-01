"""hygge-match: A cozy, comfortable library for entity resolution and deduplication."""

from matcher.core import (
    Matcher,
    MatchResults,
    MatchingAlgorithm,
    ExactMatcher,
    Evaluator,
    SimpleEvaluator,
)

__all__ = [
    "Matcher",
    "MatchResults",
    "MatchingAlgorithm",
    "ExactMatcher",
    "Evaluator",
    "SimpleEvaluator",
]
__version__ = "0.1.0"
