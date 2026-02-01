"""Matcher: A simple Python library for entity resolution and deduplication."""

from matcher.core import (
    Matcher,
    MatchResults,
    DataLoader,
    ParquetLoader,
    MatchingAlgorithm,
    ExactMatcher,
    Evaluator,
    SimpleEvaluator,
)

__all__ = [
    "Matcher",
    "MatchResults",
    "DataLoader",
    "ParquetLoader",
    "MatchingAlgorithm",
    "ExactMatcher",
    "Evaluator",
    "SimpleEvaluator",
]
__version__ = "0.1.0"
