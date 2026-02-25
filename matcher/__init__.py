"""matcher: A cozy, comfortable library for entity resolution and deduplication."""

from matcher.algorithms import MatchingAlgorithm, ExactMatcher, FuzzyMatcher
from matcher.matcher import Matcher
from matcher.deduplicator import Deduplicator
from matcher.evaluators import Evaluator, SimpleEvaluator, find_best_threshold
from matcher.results import MatchResults

__all__ = [
    "Matcher",
    "Deduplicator",
    "MatchResults",
    "MatchingAlgorithm",
    "ExactMatcher",
    "FuzzyMatcher",
    "Evaluator",
    "SimpleEvaluator",
    "find_best_threshold",
]

__version__ = "0.1.0"
