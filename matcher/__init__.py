"""hygge-match: A cozy, comfortable library for entity resolution and deduplication."""

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
