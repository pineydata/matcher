"""Tutorial data package: generate and load sample datasets on the fly.

Use from notebooks (run from repository root with docs/tutorial on PYTHONPATH)::

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(".").resolve() / "docs" / "tutorial"))
    from tutorial_data import load_evaluation, load_fuzzy_evaluation, load_blocking_evaluation
    left, right, ground_truth = load_fuzzy_evaluation()

Or generate data only (e.g. for scripts)::

    from tutorial_data import generate_fuzzy_evaluation_data
    left, right = generate_fuzzy_evaluation_data()
"""

from tutorial_data._generate import (
    generate_blocking_evaluation_data,
    generate_deduplication_data,
    generate_entity_resolution_data,
    generate_evaluation_test_data,
    generate_fuzzy_evaluation_data,
)
from tutorial_data.load import (
    load_blocking_evaluation,
    load_deduplication,
    load_entity_resolution,
    load_entity_resolution_standardized,
    load_evaluation,
    load_fuzzy_evaluation,
)
from tutorial_data.prep import standardize_for_matching

__all__ = [
    "load_evaluation",
    "load_fuzzy_evaluation",
    "load_blocking_evaluation",
    "load_entity_resolution",
    "load_entity_resolution_standardized",
    "load_deduplication",
    "standardize_for_matching",
    "generate_evaluation_test_data",
    "generate_fuzzy_evaluation_data",
    "generate_blocking_evaluation_data",
    "generate_entity_resolution_data",
    "generate_deduplication_data",
]
