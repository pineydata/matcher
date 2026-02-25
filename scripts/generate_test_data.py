"""Generate test datasets for matcher validation and tutorial.

Uses the tutorial_data package (docs/tutorial/tutorial_data/) as the single source
of generator logic. Writes parquet and ground_truth.md to data/ for tests and
for users who prefer to generate once. Tutorial notebooks can instead load
data on the fly via tutorial_data.load_* so no data need be committed.
"""

import sys
from pathlib import Path

# Allow importing tutorial_data when run as script from repo root
_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root / "docs" / "tutorial"))

from tutorial_data._generate import (
    generate_blocking_evaluation_data,
    generate_deduplication_data,
    generate_entity_resolution_data,
    generate_evaluation_test_data,
    generate_fuzzy_evaluation_data,
)


def save_ground_truth(left_df, right_df, known_matches, match_types, left_match_info, right_match_info, output_dir):
    """Save ground truth documentation."""

    ground_truth = []
    ground_truth.append("# Entity Resolution Ground Truth\n")
    ground_truth.append(f"Total known matches: 40\n\n")

    ground_truth.append("## Match Types\n\n")
    ground_truth.append("- **Email-only matches** (10): Match only on `email` field\n")
    ground_truth.append("- **Name-only matches** (10): Match only on `first_name + last_name` (emails differ)\n")
    ground_truth.append("- **Multi-field matches** (10): Match on `email + zip_code` together (names differ)\n")
    ground_truth.append("- **Mixed matches** (10): Can match on `email` OR `first_name + last_name`\n\n")

    ground_truth.append("## Known Matches\n\n")
    ground_truth.append("| Left ID | Right ID | Match Type | Rule(s) |\n")
    ground_truth.append("|---------|----------|------------|----------|\n")

    # Build mapping of match pairs - they're in the same order
    match_pairs = []
    for i, (left_info, right_info) in enumerate(zip(left_match_info, right_match_info)):
        left_idx, _, match_type = left_info
        right_idx, _, _ = right_info

        if match_type == "email":
            rules = "email"
        elif match_type == "name":
            rules = "first_name + last_name"
        elif match_type == "address+zip":
            rules = "address + zip_code"
        else:  # email_or_name
            rules = "email OR (first_name + last_name)"

        match_pairs.append((f"left_{left_idx+1}", f"right_{right_idx+1}", match_type, rules))

    for left_id, right_id, match_type, rules in match_pairs:
        ground_truth.append(f"| {left_id} | {right_id} | {match_type} | {rules} |\n")

    ground_truth.append("\n## Expected Results\n\n")
    ground_truth.append("- **Total matches**: 40\n")
    ground_truth.append("- **Match rules**: Various (see table above)\n")
    ground_truth.append("- **Match type**: exact\n")
    ground_truth.append("- **All matches should be found** (100% recall)\n")
    ground_truth.append("- **No false positives** (100% precision)\n\n")

    ground_truth.append("## Testing Different Rules\n\n")
    ground_truth.append("```python\n")
    ground_truth.append("# Email-only matches\n")
    ground_truth.append("results = matcher.match(on=\"email\")\n")
    ground_truth.append("# Should find: 10 email-only + 10 mixed = 20 matches\n\n")
    ground_truth.append("# Name-only matches\n")
    ground_truth.append("results = matcher.match(on=[\"first_name\", \"last_name\"])\n")
    ground_truth.append("# Should find: 10 name-only + 10 mixed = 20 matches\n\n")
    ground_truth.append("# Multi-field matches\n")
    ground_truth.append("results = matcher.match(on=[\"address\", \"zip_code\"])\n")
    ground_truth.append("# Should find: 10 multi-field matches\n\n")
    ground_truth.append("# Multiple rules (OR logic)\n")
    ground_truth.append("results = matcher.match(on=\"email\").refine(on=[\"first_name\", \"last_name\"])\n")
    ground_truth.append("# Should find: All 40 matches\n")
    ground_truth.append("```\n")

    output_path = output_dir / "ground_truth.md"
    output_path.write_text("".join(ground_truth))
    print(f"Saved ground truth to {output_path}")


def save_deduplication_ground_truth(duplicate_groups, output_dir):
    """Save deduplication ground truth documentation."""

    ground_truth = []
    ground_truth.append("# Deduplication Ground Truth\n")
    ground_truth.append(f"Total duplicate groups: {len(duplicate_groups)}\n")
    ground_truth.append(f"Total duplicate pairs: {len(duplicate_groups)}\n\n")
    ground_truth.append("## Known Duplicate Groups (by email)\n\n")
    ground_truth.append("| Group | Record IDs | Email |\n")
    ground_truth.append("|-------|------------|-------|\n")

    for i, group in enumerate(duplicate_groups):
        email = f"duplicate_{i}@example.com"
        record_ids = ", ".join(group)
        ground_truth.append(f"| {i+1} | {record_ids} | {email} |\n")

    ground_truth.append("\n## Expected Results\n\n")
    ground_truth.append("- **Total duplicate pairs**: 50\n")
    ground_truth.append("- **Match field**: email\n")
    ground_truth.append("- **Match type**: exact\n")
    ground_truth.append("- **All duplicates should be found** (100% recall)\n")
    ground_truth.append("- **No false positives** (100% precision)\n")

    output_path = output_dir / "ground_truth.md"
    output_path.write_text("".join(ground_truth))
    print(f"Saved ground truth to {output_path}")


def save_evaluation_style_ground_truth(output_dir: Path, title: str, body_lines: list[str]) -> None:
    """Write ground_truth.md for evaluation-style sets (30 pairs, eval_left_1..30, eval_right_1..30)."""
    lines = [f"# {title}\n\n", "Ground truth: 30 pairs. IDs are eval_left_1..eval_left_30 and eval_right_1..eval_right_30.\n\n"]
    lines.extend(body_lines)
    (output_dir / "ground_truth.md").write_text("".join(lines))
    print(f"Saved ground truth to {output_dir / 'ground_truth.md'}")


def main():
    """Generate all test datasets organized by component."""

    # Create organized directory structure by component
    data_dir = Path("data")
    exact_matcher_dir = data_dir / "ExactMatcher"
    simple_evaluator_dir = data_dir / "SimpleEvaluator"

    # ExactMatcher subdirectories
    er_dir = exact_matcher_dir / "entity_resolution"
    dedup_dir = exact_matcher_dir / "deduplication"
    eval_dir = exact_matcher_dir / "evaluation"
    fuzzy_eval_dir = exact_matcher_dir / "fuzzy_evaluation"
    blocking_eval_dir = exact_matcher_dir / "blocking_evaluation"

    # SimpleEvaluator subdirectories (for evaluation testing)
    eval_test_dir = simple_evaluator_dir / "evaluation"

    er_dir.mkdir(parents=True, exist_ok=True)
    dedup_dir.mkdir(parents=True, exist_ok=True)
    eval_dir.mkdir(parents=True, exist_ok=True)
    fuzzy_eval_dir.mkdir(parents=True, exist_ok=True)
    blocking_eval_dir.mkdir(parents=True, exist_ok=True)
    eval_test_dir.mkdir(parents=True, exist_ok=True)

    print("Generating ExactMatcher test data...")
    print("\n  Entity resolution (exotic scenarios)...")
    left_df, right_df, known_matches, match_types, left_match_info, right_match_info = generate_entity_resolution_data()

    # Save to data/ExactMatcher/entity_resolution/
    left_path = er_dir / "customers_a.parquet"
    right_path = er_dir / "customers_b.parquet"
    left_df.write_parquet(left_path)
    right_df.write_parquet(right_path)
    print(f"    Saved {left_df.height} records to {left_path}")
    print(f"    Saved {right_df.height} records to {right_path}")

    # Save ground truth
    save_ground_truth(left_df, right_df, known_matches, match_types, left_match_info, right_match_info, er_dir)

    print("\n  Deduplication...")
    dedup_df, duplicate_groups = generate_deduplication_data()

    # Save to data/ExactMatcher/deduplication/
    dedup_path = dedup_dir / "customers.parquet"
    dedup_df.write_parquet(dedup_path)
    print(f"    Saved {dedup_df.height} records to {dedup_path}")

    # Save ground truth
    save_deduplication_ground_truth(duplicate_groups, dedup_dir)

    print("\n  Evaluation (simple, stable)...")
    eval_left_df, eval_right_df = generate_evaluation_test_data()

    # Save to data/ExactMatcher/evaluation/
    eval_left_path = eval_dir / "customers_a.parquet"
    eval_right_path = eval_dir / "customers_b.parquet"
    eval_left_df.write_parquet(eval_left_path)
    eval_right_df.write_parquet(eval_right_path)
    print(f"    Saved {eval_left_df.height} records to {eval_left_path}")
    print(f"    Saved {eval_right_df.height} records to {eval_right_path}")

    print("\n  Fuzzy evaluation (name variation for fuzzy-matching tutorial)...")
    fuzzy_left_df, fuzzy_right_df = generate_fuzzy_evaluation_data()
    fuzzy_left_df.write_parquet(fuzzy_eval_dir / "customers_a.parquet")
    fuzzy_right_df.write_parquet(fuzzy_eval_dir / "customers_b.parquet")
    print(f"    Saved {fuzzy_left_df.height} + {fuzzy_right_df.height} records to {fuzzy_eval_dir}")
    save_evaluation_style_ground_truth(
        fuzzy_eval_dir,
        "Fuzzy Evaluation Ground Truth",
        [
            "- Pairs 1–15: identical on both sides (exact email finds them).\n",
            "- Pairs 16–30: same person, different emails; name variants on right (typos/spelling). "
            "Only fuzzy on full_name finds these.\n",
            "- Build ground truth in notebooks: left_id=eval_left_1..30, right_id=eval_right_1..30.\n",
        ],
    )

    print("\n  Blocking evaluation (split zip for blocking tutorial)...")
    block_left_df, block_right_df = generate_blocking_evaluation_data()
    block_left_df.write_parquet(blocking_eval_dir / "customers_a.parquet")
    block_right_df.write_parquet(blocking_eval_dir / "customers_b.parquet")
    print(f"    Saved {block_left_df.height} + {block_right_df.height} records to {blocking_eval_dir}")
    save_evaluation_style_ground_truth(
        blocking_eval_dir,
        "Blocking Evaluation Ground Truth",
        [
            "- Pairs 1–15: same zip on left and right (blocking on zip keeps them).\n",
            "- Pairs 16–30: same person (same email) but different zip; blocking on zip splits them (recall drops).\n",
            "- Build ground truth in notebooks: left_id=eval_left_1..30, right_id=eval_right_1..30.\n",
        ],
    )

    print("\nGenerating SimpleEvaluator test data...")
    print("\n  Evaluation test datasets...")
    # Copy evaluation data to SimpleEvaluator directory for evaluator-specific tests
    eval_left_df.write_parquet(eval_test_dir / "customers_a.parquet")
    eval_right_df.write_parquet(eval_test_dir / "customers_b.parquet")
    print(f"    Saved evaluation datasets to {eval_test_dir}")

    print("\n✅ All test datasets generated successfully!")
    print(f"\nExactMatcher - Entity Resolution:")
    print(f"  - {left_path} ({left_df.height} records)")
    print(f"  - {right_path} ({right_df.height} records)")
    print(f"  - 40 known matches (various rules)")
    print(f"\nExactMatcher - Deduplication:")
    print(f"  - {dedup_path} ({dedup_df.height} records)")
    print(f"  - 50 known duplicate pairs")
    print(f"\nExactMatcher - Evaluation:")
    print(f"  - {eval_left_path} ({eval_left_df.height} records)")
    print(f"  - {eval_right_path} ({eval_right_df.height} records)")
    print(f"  - 30 known matches (email only, perfect pairs)")
    print(f"\nExactMatcher - Fuzzy evaluation:")
    print(f"  - {fuzzy_eval_dir / 'customers_a.parquet'} ({fuzzy_left_df.height} records)")
    print(f"  - {fuzzy_eval_dir / 'customers_b.parquet'} ({fuzzy_right_df.height} records)")
    print(f"  - 30 known pairs (15 exact, 15 name-variant; for fuzzy tutorial)")
    print(f"\nExactMatcher - Blocking evaluation:")
    print(f"  - {blocking_eval_dir / 'customers_a.parquet'} ({block_left_df.height} records)")
    print(f"  - {blocking_eval_dir / 'customers_b.parquet'} ({block_right_df.height} records)")
    print(f"  - 30 known pairs (15 same zip, 15 split zip; for blocking tutorial)")
    print(f"\nSimpleEvaluator - Evaluation:")
    print(f"  - {eval_test_dir / 'customers_a.parquet'} ({eval_left_df.height} records)")
    print(f"  - {eval_test_dir / 'customers_b.parquet'} ({eval_right_df.height} records)")


if __name__ == "__main__":
    main()
