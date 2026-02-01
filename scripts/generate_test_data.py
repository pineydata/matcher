"""Generate test datasets for matcher validation.

Creates datasets with known matches for testing Phase 1 exact matching.
"""

import polars as pl
from pathlib import Path


def generate_entity_resolution_data():
    """Generate two datasets with exotic matching scenarios for entity resolution.

    Creates matches that test different rules:
    - Email-only matches (10)
    - Name-only matches (first_name + last_name, different emails) (10)
    - Multi-field matches (address + zip_code together) (10)
    - Mixed scenarios (can match on multiple rules) (10)
    """

    known_matches = []
    match_types = []  # Track match type for ground truth

    # 1. Email-only matches (10)
    # These match ONLY on email - names/addresses differ
    email_only = [
        ("alice.smith@example.com", "Alice", "Smith", "123 Main St", "New York", "NY", "10001", "email"),
        ("bob.jones@example.com", "Bob", "Jones", "456 Oak Ave", "Los Angeles", "CA", "10002", "email"),
        ("charlie.brown@example.com", "Charlie", "Brown", "789 Pine Rd", "Chicago", "IL", "10003", "email"),
        ("diana.prince@example.com", "Diana", "Prince", "321 Elm St", "Houston", "TX", "10004", "email"),
        ("edward.norton@example.com", "Edward", "Norton", "654 Maple Dr", "Phoenix", "AZ", "10005", "email"),
        ("fiona.apple@example.com", "Fiona", "Apple", "987 Cedar Ln", "Philadelphia", "PA", "10006", "email"),
        ("george.washington@example.com", "George", "Washington", "147 Birch Way", "San Antonio", "TX", "10007", "email"),
        ("helen.troy@example.com", "Helen", "Troy", "258 Spruce Ct", "San Diego", "CA", "10008", "email"),
        ("isaac.newton@example.com", "Isaac", "Newton", "369 Willow Pl", "Dallas", "TX", "10009", "email"),
        ("jane.doe@example.com", "Jane", "Doe", "741 Ash Blvd", "San Jose", "CA", "10010", "email"),
    ]

    # 2. Name-only matches (10 pairs = 20 records)
    # These match ONLY on first_name + last_name - emails differ
    name_only = [
        ("name1.left@example.com", "Kevin", "Bacon", "111 Name St", "Boston", "MA", "20001", "name"),
        ("name1.right@example.com", "Kevin", "Bacon", "222 Name Ave", "Seattle", "WA", "20002", "name"),
        ("name2.left@example.com", "Lisa", "Simpson", "333 Name Rd", "Denver", "CO", "20003", "name"),
        ("name2.right@example.com", "Lisa", "Simpson", "444 Name Dr", "Portland", "OR", "20004", "name"),
        ("name3.left@example.com", "Michael", "Jackson", "555 Name Ln", "Miami", "FL", "20005", "name"),
        ("name3.right@example.com", "Michael", "Jackson", "666 Name Way", "Atlanta", "GA", "20006", "name"),
        ("name4.left@example.com", "Nancy", "Drew", "777 Name Ct", "Detroit", "MI", "20007", "name"),
        ("name4.right@example.com", "Nancy", "Drew", "888 Name Pl", "Minneapolis", "MN", "20008", "name"),
        ("name5.left@example.com", "Oscar", "Wilde", "999 Name Blvd", "Tampa", "FL", "20009", "name"),
        ("name5.right@example.com", "Oscar", "Wilde", "101 Name St", "Orlando", "FL", "20010", "name"),
        ("name6.left@example.com", "Penelope", "Cruz", "201 Name St", "Cleveland", "OH", "20011", "name"),
        ("name6.right@example.com", "Penelope", "Cruz", "202 Name Ave", "Cincinnati", "OH", "20012", "name"),
        ("name7.left@example.com", "Quentin", "Tarantino", "203 Name Rd", "Pittsburgh", "PA", "20013", "name"),
        ("name7.right@example.com", "Quentin", "Tarantino", "204 Name Dr", "Raleigh", "NC", "20014", "name"),
        ("name8.left@example.com", "Rachel", "Weisz", "205 Name Ln", "Oakland", "CA", "20015", "name"),
        ("name8.right@example.com", "Rachel", "Weisz", "206 Name Way", "Sacramento", "CA", "20016", "name"),
        ("name9.left@example.com", "Steve", "Martin", "207 Name Ct", "Kansas City", "MO", "20017", "name"),
        ("name9.right@example.com", "Steve", "Martin", "208 Name Pl", "St. Louis", "MO", "20018", "name"),
        ("name10.left@example.com", "Tina", "Turner", "209 Name Blvd", "Columbus", "OH", "20019", "name"),
        ("name10.right@example.com", "Tina", "Turner", "210 Name St", "Indianapolis", "IN", "20020", "name"),
    ]

    # 3. Multi-field matches (10 pairs = 20 records)
    # These require address + zip_code together - names/emails differ
    multi_field = [
        ("multi1.left@example.com", "Patricia", "Highsmith", "111 Multi St", "Austin", "TX", "30001", "address+zip"),
        ("multi1.right@example.com", "Patricia", "Different", "111 Multi St", "Austin", "TX", "30001", "address+zip"),
        ("multi2.left@example.com", "Quentin", "Tarantino", "333 Multi Rd", "Nashville", "TN", "30002", "address+zip"),
        ("multi2.right@example.com", "Quentin", "Other", "333 Multi Rd", "Nashville", "TN", "30002", "address+zip"),
        ("multi3.left@example.com", "Rachel", "Green", "555 Multi Ln", "Virginia Beach", "VA", "30003", "address+zip"),
        ("multi3.right@example.com", "Rachel", "Blue", "555 Multi Ln", "Virginia Beach", "VA", "30003", "address+zip"),
        ("multi4.left@example.com", "Steve", "Jobs", "777 Multi Ct", "Milwaukee", "WI", "30004", "address+zip"),
        ("multi4.right@example.com", "Steve", "Gates", "777 Multi Ct", "Milwaukee", "WI", "30004", "address+zip"),
        ("multi5.left@example.com", "Tina", "Fey", "999 Multi Blvd", "Albuquerque", "NM", "30005", "address+zip"),
        ("multi5.right@example.com", "Tina", "Poehler", "999 Multi Blvd", "Albuquerque", "NM", "30005", "address+zip"),
        ("multi6.left@example.com", "Uma", "Thurman", "301 Multi St", "Tucson", "AZ", "30006", "address+zip"),
        ("multi6.right@example.com", "Uma", "Different", "301 Multi St", "Tucson", "AZ", "30006", "address+zip"),
        ("multi7.left@example.com", "Vera", "Farmiga", "303 Multi Rd", "Fresno", "CA", "30007", "address+zip"),
        ("multi7.right@example.com", "Vera", "Other", "303 Multi Rd", "Fresno", "CA", "30007", "address+zip"),
        ("multi8.left@example.com", "Will", "Smith", "305 Multi Ln", "Mesa", "AZ", "30008", "address+zip"),
        ("multi8.right@example.com", "Will", "Ferrell", "305 Multi Ln", "Mesa", "AZ", "30008", "address+zip"),
        ("multi9.left@example.com", "Xavier", "Dolan", "307 Multi Ct", "Sacramento", "CA", "30009", "address+zip"),
        ("multi9.right@example.com", "Xavier", "Other", "307 Multi Ct", "Sacramento", "CA", "30009", "address+zip"),
        ("multi10.left@example.com", "Yara", "Shahidi", "309 Multi Blvd", "Long Beach", "CA", "30010", "address+zip"),
        ("multi10.right@example.com", "Yara", "Different", "309 Multi Blvd", "Long Beach", "CA", "30010", "address+zip"),
    ]

    # 4. Mixed scenarios (10 pairs = 20 records)
    # These can match on multiple rules (email OR name) - same email AND same name
    mixed = [
        ("mixed1@example.com", "Emma", "Watson", "111 Mixed St", "Kansas City", "KS", "40001", "email_or_name"),
        ("mixed1@example.com", "Emma", "Watson", "222 Mixed Ave", "Omaha", "NE", "40002", "email_or_name"),
        ("mixed2@example.com", "Daniel", "Radcliffe", "333 Mixed Rd", "Miami", "FL", "40003", "email_or_name"),
        ("mixed2@example.com", "Daniel", "Radcliffe", "444 Mixed Dr", "Oakland", "CA", "40004", "email_or_name"),
        ("mixed3@example.com", "Rupert", "Grint", "555 Mixed Ln", "Raleigh", "NC", "40005", "email_or_name"),
        ("mixed3@example.com", "Rupert", "Grint", "666 Mixed Way", "Minneapolis", "MN", "40006", "email_or_name"),
        ("mixed4@example.com", "Tom", "Hanks", "777 Mixed Ct", "Cleveland", "OH", "40007", "email_or_name"),
        ("mixed4@example.com", "Tom", "Hanks", "888 Mixed Pl", "Tulsa", "OK", "40008", "email_or_name"),
        ("mixed5@example.com", "Meryl", "Streep", "999 Mixed Blvd", "Arlington", "TX", "40009", "email_or_name"),
        ("mixed5@example.com", "Meryl", "Streep", "101 Mixed St", "Tampa", "FL", "40010", "email_or_name"),
        ("mixed6@example.com", "Natalie", "Portman", "401 Mixed St", "New Orleans", "LA", "40011", "email_or_name"),
        ("mixed6@example.com", "Natalie", "Portman", "402 Mixed Ave", "Wichita", "KS", "40012", "email_or_name"),
        ("mixed7@example.com", "Olivia", "Wilde", "403 Mixed Rd", "Cincinnati", "OH", "40013", "email_or_name"),
        ("mixed7@example.com", "Olivia", "Wilde", "404 Mixed Dr", "St. Paul", "MN", "40014", "email_or_name"),
        ("mixed8@example.com", "Paul", "Rudd", "405 Mixed Ln", "Toledo", "OH", "40015", "email_or_name"),
        ("mixed8@example.com", "Paul", "Rudd", "406 Mixed Way", "Greensboro", "NC", "40016", "email_or_name"),
        ("mixed9@example.com", "Ryan", "Reynolds", "407 Mixed Ct", "Newark", "NJ", "40017", "email_or_name"),
        ("mixed9@example.com", "Ryan", "Reynolds", "408 Mixed Pl", "Plano", "TX", "40018", "email_or_name"),
        ("mixed10@example.com", "Zoe", "Saldana", "409 Mixed Blvd", "Henderson", "NV", "40019", "email_or_name"),
        ("mixed10@example.com", "Zoe", "Saldana", "410 Mixed St", "Lincoln", "NE", "40020", "email_or_name"),
    ]

    # Create proper pairs for each match type
    all_pairs = []

    # Email-only: 10 pairs (same record on both sides)
    for match in email_only:
        email, first, last, address, city, state, zip_code, _ = match
        all_pairs.append(((email, first, last, address, city, state, zip_code),
                         (email, first, last, address, city, state, zip_code), "email"))

    # Name-only: 10 pairs (different emails, same names)
    for i in range(0, len(name_only), 2):
        left_email, left_first, left_last, left_addr, left_city, left_state, left_zip, _ = name_only[i]
        right_email, right_first, right_last, right_addr, right_city, right_state, right_zip, _ = name_only[i+1]
        all_pairs.append(((left_email, left_first, left_last, left_addr, left_city, left_state, left_zip),
                         (right_email, right_first, right_last, right_addr, right_city, right_state, right_zip), "name"))

    # Multi-field: 10 pairs (same address+zip, different names/emails)
    for i in range(0, len(multi_field), 2):
        left_email, left_first, left_last, left_addr, left_city, left_state, left_zip, _ = multi_field[i]
        right_email, right_first, right_last, right_addr, right_city, right_state, right_zip, _ = multi_field[i+1]
        all_pairs.append(((left_email, left_first, left_last, left_addr, left_city, left_state, left_zip),
                         (right_email, right_first, right_last, right_addr, right_city, right_state, right_zip), "address+zip"))

    # Mixed: 10 pairs (same email AND same name - can match on either)
    for i in range(0, len(mixed), 2):
        left_email, left_first, left_last, left_addr, left_city, left_state, left_zip, _ = mixed[i]
        right_email, right_first, right_last, right_addr, right_city, right_state, right_zip, _ = mixed[i+1]
        all_pairs.append(((left_email, left_first, left_last, left_addr, left_city, left_state, left_zip),
                         (right_email, right_first, right_last, right_addr, right_city, right_state, right_zip), "email_or_name"))

    # Generate left source (500 records)
    left_data = []
    left_match_info = []  # Track (left_idx, match_group, match_type)

    # Add left records
    left_idx = 0
    for left_match, right_match, match_type in all_pairs:
        email, first, last, address, city, state, zip_code = left_match
        left_data.append({
            "id": f"left_{left_idx+1}",
            "email": email,
            "first_name": first,
            "last_name": last,
            "address": address,
            "city": city,
            "state": state,
            "zip_code": zip_code,
        })
        left_match_info.append((left_idx, len(all_pairs) - len([p for p in all_pairs if p[2] == match_type]) + sum(1 for p in all_pairs[:left_idx] if p[2] == match_type), match_type))
        left_idx += 1

    # Add non-matching records
    cities = ["Buffalo", "Riverside", "St. Petersburg", "Irvine", "Scottsdale", "Birmingham", "Chesapeake", "Norfolk", "Garland", "Hialeah"]
    states = ["NY", "CA", "FL", "TX", "AZ", "AL", "VA", "VA", "TX", "FL"]
    for i in range(500 - len(left_data)):
        city_idx = i % len(cities)
        left_data.append({
            "id": f"left_{left_idx+1}",
            "email": f"leftonly_{i}@example.com",
            "first_name": f"LeftOnly{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Left Street",
            "city": cities[city_idx],
            "state": states[city_idx],
            "zip_code": f"{50000 + i}",
        })
        left_idx += 1

    # Generate right source (500 records)
    right_data = []
    right_match_info = []  # Track (right_idx, match_group, match_type)

    # Add right records
    right_idx = 0
    for left_match, right_match, match_type in all_pairs:
        email, first, last, address, city, state, zip_code = right_match
        right_data.append({
            "id": f"right_{right_idx+1}",
            "email": email,
            "first_name": first,
            "last_name": last,
            "address": address,
            "city": city,
            "state": state,
            "zip_code": zip_code,
        })
        right_match_info.append((right_idx, len(all_pairs) - len([p for p in all_pairs if p[2] == match_type]) + sum(1 for p in all_pairs[:right_idx] if p[2] == match_type), match_type))
        right_idx += 1

    # Add non-matching records
    cities = ["Laredo", "Madison", "Durham", "Lubbock", "Winston-Salem", "Baton Rouge", "Grand Rapids", "Richmond", "Boise", "San Bernardino"]
    states = ["TX", "WI", "NC", "TX", "NC", "LA", "MI", "VA", "ID", "CA"]
    for i in range(500 - len(right_data)):
        city_idx = i % len(cities)
        right_data.append({
            "id": f"right_{right_idx+1}",
            "email": f"rightonly_{i}@example.com",
            "first_name": f"RightOnly{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Right Street",
            "city": cities[city_idx],
            "state": states[city_idx],
            "zip_code": f"{60000 + i}",
        })
        right_idx += 1

    left_df = pl.DataFrame(left_data)
    right_df = pl.DataFrame(right_data)

    return left_df, right_df, known_matches, match_types, left_match_info, right_match_info


def generate_deduplication_data():
    """Generate single dataset with 50 known duplicates for deduplication."""

    # Known duplicate pairs (50 pairs = 100 records, but some might be in groups)
    # For simplicity, create 50 pairs where each pair has same email

    duplicate_groups = []
    all_data = []
    record_id = 1

    # Create 50 duplicate pairs (100 records total)
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
    for i in range(50):
        email = f"duplicate_{i}@example.com"
        group = []
        city = cities[i % len(cities)]
        state = states[i % len(states)]

        # First record
        all_data.append({
            "id": f"dup_{record_id}",
            "email": email,
            "first_name": f"Duplicate{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Duplicate St",
            "city": city,
            "state": state,
            "zip_code": f"{60000 + i}",
        })
        group.append(f"dup_{record_id}")
        record_id += 1

        # Second record (duplicate)
        all_data.append({
            "id": f"dup_{record_id}",
            "email": email,  # Same email = duplicate
            "first_name": f"Duplicate{i}",  # Might have slight variations
            "last_name": f"Person{i}",
            "address": f"{i} Duplicate St",
            "city": city,
            "state": state,
            "zip_code": f"{60000 + i}",
        })
        group.append(f"dup_{record_id}")
        record_id += 1

        duplicate_groups.append(group)

    # Add 900 unique records (no duplicates)
    cities = ["Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington"]
    states = ["TX", "FL", "TX", "OH", "NC", "CA", "IN", "WA", "CO", "DC"]
    for i in range(900):
        city_idx = i % len(cities)
        all_data.append({
            "id": f"unique_{i+1}",
            "email": f"unique_{i}@example.com",
            "first_name": f"Unique{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Unique Street",
            "city": cities[city_idx],
            "state": states[city_idx],
            "zip_code": f"{70000 + i}",
        })

    df = pl.DataFrame(all_data)

    return df, duplicate_groups


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
    ground_truth.append("results = matcher.match(rules=\"email\")\n")
    ground_truth.append("# Should find: 10 email-only + 10 mixed = 20 matches\n\n")
    ground_truth.append("# Name-only matches\n")
    ground_truth.append("results = matcher.match(rules=[\"first_name\", \"last_name\"])\n")
    ground_truth.append("# Should find: 10 name-only + 10 mixed = 20 matches\n\n")
    ground_truth.append("# Multi-field matches\n")
    ground_truth.append("results = matcher.match(rules=[\"address\", \"zip_code\"])\n")
    ground_truth.append("# Should find: 10 multi-field matches\n\n")
    ground_truth.append("# Multiple rules (OR logic)\n")
    ground_truth.append("results = matcher.match(rules=[\"email\", [\"first_name\", \"last_name\"]])\n")
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


def generate_evaluation_test_data():
    """Generate simple datasets for evaluation tests.

    These are separate from the exotic test data - they have simple,
    stable ground truth that won't break when we add complexity.
    """
    # Simple entity resolution: 30 exact email matches
    left_data = []
    right_data = []

    for i in range(30):
        email = f"eval_{i}@example.com"
        left_data.append({
            "id": f"eval_left_{i+1}",
            "email": email,
            "first_name": f"Eval{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": f"{10000 + i}",
        })
        right_data.append({
            "id": f"eval_right_{i+1}",
            "email": email,  # Same email = match
            "first_name": f"Eval{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": f"{10000 + i}",
        })

    # Add some non-matching records
    for i in range(20):
        left_data.append({
            "id": f"eval_left_{i+31}",
            "email": f"leftonly_{i}@example.com",
            "first_name": f"LeftOnly{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Left St",
            "city": "Left City",
            "state": "LC",
            "zip_code": f"{20000 + i}",
        })
        right_data.append({
            "id": f"eval_right_{i+31}",
            "email": f"rightonly_{i}@example.com",
            "first_name": f"RightOnly{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Right St",
            "city": "Right City",
            "state": "RC",
            "zip_code": f"{30000 + i}",
        })

    left_df = pl.DataFrame(left_data)
    right_df = pl.DataFrame(right_data)

    return left_df, right_df


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

    # SimpleEvaluator subdirectories (for evaluation testing)
    eval_test_dir = simple_evaluator_dir / "evaluation"

    er_dir.mkdir(parents=True, exist_ok=True)
    dedup_dir.mkdir(parents=True, exist_ok=True)
    eval_dir.mkdir(parents=True, exist_ok=True)
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
    print(f"  - 30 known matches (email only)")
    print(f"\nSimpleEvaluator - Evaluation:")
    print(f"  - {eval_test_dir / 'customers_a.parquet'} ({eval_left_df.height} records)")
    print(f"  - {eval_test_dir / 'customers_b.parquet'} ({eval_right_df.height} records)")


if __name__ == "__main__":
    main()
