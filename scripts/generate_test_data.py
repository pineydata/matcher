"""Generate test datasets for matcher validation.

Creates datasets with known matches for testing Phase 1 exact matching.
"""

import polars as pl
from pathlib import Path


def generate_entity_resolution_data():
    """Generate two datasets with 30 known matches for entity resolution."""

    # Known matches (30 pairs)
    known_matches = [
        # Exact matches (20)
        ("alice.smith@example.com", "Alice", "Smith", "123 Main St", "10001"),
        ("bob.jones@example.com", "Bob", "Jones", "456 Oak Ave", "10002"),
        ("charlie.brown@example.com", "Charlie", "Brown", "789 Pine Rd", "10003"),
        ("diana.prince@example.com", "Diana", "Prince", "321 Elm St", "10004"),
        ("edward.norton@example.com", "Edward", "Norton", "654 Maple Dr", "10005"),
        ("fiona.apple@example.com", "Fiona", "Apple", "987 Cedar Ln", "10006"),
        ("george.washington@example.com", "George", "Washington", "147 Birch Way", "10007"),
        ("helen.troy@example.com", "Helen", "Troy", "258 Spruce Ct", "10008"),
        ("isaac.newton@example.com", "Isaac", "Newton", "369 Willow Pl", "10009"),
        ("jane.doe@example.com", "Jane", "Doe", "741 Ash Blvd", "10010"),
        ("kevin.bacon@example.com", "Kevin", "Bacon", "852 Poplar St", "10011"),
        ("lisa.simpson@example.com", "Lisa", "Simpson", "963 Hickory Ave", "10012"),
        ("michael.jackson@example.com", "Michael", "Jackson", "159 Cherry Rd", "10013"),
        ("nancy.drew@example.com", "Nancy", "Drew", "357 Walnut Dr", "10014"),
        ("oscar.wilde@example.com", "Oscar", "Wilde", "468 Chestnut Ln", "10015"),
        ("patricia.highsmith@example.com", "Patricia", "Highsmith", "579 Sycamore Way", "10016"),
        ("quentin.tarantino@example.com", "Quentin", "Tarantino", "680 Magnolia Ct", "10017"),
        ("rachel.green@example.com", "Rachel", "Green", "791 Dogwood Pl", "10018"),
        ("steve.jobs@example.com", "Steve", "Jobs", "802 Redwood Blvd", "10019"),
        ("tina.fey@example.com", "Tina", "Fey", "913 Sequoia St", "10020"),

        # Near-duplicates that should match on email (5)
        ("unique1@example.com", "Unique", "One", "111 First St", "20001"),
        ("unique2@example.com", "Unique", "Two", "222 Second Ave", "20002"),
        ("unique3@example.com", "Unique", "Three", "333 Third Rd", "20003"),
        ("unique4@example.com", "Unique", "Four", "444 Fourth Dr", "20004"),
        ("unique5@example.com", "Unique", "Five", "555 Fifth Ln", "20005"),

        # Partial matches - same email, different other fields (5)
        ("partial1@example.com", "Partial", "One", "999 Partial St", "30001"),
        ("partial2@example.com", "Partial", "Two", "888 Partial Ave", "30002"),
        ("partial3@example.com", "Partial", "Three", "777 Partial Rd", "30003"),
        ("partial4@example.com", "Partial", "Four", "666 Partial Dr", "30004"),
        ("partial5@example.com", "Partial", "Five", "555 Partial Ln", "30005"),
    ]

    # Generate left source (500 records)
    left_data = []
    left_ids = []

    # Add the 30 known matches
    for i, (email, first, last, address, zip_code) in enumerate(known_matches):
        left_data.append({
            "id": f"left_{i+1}",
            "email": email,
            "first_name": first,
            "last_name": last,
            "address": address,
            "zip_code": zip_code,
        })
        left_ids.append(f"left_{i+1}")

    # Add 470 non-matching records
    for i in range(470):
        left_data.append({
            "id": f"left_{i+31}",
            "email": f"leftonly_{i}@example.com",
            "first_name": f"LeftOnly{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Left Street",
            "zip_code": f"{40000 + i}",
        })

    # Generate right source (500 records)
    right_data = []
    right_ids = []

    # Add the 30 known matches (same emails, might have different other fields)
    for i, (email, first, last, address, zip_code) in enumerate(known_matches):
        right_data.append({
            "id": f"right_{i+1}",
            "email": email,  # Same email = match
            "first_name": first,  # Might differ in real scenarios
            "last_name": last,
            "address": address,
            "zip_code": zip_code,
        })
        right_ids.append(f"right_{i+1}")

    # Add 470 non-matching records
    for i in range(470):
        right_data.append({
            "id": f"right_{i+31}",
            "email": f"rightonly_{i}@example.com",
            "first_name": f"RightOnly{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Right Street",
            "zip_code": f"{50000 + i}",
        })

    left_df = pl.DataFrame(left_data)
    right_df = pl.DataFrame(right_data)

    return left_df, right_df, known_matches


def generate_deduplication_data():
    """Generate single dataset with 50 known duplicates for deduplication."""

    # Known duplicate pairs (50 pairs = 100 records, but some might be in groups)
    # For simplicity, create 50 pairs where each pair has same email

    duplicate_groups = []
    all_data = []
    record_id = 1

    # Create 50 duplicate pairs (100 records total)
    for i in range(50):
        email = f"duplicate_{i}@example.com"
        group = []

        # First record
        all_data.append({
            "id": f"dup_{record_id}",
            "email": email,
            "first_name": f"Duplicate{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Duplicate St",
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
            "zip_code": f"{60000 + i}",
        })
        group.append(f"dup_{record_id}")
        record_id += 1

        duplicate_groups.append(group)

    # Add 900 unique records (no duplicates)
    for i in range(900):
        all_data.append({
            "id": f"unique_{i+1}",
            "email": f"unique_{i}@example.com",
            "first_name": f"Unique{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Unique Street",
            "zip_code": f"{70000 + i}",
        })

    df = pl.DataFrame(all_data)

    return df, duplicate_groups


def save_ground_truth(left_df, right_df, known_matches, output_dir):
    """Save ground truth documentation."""

    ground_truth = []
    ground_truth.append("# Entity Resolution Ground Truth\n")
    ground_truth.append(f"Total known matches: {len(known_matches)}\n\n")
    ground_truth.append("## Known Matches (by email)\n\n")
    ground_truth.append("| Left ID | Right ID | Email |\n")
    ground_truth.append("|---------|----------|-------|\n")

    for i, (email, _, _, _, _) in enumerate(known_matches):
        left_id = f"left_{i+1}"
        right_id = f"right_{i+1}"
        ground_truth.append(f"| {left_id} | {right_id} | {email} |\n")

    ground_truth.append("\n## Expected Results\n\n")
    ground_truth.append("- **Total matches**: 30\n")
    ground_truth.append("- **Match field**: email\n")
    ground_truth.append("- **Match type**: exact\n")
    ground_truth.append("- **All matches should be found** (100% recall)\n")
    ground_truth.append("- **No false positives** (100% precision)\n")

    output_path = output_dir / "ground_truth_entity_resolution.md"
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

    output_path = output_dir / "ground_truth_deduplication.md"
    output_path.write_text("".join(ground_truth))
    print(f"Saved ground truth to {output_path}")


def main():
    """Generate all test datasets."""

    # Create output directory
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    print("Generating entity resolution test data...")
    left_df, right_df, known_matches = generate_entity_resolution_data()

    # Save to data/
    left_path = data_dir / "customers_a.parquet"
    right_path = data_dir / "customers_b.parquet"
    left_df.write_parquet(left_path)
    right_df.write_parquet(right_path)
    print(f"Saved {left_df.height} records to {left_path}")
    print(f"Saved {right_df.height} records to {right_path}")

    # Save ground truth
    save_ground_truth(left_df, right_df, known_matches, data_dir)

    print("\nGenerating deduplication test data...")
    dedup_df, duplicate_groups = generate_deduplication_data()

    # Save to data/
    dedup_path = data_dir / "customers.parquet"
    dedup_df.write_parquet(dedup_path)
    print(f"Saved {dedup_df.height} records to {dedup_path}")

    # Save ground truth
    save_deduplication_ground_truth(duplicate_groups, data_dir)

    print("\n✅ All test datasets generated successfully!")
    print(f"\nEntity Resolution:")
    print(f"  - Left source: {left_path} ({left_df.height} records)")
    print(f"  - Right source: {right_path} ({right_df.height} records)")
    print(f"  - Known matches: {len(known_matches)}")
    print(f"\nDeduplication:")
    print(f"  - Source: {dedup_path} ({dedup_df.height} records)")
    print(f"  - Known duplicate pairs: {len(duplicate_groups)}")


if __name__ == "__main__":
    main()
