"""Generate tutorial datasets in memory (no disk I/O).

Used by tutorial_data.load and by scripts/generate_test_data.py when writing to data/.
"""

import polars as pl


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

    # Add non-matching records (nulls + messy values for tutorial cleaning section)
    # Pool of plausible names and streets so filler rows don't look generic
    filler_left_names = [
        ("Maria", "Garcia"), ("James", "Wilson"), ("Patricia", "Martinez"), ("Robert", "Anderson"),
        ("Jennifer", "Thomas"), ("Michael", "Taylor"), ("Linda", "Moore"), ("David", "Jackson"),
        ("Barbara", "Martin"), ("William", "Lee"), ("Elizabeth", "Perez"), ("Richard", "Thompson"),
        ("Susan", "White"), ("Joseph", "Harris"), ("Jessica", "Sanchez"), ("Charles", "Clark"),
        ("Sarah", "Ramirez"), ("Christopher", "Lewis"), ("Karen", "Robinson"), ("Daniel", "Walker"),
        ("Nancy", "Young"), ("Matthew", "Hall"), ("Betty", "Allen"), ("Anthony", "King"),
        ("Margaret", "Wright"), ("Mark", "Scott"), ("Sandra", "Torres"), ("Donald", "Nguyen"),
        ("Ashley", "Hill"), ("Steven", "Flores"), ("Kimberly", "Green"), ("Paul", "Adams"),
        ("Emily", "Nelson"), ("Andrew", "Baker"), ("Donna", "Rivera"), ("Joshua", "Campbell"),
        ("Michelle", "Mitchell"), ("Kenneth", "Carter"), ("Carol", "Roberts"), ("Kevin", "Gomez"),
    ]
    streets = ["Oak Ave", "Maple Dr", "Cedar Ln", "Elm St", "Pine Rd", "Birch Way", "Willow Pl", "Ash Blvd"]
    cities = ["Buffalo", "Riverside", "St. Petersburg", "Irvine", "Scottsdale", "Birmingham", "Chesapeake", "Norfolk", "Garland", "Hialeah"]
    states = ["NY", "CA", "FL", "TX", "AZ", "AL", "VA", "VA", "TX", "FL"]
    for i in range(500 - len(left_data)):
        city_idx = i % len(cities)
        first, last = filler_left_names[i % len(filler_left_names)]
        base_email = f"{first.lower()}.{last.lower()}.{i}@example.com"
        if i < 5:
            email = None
        elif i < 15:
            email = f"  {first.lower()}.{last.lower()}.{i}@EXAMPLE.com  "
        else:
            email = base_email
        first_display = f"  {first}{i}  " if 20 <= i < 25 else first
        left_data.append({
            "id": f"left_{left_idx+1}",
            "email": email,
            "first_name": first_display,
            "last_name": last,
            "address": f"{100 + (i % 997)} {streets[i % len(streets)]}",
            "city": cities[city_idx],
            "state": states[city_idx],
            "zip_code": None if i in (10, 11, 12) else f"{50000 + i}",
        })
        left_idx += 1

    # Generate right source (500 records)
    right_data = []
    right_match_info = []  # Track (right_idx, match_group, match_type)

    # Add right records (use different column names: email_address, postal_code — tutorial fixes in schema step)
    right_idx = 0
    for left_match, right_match, match_type in all_pairs:
        email, first, last, address, city, state, zip_code = right_match
        right_data.append({
            "id": f"right_{right_idx+1}",
            "email_address": email,
            "first_name": first,
            "last_name": last,
            "address": address,
            "city": city,
            "state": state,
            "postal_code": zip_code,
        })
        right_match_info.append((right_idx, len(all_pairs) - len([p for p in all_pairs if p[2] == match_type]) + sum(1 for p in all_pairs[:right_idx] if p[2] == match_type), match_type))
        right_idx += 1

    # Add non-matching records (nulls + messy values for tutorial cleaning section)
    filler_right_names = [
        ("Amanda", "Phillips"), ("Brian", "Evans"), ("Stephanie", "Turner"), ("George", "Diaz"),
        ("Melissa", "Parker"), ("Edward", "Collins"), ("Deborah", "Edwards"), ("Ronald", "Stewart"),
        ("Sharon", "Morris"), ("Timothy", "Murphy"), ("Cynthia", "Cook"), ("Jason", "Rogers"),
        ("Kathleen", "Morgan"), ("Jeffrey", "Peterson"), ("Amy", "Cooper"), ("Ryan", "Reed"),
        ("Angela", "Bailey"), ("Jacob", "Bell"), ("Brenda", "Gonzalez"), ("Gary", "Howard"),
        ("Pamela", "Ward"), ("Nicholas", "Cox"), ("Nicole", "Richardson"), ("Eric", "Wood"),
        ("Samantha", "Watson"), ("Stephen", "Brooks"), ("Katherine", "Bennett"), ("Jonathan", "Gray"),
        ("Christine", "James"), ("Larry", "Kelly"), ("Debra", "Sanders"), ("Frank", "Ross"),
        ("Rachel", "Price"), ("Scott", "Powell"), ("Catherine", "Long"), ("Raymond", "Patterson"),
        ("Carolyn", "Hughes"), ("Gregory", "Flores"), ("Janet", "Washington"), ("Samuel", "Butler"),
    ]
    streets_r = ["Park St", "Lake Rd", "View Dr", "Sunset Blvd", "Hill Ln", "Garden Way", "Mill St", "Spring Ave"]
    cities = ["Laredo", "Madison", "Durham", "Lubbock", "Winston-Salem", "Baton Rouge", "Grand Rapids", "Richmond", "Boise", "San Bernardino"]
    states = ["TX", "WI", "NC", "TX", "NC", "LA", "MI", "VA", "ID", "CA"]
    for i in range(500 - len(right_data)):
        city_idx = i % len(cities)
        first, last = filler_right_names[i % len(filler_right_names)]
        base_email = f"{first.lower()}.{last.lower()}.{i}@example.com"
        email = f"  {first.lower()}.{last.lower()}.{i}@Example.COM  " if 8 <= i < 18 else base_email
        first_name = None if i < 4 else (f"  {first}{i}  " if 15 <= i < 20 else first)
        right_data.append({
            "id": f"right_{right_idx+1}",
            "email_address": email,
            "first_name": first_name,
            "last_name": last,
            "address": f"{200 + (i % 991)} {streets_r[i % len(streets_r)]}",
            "city": cities[city_idx],
            "state": states[city_idx],
            "postal_code": None if i in (5, 6, 7) else f"{60000 + i}",
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


def generate_fuzzy_evaluation_data():
    """Generate evaluation-sized data with name variation for fuzzy-matching pedagogy.

    50 left, 50 right, 30 known pairs. First 15 pairs are identical (exact match on email).
    Next 15 pairs are same person but different emails and name variants on the right
    (typos, spelling variants) so they are only findable by fuzzy on full_name.
    Teaches: exact on email finds 15; fuzzy on name finds the rest.
    """
    left_data = []
    right_data = []

    # Pairs 1-15: identical on both sides (exact email finds them)
    for i in range(15):
        email = f"fuzzy_exact_{i}@example.com"
        first = f"Exact{i}"
        last = f"Person{i}"
        left_data.append({
            "id": f"eval_left_{i+1}",
            "email": email,
            "first_name": first,
            "last_name": last,
            "address": f"{i} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": f"{10000 + i}",
        })
        right_data.append({
            "id": f"eval_right_{i+1}",
            "email": email,
            "first_name": first,
            "last_name": last,
            "address": f"{i} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": f"{10000 + i}",
        })

    # Pairs 16-30: name variants on right, different emails (only fuzzy on name finds them)
    # (canonical_left, variant_right) for full_name similarity
    name_variants = [
        ("Katherine", "Jones", "Catherine", "Jones"),
        ("Steven", "Smith", "Stephen", "Smith"),
        ("Jon", "Doe", "John", "Doe"),
        ("Michael", "Brown", "Micheal", "Brown"),
        ("Margaret", "Wilson", "Margret", "Wilson"),
        ("Christopher", "Lee", "Cristopher", "Lee"),
        ("Elizabeth", "Taylor", "Elisabeth", "Taylor"),
        ("Catherine", "Davis", "Katharine", "Davis"),
        ("Nicholas", "Martinez", "Nicolas", "Martinez"),
        ("Anthony", "Garcia", "Antony", "Garcia"),
        ("Deborah", "Clark", "Debra", "Clark"),
        ("Jennifer", "Lewis", "Jenifer", "Lewis"),
        ("Matthew", "Walker", "Mathew", "Walker"),
        ("Timothy", "Hall", "Timoty", "Hall"),
        ("Patricia", "Young", "Patrica", "Young"),
    ]
    for idx, (f_left, l_left, f_right, l_right) in enumerate(name_variants):
        i = 15 + idx
        left_data.append({
            "id": f"eval_left_{i+1}",
            "email": f"fuzzy_left_{i}@example.com",
            "first_name": f_left,
            "last_name": l_left,
            "address": f"{i} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": f"{10000 + i}",
        })
        right_data.append({
            "id": f"eval_right_{i+1}",
            "email": f"fuzzy_right_{i}@example.com",
            "first_name": f_right,
            "last_name": l_right,
            "address": f"{i} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": f"{10000 + i}",
        })

    # Non-matching records (20 on each side)
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

    return pl.DataFrame(left_data), pl.DataFrame(right_data)
def generate_blocking_evaluation_data():
    """Generate evaluation-sized data where some true pairs have different zip codes.

    50 left, 50 right, 30 known pairs. First 15 pairs share the same zip (blocking
    keeps them). Next 15 pairs are same person (same email) but different zip on
    left vs right, so blocking on zip puts them in different blocks and recall drops.
    Teaches: no blocking finds 30; blocking on zip finds only 15 (recall 50%).
    """
    left_data = []
    right_data = []

    # Pairs 1-15: same zip on both sides (blocking on zip_code keeps them)
    for i in range(15):
        email = f"block_same_{i}@example.com"
        zip_code = f"{10000 + i}"
        left_data.append({
            "id": f"eval_left_{i+1}",
            "email": email,
            "first_name": f"SameZip{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": zip_code,
        })
        right_data.append({
            "id": f"eval_right_{i+1}",
            "email": email,
            "first_name": f"SameZip{i}",
            "last_name": f"Person{i}",
            "address": f"{i} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": zip_code,
        })

    # Pairs 16-30: same email/person but different zip (blocking on zip splits them)
    for i in range(15):
        j = 15 + i
        email = f"block_split_{i}@example.com"
        left_data.append({
            "id": f"eval_left_{j+1}",
            "email": email,
            "first_name": f"SplitZip{i}",
            "last_name": f"Person{i}",
            "address": f"{j} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": "20101",  # left in one block
        })
        right_data.append({
            "id": f"eval_right_{j+1}",
            "email": email,
            "first_name": f"SplitZip{i}",
            "last_name": f"Person{i}",
            "address": f"{j} Eval St",
            "city": "Test City",
            "state": "TS",
            "zip_code": "20202",  # right in different block
        })

    # Non-matching records (20 on each side)
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

    return pl.DataFrame(left_data), pl.DataFrame(right_data)


def generate_evaluation_test_data():
    """Generate simple datasets for evaluation tests.

    These are separate from the exotic test data - they have simple,
    stable ground truth that won't break when we add complexity.
    Perfect pairs (identical on email, same zip): good for exact matching
    and measurement loop; not for teaching blocking/fuzzy trade-offs.
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

