"""Generate tutorial datasets in memory (no disk/I/O).

Used by tutorial_data.load and by scripts/generate_test_data.py when writing to data/.

---

Data generation design (pedagogical order)

Think of the generator as building layers so each tutorial step has something to teach:

  1. Exact name matches   — True pairs with identical full_name on both sides.
                            Run A (full_name only) finds these.

  2. Email adds value     — True pairs that match on email but NOT on exact full_name
                            (e.g. same person, nickname on one side: William / Bill).
                            Run A misses them; Run B (full_name then email cascade) finds them.
                            So B recall > A recall.

  3. False positives      — Many rows share the same zip (small zip pool). When we add
                            full_name + zip_code cascade (Run C), we get more true pairs
                            but also many false pairs (non-matches with same zip).
                            So C has higher recall but much lower precision.

  4. Fuzzy name matches   — True pairs with name variants (Jon / Jonathan, Kate / Catherine).
                            Exact full_name misses; fuzzy on name (04) recovers them.

  5. Random mismatches    — Filler rows that do not match any true pair (nulls, messy values
                            for 01; rest are just other people).

We create exact-name pairs, then email-adds-value pairs, then fuzzy-name pairs (30 true pairs).
We then add 10 left + 10 right records from address_zip as **filler** (same first name, different
last name, same address+zip—different people, so not in ground truth). Then filler to 500 each.
Run C (full_name then zip) still sees zip matches and pulls in these false pairs.
"""

import polars as pl

# Small zip pool so many rows share a zip → zip cascade produces many false positives
_ZIP_POOL = ("90210", "10001", "60601", "75201", "85001")


def generate_entity_resolution_data():
    """Generate two datasets for the entity resolution tutorial (01–04+).

    Builds 30 true pairs: exact name (10) → email adds value (10) → fuzzy name (10).
    Then adds 10 left + 10 right from address_zip as filler (same first name, different last name
    at same address—different people, not in ground truth). Then filler to 500 each.
    """

    known_matches = []
    match_types = []

    # -------------------------------------------------------------------------
    # 1. Exact name matches (10 pairs)
    # Same full_name both sides, different emails. Run A (full_name only) finds these.
    # Different zips left/right so zip cascade does not find them.
    # -------------------------------------------------------------------------
    exact_name = [
        ("alice.smith@acme.org", "Alice", "Smith", "123 Main St", "New York", "NY", _ZIP_POOL[0], "exact_name"),
        ("alice.smith@globex.com", "Alice", "Smith", "456 Other Ave", "Boston", "MA", _ZIP_POOL[1], "exact_name"),
        ("bob.jones@acme.org", "Bob", "Jones", "789 Oak Rd", "Chicago", "IL", _ZIP_POOL[1], "exact_name"),
        ("bob.jones@globex.com", "Bob", "Jones", "101 Pine Ln", "Denver", "CO", _ZIP_POOL[2], "exact_name"),
        ("carol.white@acme.org", "Carol", "White", "202 Elm St", "Houston", "TX", _ZIP_POOL[2], "exact_name"),
        ("carol.white@globex.com", "Carol", "White", "303 Maple Dr", "Phoenix", "AZ", _ZIP_POOL[3], "exact_name"),
        ("dave.brown@acme.org", "Dave", "Brown", "404 Cedar Ct", "Philadelphia", "PA", _ZIP_POOL[3], "exact_name"),
        ("dave.brown@globex.com", "Dave", "Brown", "505 Birch Way", "San Antonio", "TX", _ZIP_POOL[4], "exact_name"),
        ("eva.garcia@acme.org", "Eva", "Garcia", "606 Willow Pl", "San Diego", "CA", _ZIP_POOL[4], "exact_name"),
        ("eva.garcia@globex.com", "Eva", "Garcia", "707 Ash Blvd", "Dallas", "TX", _ZIP_POOL[0], "exact_name"),
        ("frank.lee@acme.org", "Frank", "Lee", "808 Pine Rd", "San Jose", "CA", _ZIP_POOL[0], "exact_name"),
        ("frank.lee@globex.com", "Frank", "Lee", "909 Oak Ave", "Austin", "TX", _ZIP_POOL[1], "exact_name"),
        ("grace.kim@acme.org", "Grace", "Kim", "110 Cedar Ln", "Jacksonville", "FL", _ZIP_POOL[2], "exact_name"),
        ("grace.kim@globex.com", "Grace", "Kim", "211 Elm St", "Columbus", "OH", _ZIP_POOL[3], "exact_name"),
        ("henry.wong@acme.org", "Henry", "Wong", "312 Maple Dr", "Charlotte", "NC", _ZIP_POOL[3], "exact_name"),
        ("henry.wong@globex.com", "Henry", "Wong", "413 Birch Way", "Seattle", "WA", _ZIP_POOL[4], "exact_name"),
        ("iris.chen@acme.org", "Iris", "Chen", "514 Willow Pl", "Portland", "OR", _ZIP_POOL[4], "exact_name"),
        ("iris.chen@globex.com", "Iris", "Chen", "615 Ash Blvd", "Detroit", "MI", _ZIP_POOL[0], "exact_name"),
        ("jack.nelson@acme.org", "Jack", "Nelson", "716 Pine Rd", "Minneapolis", "MN", _ZIP_POOL[1], "exact_name"),
        ("jack.nelson@globex.com", "Jack", "Nelson", "817 Oak Ave", "Cleveland", "OH", _ZIP_POOL[2], "exact_name"),
    ]

    # -------------------------------------------------------------------------
    # 2. Email adds value (10 pairs)
    # Same email both sides, but name variant (William / Bill) so exact full_name
    # does NOT match. Run A misses; Run B (full_name then email cascade) finds them.
    # Same zip both sides so they could be found by zip cascade too.
    # -------------------------------------------------------------------------
    email_adds_value = [
        ("william.davis@acme.org", "William", "Davis", "100 A St", "Miami", "FL", _ZIP_POOL[0], "email_adds_value"),
        ("william.davis@acme.org", "Bill", "Davis", "200 B Ave", "Tampa", "FL", _ZIP_POOL[0], "email_adds_value"),
        ("margaret.taylor@acme.org", "Margaret", "Taylor", "301 C Rd", "Atlanta", "GA", _ZIP_POOL[1], "email_adds_value"),
        ("margaret.taylor@acme.org", "Maggie", "Taylor", "402 D Ln", "Raleigh", "NC", _ZIP_POOL[1], "email_adds_value"),
        ("robert.martinez@acme.org", "Robert", "Martinez", "503 E Ct", "Nashville", "TN", _ZIP_POOL[2], "email_adds_value"),
        ("robert.martinez@acme.org", "Bob", "Martinez", "604 F Way", "Memphis", "TN", _ZIP_POOL[2], "email_adds_value"),
        ("elizabeth.hall@acme.org", "Elizabeth", "Hall", "705 G Pl", "Louisville", "KY", _ZIP_POOL[3], "email_adds_value"),
        ("elizabeth.hall@acme.org", "Liz", "Hall", "806 H Blvd", "Baltimore", "MD", _ZIP_POOL[3], "email_adds_value"),
        ("james.wilson@acme.org", "James", "Wilson", "907 I St", "Milwaukee", "WI", _ZIP_POOL[4], "email_adds_value"),
        ("james.wilson@acme.org", "Jim", "Wilson", "108 J Ave", "Albuquerque", "NM", _ZIP_POOL[4], "email_adds_value"),
        ("patricia.clark@acme.org", "Patricia", "Clark", "209 K Rd", "Tucson", "AZ", _ZIP_POOL[0], "email_adds_value"),
        ("patricia.clark@acme.org", "Pat", "Clark", "310 L Dr", "Fresno", "CA", _ZIP_POOL[0], "email_adds_value"),
        ("daniel.lewis@acme.org", "Daniel", "Lewis", "411 M Ct", "Sacramento", "CA", _ZIP_POOL[1], "email_adds_value"),
        ("daniel.lewis@acme.org", "Dan", "Lewis", "512 N Way", "Kansas City", "MO", _ZIP_POOL[1], "email_adds_value"),
        ("christopher.lee@acme.org", "Christopher", "Lee", "613 O Pl", "Virginia Beach", "VA", _ZIP_POOL[2], "email_adds_value"),
        ("christopher.lee@acme.org", "Chris", "Lee", "714 P Blvd", "Omaha", "NE", _ZIP_POOL[2], "email_adds_value"),
        ("anthony.green@acme.org", "Anthony", "Green", "815 Q St", "Oakland", "CA", _ZIP_POOL[3], "email_adds_value"),
        ("anthony.green@acme.org", "Tony", "Green", "916 R Ave", "Mesa", "AZ", _ZIP_POOL[3], "email_adds_value"),
        ("jennifer.adams@acme.org", "Jennifer", "Adams", "117 S Rd", "Long Beach", "CA", _ZIP_POOL[4], "email_adds_value"),
        ("jennifer.adams@acme.org", "Jen", "Adams", "218 T Ln", "Colorado Springs", "CO", _ZIP_POOL[4], "email_adds_value"),
    ]

    # -------------------------------------------------------------------------
    # 3. Address+zip — false-positive driver (NOT true pairs: same first name, different last name = different people)
    # These 10 pairs are added as FILLER only (same address+zip, different names). full_name misses;
    # zip cascade finds them as candidates → they should count as false positives in ground truth.
    # -------------------------------------------------------------------------
    address_zip = [
        ("patricia.lane@acme.org", "Patricia", "Lane", "111 Multi St", "Austin", "TX", _ZIP_POOL[0], "address_zip"),
        ("p.different@globex.com", "Patricia", "Moss", "111 Multi St", "Austin", "TX", _ZIP_POOL[0], "address_zip"),
        ("quentin.reed@acme.org", "Quentin", "Reed", "333 Multi Rd", "Nashville", "TN", _ZIP_POOL[1], "address_zip"),
        ("q.other@globex.com", "Quentin", "Cole", "333 Multi Rd", "Nashville", "TN", _ZIP_POOL[1], "address_zip"),
        ("rachel.green@acme.org", "Rachel", "Green", "555 Multi Ln", "Virginia Beach", "VA", _ZIP_POOL[2], "address_zip"),
        ("r.blue@globex.com", "Rachel", "Blue", "555 Multi Ln", "Virginia Beach", "VA", _ZIP_POOL[2], "address_zip"),
        ("steve.marsh@acme.org", "Steve", "Marsh", "777 Multi Ct", "Milwaukee", "WI", _ZIP_POOL[3], "address_zip"),
        ("s.gates@globex.com", "Steve", "Gates", "777 Multi Ct", "Milwaukee", "WI", _ZIP_POOL[3], "address_zip"),
        ("tina.webb@acme.org", "Tina", "Webb", "999 Multi Blvd", "Albuquerque", "NM", _ZIP_POOL[4], "address_zip"),
        ("t.shaw@globex.com", "Tina", "Shaw", "999 Multi Blvd", "Albuquerque", "NM", _ZIP_POOL[4], "address_zip"),
        ("uma.clark@acme.org", "Uma", "Clark", "301 Multi St", "Tucson", "AZ", _ZIP_POOL[0], "address_zip"),
        ("u.different@globex.com", "Uma", "Ward", "301 Multi St", "Tucson", "AZ", _ZIP_POOL[0], "address_zip"),
        ("vera.hill@acme.org", "Vera", "Hill", "303 Multi Rd", "Fresno", "CA", _ZIP_POOL[1], "address_zip"),
        ("v.other@globex.com", "Vera", "Page", "303 Multi Rd", "Fresno", "CA", _ZIP_POOL[1], "address_zip"),
        ("will.davis@acme.org", "Will", "Davis", "305 Multi Ln", "Mesa", "AZ", _ZIP_POOL[2], "address_zip"),
        ("w.foster@globex.com", "Will", "Foster", "305 Multi Ln", "Mesa", "AZ", _ZIP_POOL[2], "address_zip"),
        ("xavier.lee@acme.org", "Xavier", "Lee", "307 Multi Ct", "Sacramento", "CA", _ZIP_POOL[3], "address_zip"),
        ("x.other@globex.com", "Xavier", "Bell", "307 Multi Ct", "Sacramento", "CA", _ZIP_POOL[3], "address_zip"),
        ("yara.moore@acme.org", "Yara", "Moore", "309 Multi Blvd", "Long Beach", "CA", _ZIP_POOL[4], "address_zip"),
        ("y.different@globex.com", "Yara", "Reed", "309 Multi Blvd", "Long Beach", "CA", _ZIP_POOL[4], "address_zip"),
    ]

    # -------------------------------------------------------------------------
    # 4. Fuzzy name matches (10 pairs)
    # Name variant (Jonathan / Jon), different emails. Exact full_name misses;
    # fuzzy on name (04) recovers. Different zips so zip cascade does not find them.
    # -------------------------------------------------------------------------
    fuzzy_name = [
        ("jonathan.smith@acme.org", "Jonathan", "Smith", "111 Name St", "Boston", "MA", _ZIP_POOL[0], "fuzzy_name"),
        ("j.smith@globex.com", "Jon", "Smith", "222 Name Ave", "Seattle", "WA", _ZIP_POOL[1], "fuzzy_name"),
        ("katherine.jones@acme.org", "Katherine", "Jones", "333 Name Rd", "Denver", "CO", _ZIP_POOL[2], "fuzzy_name"),
        ("catherine.jones@globex.com", "Catherine", "Jones", "444 Name Dr", "Portland", "OR", _ZIP_POOL[3], "fuzzy_name"),
        ("michael.brown@acme.org", "Michael", "Brown", "555 Name Ln", "Miami", "FL", _ZIP_POOL[4], "fuzzy_name"),
        ("mike.brown@globex.com", "Mike", "Brown", "666 Name Way", "Atlanta", "GA", _ZIP_POOL[0], "fuzzy_name"),
        ("robert.wilson@acme.org", "Robert", "Wilson", "777 Name Ct", "Detroit", "MI", _ZIP_POOL[1], "fuzzy_name"),
        ("r.wilson@globex.com", "R.", "Wilson", "888 Name Pl", "Minneapolis", "MN", _ZIP_POOL[2], "fuzzy_name"),
        ("steven.moore@acme.org", "Steven", "Moore", "999 Name Blvd", "Tampa", "FL", _ZIP_POOL[3], "fuzzy_name"),
        ("steven.moore@globex.com", "Steve", "Moore", "101 Name St", "Orlando", "FL", _ZIP_POOL[4], "fuzzy_name"),
        ("matthew.young@acme.org", "Matthew", "Young", "201 Name St", "Cleveland", "OH", _ZIP_POOL[0], "fuzzy_name"),
        ("matthew.young@globex.com", "Matt", "Young", "202 Name Ave", "Cincinnati", "OH", _ZIP_POOL[1], "fuzzy_name"),
        ("nicholas.scott@acme.org", "Nicholas", "Scott", "203 Name Rd", "Pittsburgh", "PA", _ZIP_POOL[2], "fuzzy_name"),
        ("nicholas.scott@globex.com", "Nick", "Scott", "204 Name Dr", "Raleigh", "NC", _ZIP_POOL[3], "fuzzy_name"),
        ("timothy.king@acme.org", "Timothy", "King", "205 Name Ln", "Oakland", "CA", _ZIP_POOL[4], "fuzzy_name"),
        ("timothy.king@globex.com", "Tim", "King", "206 Name Way", "Sacramento", "CA", _ZIP_POOL[0], "fuzzy_name"),
        ("deborah.wright@acme.org", "Deborah", "Wright", "207 Name Ct", "Kansas City", "MO", _ZIP_POOL[1], "fuzzy_name"),
        ("deborah.wright@globex.com", "Debra", "Wright", "208 Name Pl", "St. Louis", "MO", _ZIP_POOL[2], "fuzzy_name"),
        ("alexander.hill@acme.org", "Alexander", "Hill", "209 Name Blvd", "Columbus", "OH", _ZIP_POOL[3], "fuzzy_name"),
        ("alexander.hill@globex.com", "Alex", "Hill", "210 Name St", "Indianapolis", "IN", _ZIP_POOL[4], "fuzzy_name"),
    ]

    # Build all_pairs in pedagogical order (only TRUE pairs: 30 total)
    # Do NOT add address_zip to all_pairs — those are different people (same first name, different last name).
    all_pairs = []
    for i in range(0, len(exact_name), 2):
        l = exact_name[i]
        r = exact_name[i + 1]
        all_pairs.append(
            ((l[0], l[1], l[2], l[3], l[4], l[5], l[6]), (r[0], r[1], r[2], r[3], r[4], r[5], r[6]), "exact_name")
        )
    for i in range(0, len(email_adds_value), 2):
        l = email_adds_value[i]
        r = email_adds_value[i + 1]
        all_pairs.append(
            ((l[0], l[1], l[2], l[3], l[4], l[5], l[6]), (r[0], r[1], r[2], r[3], r[4], r[5], r[6]), "email_adds_value")
        )
    for i in range(0, len(fuzzy_name), 2):
        l = fuzzy_name[i]
        r = fuzzy_name[i + 1]
        all_pairs.append(
            ((l[0], l[1], l[2], l[3], l[4], l[5], l[6]), (r[0], r[1], r[2], r[3], r[4], r[5], r[6]), "fuzzy_name")
        )

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

    # Add address_zip rows as FILLER (same first name, different last name = different people, not in ground truth)
    for i in range(0, len(address_zip), 2):
        email, first, last, address, city, state, zip_code = address_zip[i][0], address_zip[i][1], address_zip[i][2], address_zip[i][3], address_zip[i][4], address_zip[i][5], address_zip[i][6]
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
        base_email = f"{first.lower()}.{last.lower()}.{i}@acme.org"
        if i < 5:
            email = None
        elif i < 15:
            email = f"  {first.lower()}.{last.lower()}.{i}@ACME.ORG  "
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
            "zip_code": None if i in (10, 11, 12) else _ZIP_POOL[i % len(_ZIP_POOL)],
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

    # Add address_zip rows as FILLER (right side: different people, not in ground truth)
    for i in range(1, len(address_zip), 2):
        email, first, last, address, city, state, zip_code = address_zip[i][0], address_zip[i][1], address_zip[i][2], address_zip[i][3], address_zip[i][4], address_zip[i][5], address_zip[i][6]
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
        base_email = f"{first.lower()}.{last.lower()}.{i}@globex.com"
        email = f"  {first.lower()}.{last.lower()}.{i}@GLOBEX.COM  " if 8 <= i < 18 else base_email
        first_name = None if i < 4 else (f"  {first}{i}  " if 15 <= i < 20 else first)
        right_data.append({
            "id": f"right_{right_idx+1}",
            "email_address": email,
            "first_name": first_name,
            "last_name": last,
            "address": f"{200 + (i % 991)} {streets_r[i % len(streets_r)]}",
            "city": cities[city_idx],
            "state": states[city_idx],
            "postal_code": None if i in (5, 6, 7) else _ZIP_POOL[i % len(_ZIP_POOL)],
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

