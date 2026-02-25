# Entity Resolution Ground Truth
Total known matches: 40

## Match Types

- **Email-only matches** (10): Match only on `email` field
- **Name-only matches** (10): Match only on `first_name + last_name` (emails differ)
- **Multi-field matches** (10): Match on `email + zip_code` together (names differ)
- **Mixed matches** (10): Can match on `email` OR `first_name + last_name`

## Known Matches

| Left ID | Right ID | Match Type | Rule(s) |
|---------|----------|------------|----------|
| left_1 | right_1 | email | email |
| left_2 | right_2 | email | email |
| left_3 | right_3 | email | email |
| left_4 | right_4 | email | email |
| left_5 | right_5 | email | email |
| left_6 | right_6 | email | email |
| left_7 | right_7 | email | email |
| left_8 | right_8 | email | email |
| left_9 | right_9 | email | email |
| left_10 | right_10 | email | email |
| left_11 | right_11 | name | first_name + last_name |
| left_12 | right_12 | name | first_name + last_name |
| left_13 | right_13 | name | first_name + last_name |
| left_14 | right_14 | name | first_name + last_name |
| left_15 | right_15 | name | first_name + last_name |
| left_16 | right_16 | name | first_name + last_name |
| left_17 | right_17 | name | first_name + last_name |
| left_18 | right_18 | name | first_name + last_name |
| left_19 | right_19 | name | first_name + last_name |
| left_20 | right_20 | name | first_name + last_name |
| left_21 | right_21 | address+zip | address + zip_code |
| left_22 | right_22 | address+zip | address + zip_code |
| left_23 | right_23 | address+zip | address + zip_code |
| left_24 | right_24 | address+zip | address + zip_code |
| left_25 | right_25 | address+zip | address + zip_code |
| left_26 | right_26 | address+zip | address + zip_code |
| left_27 | right_27 | address+zip | address + zip_code |
| left_28 | right_28 | address+zip | address + zip_code |
| left_29 | right_29 | address+zip | address + zip_code |
| left_30 | right_30 | address+zip | address + zip_code |
| left_31 | right_31 | email_or_name | email OR (first_name + last_name) |
| left_32 | right_32 | email_or_name | email OR (first_name + last_name) |
| left_33 | right_33 | email_or_name | email OR (first_name + last_name) |
| left_34 | right_34 | email_or_name | email OR (first_name + last_name) |
| left_35 | right_35 | email_or_name | email OR (first_name + last_name) |
| left_36 | right_36 | email_or_name | email OR (first_name + last_name) |
| left_37 | right_37 | email_or_name | email OR (first_name + last_name) |
| left_38 | right_38 | email_or_name | email OR (first_name + last_name) |
| left_39 | right_39 | email_or_name | email OR (first_name + last_name) |
| left_40 | right_40 | email_or_name | email OR (first_name + last_name) |

## Expected Results

- **Total matches**: 40
- **Match rules**: Various (see table above)
- **Match type**: exact
- **All matches should be found** (100% recall)
- **No false positives** (100% precision)

## Testing Different Rules

```python
# Email-only matches
results = matcher.match(on="email")
# Should find: 10 email-only + 10 mixed = 20 matches

# Name-only matches
results = matcher.match(on=["first_name", "last_name"])
# Should find: 10 name-only + 10 mixed = 20 matches

# Multi-field matches
results = matcher.match(on=["address", "zip_code"])
# Should find: 10 multi-field matches

# Multiple rules (OR logic)
results = matcher.match(on="email").refine(on=["first_name", "last_name"])
# Should find: All 40 matches
```
