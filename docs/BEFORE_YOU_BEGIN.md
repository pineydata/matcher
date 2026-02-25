# Before You Begin: Data Preparation for Matching

Preparation to do on your data **before** you design and run a matching algorithm with matcher. Covers standardization, feature engineering, and other aspects that make matching more reliable and easier to evaluate.

Use this doc first; then follow [MATCHING_ALGORITHM_DESIGN.md](MATCHING_ALGORITHM_DESIGN.md) to choose rules, blocking, and evaluation.

---

## 1. Schema and Identifiers

**Required columns**

- **Entity resolution (two sources):** Each DataFrame must have the ID column you pass to `Matcher(..., left_id=..., right_id=...)`. Those columns are required; matcher will raise if they are missing.
- **Deduplication (one source):** The single DataFrame must have the column you pass to `Deduplicator(..., id_col=...)`.
- **Match and blocking fields:** Every column you use in `on` (for `match(on=...)` and `refine(on=...)`), or in `match(on=[...], matching_algorithm=FuzzyMatcher(...))`, or `blocking_key` must exist in the relevant table(s). For entity resolution, each such column must be present in **both** left and right and must have the **same name** in both (matcher joins on that name and validates up front).

**Uniqueness**

- ID columns are used to join and deduplicate match pairs. They do not need to be globally unique across left and right (e.g. both sides can have `id` 1, 2, 3). For deduplication, `id_col` should uniquely identify each row in the source.
- If the same (left_id, right_id) pair could be produced by more than one rule, matcher combines and de-duplicates by IDs, but your downstream logic may assume one row per pair.

**Naming**

- For evaluation, ground truth must have `left_id` and `right_id` columns. Matcher’s output uses your left ID column name and `{right_id}_right` (e.g. `id_right`). When calling `evaluate()`, pass `right_id_col="id_right"` if your matches use that suffix.

---

## 2. Nulls and Missing Values

**How matcher treats nulls**

- **Exact matching:** Polars inner joins exclude nulls. Rows where *any* field in the rule has a null do not match (including null to null). See [algorithms.py](../matcher/algorithms.py) docstring.
- **Fuzzy matching:** Rows where the fuzzy field is null are excluded from matching (same idea: no match).
- **Blocking:** Rows with null in the blocking key are grouped into a single block (left nulls and right nulls are compared only within that block).

**What to do before matching**

- **Decide policy for match fields:** If you want “missing” to mean something (e.g. treat empty string as no match, or match on another field only), normalize before calling matcher:
  - Drop rows with nulls in key match fields, or
  - Fill with a sentinel (e.g. `""` or `"MISSING"`) only if you intend to match on that value.
- **Blocking key:** If many rows have null blocking key, they all land in one block; that can be slow or memory-heavy. Consider deriving a fallback (e.g. “UNKNOWN”) or filtering for analysis.

---

## 3. Standardization for Exact Matching

Matcher does **no** normalization for exact rules; comparison is by Polars join (equality). If you need “same value” to ignore case or whitespace, do it beforehand.

**Common steps**

- **Case:** Lowercase (or uppercase) identifiers if you want `"John@Email.com"` and `"john@email.com"` to match.  
  Example: `df.with_columns(pl.col("email").str.to_lowercase())`
- **Whitespace:** Strip leading/trailing spaces and optionally collapse internal spaces.  
  Example: `pl.col("email").str.strip_chars()`
- **Punctuation / format:** For fields like phone or postcode, decide one canonical form (e.g. digits only, or a single hyphen/dash style) and apply it to both sources so exact match is meaningful.

**Where to do it**

- In a preprocessing notebook or pipeline: create cleaned columns (e.g. `email_clean`) and use those in `rules` so the raw columns remain available for inspection or export.

---

## 4. Standardization for Fuzzy Matching

**What matcher does internally**

- For the fuzzy field only, matcher applies **lowercase** and **strip** (trim) when computing similarity. It does not change the stored data or add new columns. See [matcher.py](../matcher/matcher.py) (e.g. `str.to_lowercase().str.strip_chars()`).

**What to do before calling fuzzy match**

- **Type:** The fuzzy field must be a string column (`Utf8`). Cast if needed: `pl.col("name").cast(pl.Utf8)`.
- **Encoding:** Ensure strings are valid UTF-8; fix or drop rows with bad encoding if you load from external systems.
- **Optional extra normalization:** If your data has systematic variation (e.g. “Jr.”, “Ph.D.”, extra spaces), consider:
  - Normalizing spacing (e.g. collapse multiple spaces to one).
  - Replacing or removing punctuation that shouldn’t affect identity (e.g. periods in abbreviations).
  - Doing this in a derived column and using that column in `match(on=["name_clean"], matching_algorithm=FuzzyMatcher(...))` so you keep the original for review.

---

## 5. Feature Engineering

**Derived columns for matching**

- **Full name:** If you want one fuzzy field for “name”, create e.g. `full_name = first_name + " " + last_name` (and handle nulls/middles). Then use `match(on=["full_name"], matching_algorithm=FuzzyMatcher(...))`.
- **Address line:** Similarly, a single `address_line` (e.g. street + city + zip) can be used for one rule or one fuzzy field, with consistent separators and null handling.

**Derived columns for blocking**

- **Geography:** Use existing columns (e.g. `zip_code`, `state`) if they exist. If not, derive from address or other fields.
- **Prefix / bucket:** For a weaker blocker (e.g. first letter of surname), add e.g. `surname_letter = pl.col("last_name").str.slice(0, 1)` and use it as `blocking_key` to reduce comparisons while keeping recall in mind.
- **Consistency:** Apply the same derivation on both sources (and in ground truth if you ever block on it) so blocks align.

**Identifiers**

- **Email:** Lowercase and strip; optionally strip dots in local part or normalize domain (e.g. gmail.com vs googlemail.com) if you need those to match.
- **Phone:** Normalize to digits (and optionally country/area code) so formats like `(555) 123-4567` and `5551234567` match when used in exact rules.

---

## 6. Data Types and Encoding

- **Fuzzy:** The field passed to `match(on=[...], matching_algorithm=FuzzyMatcher(...))` must be `Utf8`. Cast before matching if you have other types (e.g. from CSV or DB).
- **Exact:** Join keys can be any type Polars can compare for equality (e.g. string, int). Left and right must be the same type for the same field (e.g. don’t join string to int).
- **IDs:** Typically string or int; ensure no accidental float or mixed types so joins and evaluation are stable.

---

## 7. Ground Truth and Review Data

**If you plan to evaluate**

- **Ground truth:** A Polars DataFrame with columns `left_id` and `right_id` listing known true pairs. Load from CSV/Parquet or build from existing keys; matcher does not create it.
- **ID alignment:** Use the same ID column names and values as in your match results (e.g. left_id from left table, right_id from right table; for dedup, right side is `id_right`).
- **Sampling for review:** If you will create ground truth by human review, export a sample (e.g. `results.sample(n=50).export_for_review("review.csv")`), then convert reviewed pairs into a ground truth DataFrame with `left_id` and `right_id`.

**If you plan to tune**

- Have enough representative pairs (and non-pairs) so that precision/recall and `find_best_threshold` are meaningful. Prefer a stable evaluation set so you can compare runs.

---

## 8. Checklist Summary

| Area | Preparation |
|------|-------------|
| **Schema** | Required ID column(s) and all rule/blocking/fuzzy fields present; names consistent with `left_id` / `right_id` / `id_col`. |
| **Nulls** | Policy for nulls in match and blocking fields (drop, fill, or leave; remember matcher excludes nulls from exact/fuzzy matches). |
| **Exact rules** | Standardize case, whitespace, and format (e.g. phone, email) in columns used in `match(on=...)` or `refine(on=...)`. |
| **Fuzzy field** | Utf8; optional extra normalization in a derived column; encoding and spacing considered. |
| **Feature engineering** | Derived columns for full name, address, or blocking (prefix/bucket) as needed; same logic on both sources. |
| **Ground truth** | `left_id` and `right_id` DataFrame ready if you will call `evaluate()` or `find_best_threshold()`. |

Do this prep in a notebook or pipeline before you run matcher; then use [MATCHING_ALGORITHM_DESIGN.md](MATCHING_ALGORITHM_DESIGN.md) to choose rules, blocking, and evaluation strategy.
