# Summary - `stg_reviews` (SCD-2 + Parsing Model)

The `stg_reviews` model performs **input parsing, deduplication, business-key generation**, and **Slowly Changing Dimension Type-2 (SCD-2)** versioning for Amazon product reviews.
It is designed as a **single model** that handles both the *current-state* and *historical* review versions.

---

## RAW → PARSED → DEDUPED INGESTION PIPELINE

### Step 1: Load Raw Data

We read the raw feed from:

```
capstone_amazon_raw.capstone_amazon_review_raw_table
```

Raw fields include:
`asin`, `overall`, `verified`, `reviewTime`, `unixReviewTime`, `reviewText`, `summary`, `reviewerID`, `reviewerName`.

---

### Step 2: Parse and Normalize Fields

We clean and standardize all columns:

| Field                          | Processing                                                      |
| ------------------------------ | --------------------------------------------------------------- |
| `asin`                         | uppercase + trim                                                |
| `overall`                      | safe float cast, clamped between 0.0–5.0                        |
| `verified`                     | normalized across various truthy/falsy raw strings              |
| `review_date`                  | parsed using multiple date formats + fallback to unix timestamp |
| `review_text` + `summary`      | whitespace-normalized and empty-string → NULL                   |
| `reviewer_id`, `reviewer_name` | trimmed, empty → NULL                                           |

---

### Step 3: Deduplication

Multiple raw rows may represent the same logical review (due to ingestion duplicates).

We dedupe by:

```
asin, reviewer_id, unix_review_time
```

and keep only the most recent record.

---

## BUSINESS KEY (`review_pk`) GENERATION

We generate a deterministic **business key** by hashing all fields that uniquely differentiate one review from another:

```sql
md5(
    asin
    || reviewer_id
    || review_date
    || unix_review_time
    || md5(review_text)
) AS review_pk
```

### Why include `review_text`?

Because some reviewers post **multiple reviews on the same product at the same time**, and only the text differs.
Including a hash of the text ensures **true uniqueness** of each review.

---

## FULL-REFRESH (First Run) - SCD-2 SEEDING

When dbt runs in non-incremental mode:

```
dbt run --select stg_reviews --full-refresh
```

we:

* Insert all reviews
* Assign `version = 1`
* Set:

  * `effective_from = now()`
  * `effective_to = NULL`
  * `is_current = TRUE`
  * `surrogate_id = UUID_STRING()`

This builds the **initial SCD-2 snapshot**.

---

## INCREMENTAL RUNS - SCD-2 VERSIONING

When dbt runs incrementally:

```
dbt run --select stg_reviews
```

we only insert new **versions** of reviews where changes occurred.

### 4.1 Load the current feed

Latest cleaned rows from `source_final`.

---

### 4.2 Simulation Logic (Only for testing SCD-2)

To force SCD-2 behavior in dev/testing, the model randomly picks **10 current review_pks** and assigns a **simulated new `overall` rating** in the range 1.0–5.0:

```sql
round(1.0 + uniform(0::float, 4::float, random()), 1)
```

This ensures:

* predictable range: 1.0–5.0
* stable float behavior (no scientific notation issues)

---

### 4.3 Detecting Changed vs New Rows

A new SCD-2 version is created when:

```
tcur.overall IS DISTINCT FROM ff.overall
    OR
tcur.review_pk is null (never seen before)
```

(We no longer compare review_text because review_pk already incorporates it.)

---

### 4.4 Insert a NEW SCD-2 VERSION

For each changed row:

* `surrogate_id = UUID_STRING()`
* `version = previous_max_version + 1`
* `effective_from = now()`
* `effective_to = NULL`
* `is_current = TRUE`

We **append only** - we do not update existing records.

---

## POST-HOOK - Expire Old Versions

After inserting new versions, we run a post-hook:

```sql
update {{ this }}
set effective_to = current_timestamp(),
    is_current = false
where is_current = true
  and exists (
        select 1
        from {{ this }} t2
        where t2.review_pk = {{ this }}.review_pk
        and t2.version > {{ this }}.version
  );
```

This ensures:

* only **one version is current** for each review_pk
* previous versions are closed with `effective_to`

This is **textbook SCD-2 implementation**.

---

## FINAL RESULT - A FULL SCD-2 HISTORY TABLE

The table contains:

| Column                | Description                                 |
| --------------------- | ------------------------------------------- |
| `surrogate_id`        | Unique SCD-2 row id                         |
| `review_pk`           | Business key of the review                  |
| `version`             | SCD-2 version number                        |
| `is_current`          | TRUE for the active record                  |
| `effective_from`      | When version became active                  |
| `effective_to`        | When version expired                        |
| All review attributes | (`overall`, `review_text`, `summary`, etc.) |
| `loaded_at`           | Audit timestamp                             |

---

## High-Level Summary (Copy-Paste Ready)

**The `stg_reviews` model:**

1. **Parses & cleans raw Amazon review data**
2. **Deduplicates** records to remove ingestion-level duplicates
3. Creates a **business key (`review_pk`)** using ASIN + reviewer + timestamp + hashed review text
4. **Implements SCD-2 versioning**:

   * Full-refresh: seeds version 1 for all rows
   * Incremental runs: inserts new versions when data changes
   * Post-hook updates previous versions (`effective_to`, `is_current=false`)
5. **Generates simulated updates in dev environments** so SCD-2 can be validated
6. Produces a complete **historical table** with all changes over time
---
**[Back](./README.md)**

---
