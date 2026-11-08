---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-3]
---

# Domain 3.05 — Bronze → Silver Cleaning and Data Quality FAQ

Source: [Data Cleaning and Data Quality](../certifications/data-engineer-associate/03-data-transformation-and-modeling/05-data-cleaning-and-quality.md).

## Typical Bronze → Silver recipe

```sql
CREATE OR REPLACE TABLE main.silver.customers AS
SELECT customer_id,
       lower(trim(email)) AS email,
       coalesce(upper(trim(country)), 'UNKNOWN') AS country,
       coalesce(try_cast(lifetime_value AS DECIMAL(12,2)), 0) AS lifetime_value,
       to_date(signup_date, 'yyyy-MM-dd') AS signup_date
FROM main.bronze.customers_raw
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id ORDER BY _ingested_at DESC, source_event_id DESC
) = 1;
```

Use a real, stable tie-breaker column (the example assumes `source_event_id` exists); otherwise latest-record selection may be nondeterministic.

## What does each cleaning operation do?
- `trim` strips surrounding spaces; `lower` / `upper` standardize case.
- `CAST` can fail on malformed values under ANSI mode; `TRY_CAST` returns NULL for malformed supported conversions.
- `COALESCE(x, y, ...)` returns the first non-NULL value.
- `dropna(subset=['customer_id'])` removes the **whole row** if that key is missing.
- Empty string is not the same as SQL NULL; NaN is a special floating-point value.
- Replacing malformed financial amounts with zero can hide upstream quality problems: do so only when business rules explicitly allow it.

## Profiling vs validation vs reconciliation
- **Profiling:** inspect distributions, missing values, distinct counts, outliers *before and after cleaning*.
- **Validation:** test business constraints on data in a table or pipeline.
- **Reconciliation:** compare counts, totals, and key coverage *between source and target* or between layers.
- `COUNT(DISTINCT key)` is exact; `approx_count_distinct(key)` trades a small amount of accuracy for compact state.

## Lakeflow expectations: 3 outcomes

```sql
CREATE OR REFRESH STREAMING TABLE main.silver.orders (
 CONSTRAINT valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
 CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION FAIL UPDATE,
 CONSTRAINT recent EXPECT (order_ts > '2020-01-01')
) AS SELECT * FROM STREAM(main.bronze.orders);
```

- **WARN** (default): keep invalid rows and record violations in pipeline metrics/event logs; not necessarily a user-facing popup.
- **DROP ROW:** remove invalid rows.
- **FAIL UPDATE:** fail the pipeline update rather than simply rejecting one row.

## Quarantine: route invalid rows to another table

```sql
CREATE OR REFRESH STREAMING TABLE main.silver.orders_valid AS
SELECT * FROM STREAM(main.bronze.orders)
WHERE order_id IS NOT NULL AND amount > 0;

CREATE OR REFRESH STREAMING TABLE main.silver.orders_quarantine AS
SELECT *, current_timestamp() AS quarantined_at
FROM STREAM(main.bronze.orders)
WHERE order_id IS NULL OR amount IS NULL OR amount <= 0;
```

**NULL pitfall:** `NOT (order_id IS NOT NULL AND amount > 0)` can evaluate to UNKNOWN when `amount` is NULL, so it does not correctly quarantine every invalid row.

## Delta constraints vs expectations
- `NOT NULL` and `CHECK` constraints are enforced on writes; violations reject a transaction.
- Informational PK/FK declarations are **not enforced** as automatic deduplication/referential integrity.
- Expectation metrics and row-level handling belong to Lakeflow processing.
