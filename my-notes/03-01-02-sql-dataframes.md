---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-3]
---

# Domain 3.01–3.02 — Spark SQL and DataFrame FAQ

Source: [Spark SQL](../certifications/data-engineer-associate/03-data-transformation-and-modeling/01-spark-sql-fundamentals.md) · [DataFrames](../certifications/data-engineer-associate/03-data-transformation-and-modeling/02-dataframe-operations.md).

## Spark SQL vs PySpark DataFrame
Both interfaces use Spark's Catalyst optimizer and distributed engine; SQL is particularly useful for SQL/dbt workflows.

```python
df = spark.sql("SELECT claim_id, amount FROM main.silver.claims WHERE amount > 0")
filtered = spark.table("main.silver.claims").filter("amount > 0").select("claim_id", "amount")
```

## Lazy transformations vs actions
- Transformations: `select`, `filter`, `withColumn`, `join`, `groupBy`, `limit` construct a logical plan.
- Actions: `show`, `count`, `collect`, `first`, and writing trigger execution.
- `collect()` returns *all* matching rows to **driver memory**, risking driver OOM. Use `show(5)` or `take(5)` for inspection; `limit(5)` alone is still lazy.
- DataFrames are immutable; `withColumn` returns a new DataFrame.

## SQL comparisons: CTE and CTAS

```sql
-- CTE: temporary named query within this statement, no persistent table
WITH recent AS (
 SELECT * FROM main.silver.claims WHERE modified_at >= DATE '2026-11-01'
)
SELECT COUNT(*) FROM recent;

-- CTAS: create a persisted table from query results
CREATE OR REPLACE TABLE main.gold.large_claims AS
SELECT * FROM main.silver.claims WHERE amount > 10000;
```

## Window functions and filtering
- `ROW_NUMBER()`: unique sequential index within group, even for ties.
- `RANK()`: ties share rank, with gaps; `DENSE_RANK()`: ties share rank, without gaps.
- `LAG(x)`: previous ordered row; `LEAD(x)`: next ordered row.
- `PARTITION BY` in a window sets logical groups; do not confuse it with physical Spark partitions.

```sql
SELECT claim_id, claim_status, modified_at,
       LAG(claim_status) OVER (
         PARTITION BY claim_id ORDER BY modified_at
       ) AS previous_status
FROM main.silver.claim_history;
```

## Views and namespaces
- Unity Catalog objects use `catalog.schema.object`.
- A **temporary view** is session-scoped, not a durable Gold table.
- A **persistent view** stores a definition, not precomputed rows.
- SQL `QUALIFY ROW_NUMBER() OVER (...) = 1` directly filters window results in Databricks SQL; Synapse Dedicated typically needs a CTE/subquery.

## Synapse / dbt connection
A dbt model is SQL compiled and executed by Databricks. A dbt macro generates reusable SQL at compile time and does not independently create a physical table. `ref('silver_claims')` resolves references and registers model dependencies.

**Exam traps:** SQL `UNION` deduplicates; SQL `UNION ALL` keeps duplicates. PySpark `union` and `unionByName` both keep duplicates, but the latter matches columns by name.
