---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-3]
---
# Domain 3 — Data Transformation and Modeling

> One chapter, concept-grouped reference. All worked examples from the previous seven section notes are preserved below; duplicate frontmatter and titles were removed. Relevant course: [Domain 3](../certifications/data-engineer-associate/03-data-transformation-and-modeling/README.md).

## Concept map
1. Spark SQL, DataFrames, actions, windows, views
2. Joins, aggregations and deduplication
3. Complex types, flattening and pivot
4. NULLs, JSON, caching and UDFs
5. Cleaning, constraints, expectations and quarantine
6. Gold objects, CDC and dbt
7. Spark performance, AQE, skew and salting

**Remember:** Delta is a *table format*, not incremental loading. Bronze, Silver and Gold can all store Delta tables. Managed/external describes table management; full/incremental describes ingestion strategy.


---

## Part 1: 02 sql dataframes

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

---

## Part 2: joins aggregations

# Domain 3.03 — Joins, Deduplication and Aggregation

Source: [Joins and Aggregations](../certifications/data-engineer-associate/03-data-transformation-and-modeling/03-joins-aggregations.md).

## Join types
- INNER: matched rows.
- LEFT: all left rows, with NULLs on unmatched right.
- LEFT SEMI: left rows with a match, *without* right-side columns.
- LEFT ANTI: left rows without a match.
- CROSS: Cartesian product; result cardinality can explode.
- Multiple join keys: `ON a.claim_id=b.claim_id AND a.line_id=b.line_id`.

## Broadcast vs Sort-Merge
- **Broadcast Hash Join (BHJ):** distribute a genuinely small dimension to executors; avoid shuffling the large side for the join.
- **Sort-Merge Join (SMJ):** shuffle both sides to matching join-key partitions, sort locally, then merge matching keys. Often suitable when neither side is small.
- `spark.sql.autoBroadcastJoinThreshold` is a *planning threshold*, **not a guarantee** of broadcast.
- `/*+ BROADCAST(d) */` requests broadcast even above the automatic threshold if the join strategy supports it; do not force large tables (driver/executor OOM risk).

```sql
SELECT /*+ BROADCAST(d) */ f.claim_id, d.provider_name
FROM main.gold.fact_claim f
JOIN main.gold.dim_provider d ON f.provider_id = d.provider_id;
```

## Aggregates and uniqueness
- `COUNT(*)` counts rows; `COUNT(col)` excludes NULLs; `COUNT(DISTINCT col)` is exact.
- `approx_count_distinct(col)` estimates distinct values using a compact probabilistic sketch; useful for very high cardinality.
- `AVG`, `SUM`, `MIN`, `MAX` and `GROUP BY` are familiar from Synapse SQL.

## Picking latest change per key

```sql
SELECT * EXCEPT (rn)
FROM (
 SELECT *, ROW_NUMBER() OVER (
    PARTITION BY claim_id ORDER BY modified_at DESC, ingest_sequence DESC
 ) AS rn
 FROM main.silver.claim_events
)
WHERE rn = 1;
```

`dropDuplicates(['claim_id'])` removes duplicate keys but does **not** guarantee keeping the newest version. Use ordered `ROW_NUMBER` and a deterministic tie-breaker.

## Physical versus logical partitions
A join/grouping key influences shuffle distribution. A physical Spark Partition is a chunk processed by a Task; `PARTITION BY` in a SQL window is a *logical group*, not a direct allocation of physical partitions.

---

## Part 3: collections and pivot

# Domain 3.04 — Collections, Structs and Pivot

Source: [Advanced Transformations](../certifications/data-engineer-associate/03-data-transformation-and-modeling/04-advanced-transformations.md).

## 1. Array operations: `array`, `array_contains`, `size`, `explode`, `collect_list`

An `ARRAY` stores multiple elements in **one column of one row**.

```python
from pyspark.sql import functions as F

# name | first_name | last_name → name | ["Alice", "Chen"]
employees.select(
    "name", F.array("first_name", "last_name").alias("name_parts")
)

# skills = ["Python", "SQL"] → knows_python = true; num_skills = 2
employees.select(
    "name",
    F.array_contains("skills", "Python").alias("knows_python"),
    F.size("skills").alias("num_skills")
)

# ["Python", "SQL"] → 2 rows: (Alice, Python), (Alice, SQL)
employees.selectExpr("name", "explode(skills) AS skill")

# Multiple employee rows → one ARRAY of names per department
employees.groupBy("department").agg(F.collect_list("name").alias("names"))
```

`explode` increases row count; `collect_list` groups values into a distributed array, **not** the same as the DataFrame action `collect()`, which returns rows to driver memory. `collect_list` does not guarantee element order after shuffles. `explode_outer` keeps a row for a null or empty array, producing a null element.

## 2. `lit()` vs `col()`: constant vs column reference

```python
F.lit("salary")   # Literal STRING "salary" in every row
F.col("salary")   # Current row's salary field (e.g., 85000)
F.lit(100)        # Literal INTEGER 100
```

`lit` abbreviates **literal**, not list.

## 3. Maps: `create_map`, `map_keys`, `map_values`, map indexing, `explode`

A Map stores key/value pairs; **values need a common compatible type**. Here the numeric salary is deliberately cast to STRING to coexist with department names.

```python
df_map = employees.select(
    "name",
    F.create_map(
        F.lit("salary"), F.col("salary").cast("string"),
        F.lit("department"), F.col("department")
    ).alias("info")
)
df_accessed = (df_map
    .withColumn("dept", F.col("info")["department"])
    .withColumn("sal", F.col("info")["salary"])
)
df_keys = df_map.select(F.map_keys("info").alias("keys"))
df_values = df_map.select(F.map_values("info").alias("values"))
df_exploded = df_map.selectExpr("name", "explode(info) AS (key, value)")
```

For Alice (`salary=85000`, `department=IT`), `info` is like `{"salary":"85000","department":"IT"}`. `info["department"]` returns `IT`; exploding produces two rows: `salary | 85000` and `department | IT`. Map iteration order is not guaranteed.

**ARRAY vs MAP vs STRUCT:** `ARRAY<STRING>` stores indexed elements; `MAP<STRING,STRING>` stores dynamic keys with one value type; `STRUCT<salary:INT,department:STRING>` stores *named fields of potentially different types*. Use STRUCT for fixed, heterogeneous fields.

## 4. Array of Structs: two-stage flattening

Source column type:

```text
addresses: ARRAY<STRUCT<street: STRING, city: STRING>>
Alice: [{street: "King St", city: "Toronto"},
        {street: "Queen St", city: "Kingston"}]
```

```python
df_unnested = (
    employees
    .select("id", "name", F.explode("addresses").alias("address"))
    .select("id", "name", "address.street", "address.city")
)
```

**Step 1:** `explode(addresses)` converts Alice to two rows, each with one `address` Struct. **Step 2:** `address.street` and `address.city` extract Struct fields as columns.

| name | street | city |
|---|---|---|
| Alice | King St | Toronto |
| Alice | Queen St | Kingston |

A plain Struct can be flattened without `explode`: `F.col("contact_info.email").alias("email")`.

## 5. PIVOT = rows → columns + aggregation

Input:

| year | department | salary |
|---|---|---:|
| 2024 | Engineering | 80000 |
| 2024 | Engineering | 90000 |
| 2024 | Sales | 60000 |
| 2025 | Engineering | 100000 |

Output:

| year | Engineering | Sales | HR |
|---|---:|---:|---:|
| 2024 | 170000 | 60000 | NULL |
| 2025 | 100000 | NULL | NULL |

```python
df_pivoted = (
    employees
    .groupBy("year")  # result rows
    .pivot("department", ["Engineering", "Sales", "HR"])  # result columns
    .agg(F.sum("salary"))  # values in cells
)
```

Equivalent Spark SQL (project the three relevant columns first so other fields such as `id` do not introduce unwanted implicit groupings):

```sql
SELECT *
FROM (SELECT year, department, salary FROM employees)
PIVOT (
    SUM(salary)
    FOR department IN (
        'Engineering' AS eng, 'Sales' AS sales, 'HR' AS hr
    )
);
```

**Without PIVOT** — conditional aggregation, familiar from SQL Server / Synapse:

```sql
SELECT year,
    SUM(CASE WHEN department = 'Engineering' THEN salary END) AS eng,
    SUM(CASE WHEN department = 'Sales' THEN salary END) AS sales,
    SUM(CASE WHEN department = 'HR' THEN salary END) AS hr
FROM employees
GROUP BY year;
```

`AS eng` is a column **alias**. If `ELSE 0` is added, absent departments get 0 instead of NULL (unlike the shown PIVOT). Explicit pivot values also avoid a discovery step to determine the output columns.


## Exam reminders

- `explode` expands array/map values into rows; struct field extraction uses dot notation.
- `collect_list` aggregates values into arrays; `collect()` returns rows to the driver.
- Pivot reshapes grouped categorical values into columns; `SUM(CASE WHEN...)` is a SQL alternative.

---

## Part 4: null json udf

# Domain 3.04 — NULLs, Windows, JSON, Cache and UDFs

Source: [Advanced Transformations](../certifications/data-engineer-associate/03-data-transformation-and-modeling/04-advanced-transformations.md).

## 6. `isnull` vs `isnan` vs `fillna` vs T-SQL `ISNULL`

`NULL` means a missing/unknown value; `NaN` is a special FLOAT/DOUBLE value (Not a Number). They are distinct.

| salary | `F.isnull("salary")` | `F.isnan("salary")` |
|---|---|---|
| NULL | true | false |
| NaN | false | true |
| 85000.0 | false | false |

```python
df_checked = employees.select(
    "name",
    F.isnull("email").alias("email_missing"),
    F.isnan("salary").alias("salary_nan"),
    (F.isnull("salary") | F.isnan("salary")).alias("salary_missing_or_nan")
)

df_filled = employees.fillna({"salary": 0, "department": "Unknown"})
df_cleaned = employees.dropna(subset=["email"])
```

`fillna` replaces **NULL and floating-point NaN** with the supplied suitable replacement; it does not automatically correct empty strings or negative numbers. Both `fillna` and `dropna` return new DataFrames.

| Intent | Synapse / T-SQL | Spark SQL | PySpark |
|---|---|---|---|
| Test missing | `salary IS NULL` | `salary IS NULL` | `F.isnull("salary")` |
| Replace missing | `ISNULL(salary, 0)` | `COALESCE(salary, 0)` | `df.fillna({"salary": 0})` |

**Trap:** T-SQL `ISNULL(a, b)` replaces values, while PySpark `isnull(a)` tests them.

## 7. Forward fill: carry the latest non-NULL value forward

Example sorted salaries `[80000, NULL, NULL, 95000, NULL]` become `[80000, 80000, 80000, 95000, 95000]`.

```python
from pyspark.sql.window import Window

w = (Window.partitionBy("employee_id")
    .orderBy("effective_date", "record_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df_forward = salary_history.withColumn(
    "salary_filled",
    F.last("salary", ignorenulls=True).over(w)
)
```

`partitionBy` ensures **one employee's salary does not fill another's**. `orderBy` needs a deterministic tie-breaker; the frame starts at the earliest row in that employee's group and ends at the current row. `ignorenulls=True` skips NULL. A leading sequence of NULLs stays NULL until an actual value appears.

Spark SQL equivalent:

```sql
SELECT employee_id, effective_date,
       LAST_VALUE(salary, TRUE) OVER (
           PARTITION BY employee_id
           ORDER BY effective_date, record_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS salary_filled
FROM salary_history;
```

Do not forward-fill unrelated employee records or independently meaningful events just because they are ordered by date.

## 8. `from_json()`: JSON string → typed complex column

```python
# json_data = '{"name":"Alice","age":30}' (STRING, not yet a STRUCT)
parsed = df.withColumn(
    "parsed",
    F.from_json("json_data", "name STRING, age INT")
)
flat = parsed.select("parsed.name", "parsed.age")
# Reverse: F.to_json("parsed")
```

`from_json` parses a **DataFrame column** with an explicit schema into a STRUCT, MAP, or ARRAY. In permissive parsing, malformed JSON normally results in null output; missing fields are null. `spark.read.json(path)`, by contrast, **reads JSON files / sources to create a DataFrame**.

## 9. `cache()`: reuse an intermediate distributed result

```python
df_cleaned = df.filter(F.col("sales") > 100).cache()
df_cleaned.count()        # action: calculates and populates cache
result1 = df_cleaned.select("name", "sales")     # lazy
result2 = df_cleaned.filter(F.col("sales") > 500)  # lazy
result1.show()
result2.show()
df_cleaned.unpersist()    # release cache, do not delete source data
```

`cache()` is **lazy**: merely declaring it does not materialize data. Cached blocks are distributed among executors, **not collected into driver memory** like `collect()`. Cache only reused, expensive transformations; it consumes memory/disk and is not durable storage. For reusable ETL materialization across runs, use a Delta table.

## 10. Regular Python UDF vs vectorized Pandas UDF

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

@udf(DoubleType())
def regular_raise(salary):
    return salary * 1.1 if salary is not None else None

@pandas_udf("double")
def pandas_raise(salary: pd.Series) -> pd.Series:
    return salary * 1.1

regular = employees.withColumn("raised", regular_raise("salary"))
vectorized = employees.withColumn("raised", pandas_raise("salary"))

# Usually best: built-in JVM/Spark expression, no Python UDF at all:
preferred = employees.withColumn("raised", F.col("salary") * 1.1)
```

- **Regular UDF:** Python function runs row by row.
- **Pandas UDF:** Apache Arrow transfers columnar batches; `pd.Series` enables vectorized batch computation, reducing Python function calls and serialization/conversion overhead.
- **Built-ins:** can be optimized by Catalyst and avoid Python-worker execution; use first.
- Pandas UDF is **often**, not always, faster than a regular UDF. Batch size, workload, vectorization, and data conversion all affect speed.
- `F.when(condition, value).otherwise(other)` is the built-in equivalent of SQL `CASE WHEN`.


## Exam reminders

- Distinguish NULL from NaN and Spark `isnull` from T-SQL `ISNULL`.
- An ordered window with `IGNORE NULLS` can forward-fill within one key.
- `from_json` parses a string column; `spark.read.json` reads JSON data sources.
- Built-in Spark expressions are preferred to Python UDFs when available.

---

## Part 5: cleaning quality

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

---

## Part 6: gold cdc dbt

# Domain 3.06 — Gold Objects, CDC, dbt and Migration Choices

Source: [Gold Layer Objects](../certifications/data-engineer-associate/03-data-transformation-and-modeling/06-gold-layer-objects.md).

## Choosing Gold object types

| Object | Stored? | When computed / refreshed | Typical use |
|---|---|---|---|
| Delta Table | Yes | Custom job / INSERT / MERGE / CTAS | Fact/dimension with late corrections |
| View | No precomputed data | Recomputed when read | Light projection / semantic abstraction |
| Materialized View (MV) | Yes | Managed refresh, scheduled or triggered | Frequently queried aggregates |
| Streaming Table (ST) | Yes | Incremental pipeline processing | New events and managed CDC |

A Gold layer is a **business-ready layer**, not a requirement to use MV/ST. Delta Table means Delta transaction-log format; **not** Synapse's colloquial use of “delta load” for incremental ingestion.

## Streaming and historical UPDATE/DELETE
- A plain `STREAM(source)` design processes new input. It does **not** automatically reconcile arbitrary historical corrections in the source Delta table.
- A streaming Delta read may fail on upstream modifications without suitable change-handling configuration. Do not assume old rows are silently re-read.
- For source CDC events, use **AUTO CDC**, or use a batch Delta Table + custom `MERGE`; MV is another option when managed query refresh is appropriate.
- Full refresh can rebuild when the complete source history is available, but is costly.

## AUTO CDC event processing (SCD Type 1)

Source `main.bronze.claim_changes` must provide event records with keys, order and deletion metadata.

```sql
-- In a Lakeflow Spark Declarative Pipeline SQL source file
CREATE OR REFRESH STREAMING TABLE main.gold.fact_claim;

CREATE FLOW claim_cdc_flow AS AUTO CDC INTO main.gold.fact_claim
FROM STREAM(main.bronze.claim_changes)
KEYS (claim_id, line_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 1;
```

- **Composite keys:** `KEYS (claim_id, line_id)` matches both columns.
- **INSERT/UPDATE:** normal non-delete events upsert by key and sequence automatically; no separate INSERT/UPDATE clauses are required.
- **DELETE:** explicitly define which events remove rows via `APPLY AS DELETE WHEN`.
- **SCD Type 1:** latest state only; `STORED AS SCD TYPE 2` keeps historical versions with `__START_AT` / `__END_AT`.
- **IMPORTANT:** AUTO CDC applies **provided** change events; it does not detect source database UPDATE/DELETE by itself. Watermark extraction on `modified_at` may not capture DELETEs.
- Partially populated updates require careful field/NULL semantics; consider `IGNORE NULL UPDATES` only when NULL means “not supplied,” not “clear the value.”
- Typically one CDC flow per target table, but multiple flows can live in one Pipeline. One-time historic loads do not require AUTO CDC.

## MV refresh vs ST refresh
- **View:** latest source data reflected at query time.
- **MV:** precomputed result is updated when `REFRESH MATERIALIZED VIEW`, schedule or `TRIGGER ON UPDATE` runs; not instantaneous on every source write.
- **ST:** pipeline checkpoint tracks new stream input; automatic CDC needs explicit CDC processing rules.

## Synapse → Databricks with dbt
- **dbt Model:** SQL defining a target relation; choose `table`, `view`, `incremental` or adapter-supported MV/ST.
- **dbt Macro:** reusable Jinja SQL code generator; expanded at compilation rather than being a data-processing table.
- **dbt `ref()`:** resolves upstream model and registers DAG dependencies.
- **dbt incremental + MERGE:** works well for SQL models that receive changed rows; handle deletes/late arrivals explicitly.

```sql
-- models/gold/fact_claim.sql (illustrative dbt model)
{{ config(materialized='incremental',
          unique_key=['claim_id','line_id'],
          incremental_strategy='merge') }}
SELECT claim_id, line_id, amount, modified_at
FROM {{ ref('silver_claims') }}
{% if is_incremental() %}
WHERE modified_at >= (
    SELECT COALESCE(MAX(modified_at), TIMESTAMP '1900-01-01')
    FROM {{ this }}
)
{% endif %}
```

This watermark pattern is illustrative, not a general-purpose CDC replacement. For hard deletes, missing timestamps or late-arriving changes, design explicit handling.

**Exam distinction:** AUTO CDC belongs to Lakeflow pipeline CDC processing; a dbt incremental model that compiles a `MERGE` is a different way to update a Delta target.

---

## Part 7: spark tuning aqe

# Domain 3.07 — Spark Runtime, Shuffle Tuning, AQE and Salting

Source: [Spark Tuning Parameters](../certifications/data-engineer-associate/03-data-transformation-and-modeling/07-spark-tuning-parameters.md).

## Key vocabulary
- **OSS:** open-source software, here Apache Spark defaults rather than Databricks-managed defaults.
- **JVM:** Java Virtual Machine; Spark driver/executor processes largely run JVM code.
- **GC:** garbage collection of JVM heap objects; excessive GC can hurt performance.
- **RDD:** Resilient Distributed Dataset, lower-level Spark partitioned data abstraction.
- **Cluster:** computing resources (driver and worker nodes). **Executor:** process on worker that runs tasks. **Task:** a unit of work. **Partition:** a chunk of distributed data, usually one Task per partition *within one Stage*.
- **Parallel task slots:** limited by available executor cores and CPU per task, **not** by `spark.default.parallelism` directly.

## 200 partitions vs 8 running tasks
With 200 partitions and 8 available single-core task slots, a Stage can run up to ~8 Tasks at once. As a slot becomes free, the next waiting Task can run; Spark does **not** wait for every Task in a rigid eight-task batch. Total task count and available concurrent slots are different quantities.

## Configuration summary

| Setting | Meaning | Trap |
|---|---|---|
| `spark.sql.shuffle.partitions` | Initial SQL/DataFrame shuffle partition count (OSS default 200) | Not executor count |
| `spark.default.parallelism` | Default partition count for many RDD operations | Does not set SQL/DataFrame shuffle count |
| `spark.executor.memory` | JVM heap *per executor* | Not total worker-node RAM |
| `spark.driver.memory` | JVM heap for driver | `collect()` may OOM driver |
| `spark.sql.autoBroadcastJoinThreshold` | Broadcast candidate size threshold at planning | Below threshold is not a guarantee |
| `spark.databricks.adaptive.autoBroadcastJoinThreshold` | AQE adaptive broadcast threshold (Databricks-specific) | Runtime decision differs from initial plan |

```python
# 50 MiB = 50 * 1024 * 1024 = 52,428,800 bytes
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
```

Databricks serverless may use `auto` for shuffle partitions; memory settings typically require compute-level controls, and not every Spark config is user-settable.

## Broadcast Join vs Sort-Merge Join
- **Broadcast Hash Join:** replicate the small side on executors and join locally with large-side partitions; avoids a large-side join shuffle.
- **Sort-Merge Join:** partition both sides by join key, shuffle as needed, sort within corresponding partitions, then merge.
- SQL `/*+ BROADCAST(d) */` asks Spark to broadcast the specified side even above the automatic threshold if the strategy is valid. Broadcasting a large side can cause OOM.
- The term **honoured** means *respected / followed* (hint accepted).

## AQE = Adaptive Query Execution
AQE observes runtime shuffle statistics and can:
1. **Coalesce** several too-small shuffle partitions into fewer task units, lowering scheduling overhead.
2. **Switch** a planned shuffle join to Broadcast Hash Join when runtime statistics show a sufficiently small side and conditions permit.
3. **Split skewed join partitions** into smaller tasks, sometimes replicating the other join-side partition so all matching keys still join correctly.

AQE normally decides after upstream shuffle stages produce runtime statistics. It **does not** magically split an already-running slow Task midway through execution.

## Why fewer shuffle partitions may hurt
For 100 GiB of uniformly shuffled data: 200 partitions average ~0.5 GiB each; 50 partitions average ~2 GiB. Bigger partitions may increase memory spill to disk and Task duration. But increasing partitions **does not inherently split one hot grouping key** into many partitions.

## Salting for aggregation skew
Suppose `provider_id=101` dominates a `GROUP BY provider_id`. Introduce a salt 0–3 to divide that logical group:

```python
from pyspark.sql import functions as F

df_salted = df.withColumn("salt", (F.rand() * 4).cast("int"))
partial = (df_salted.groupBy("provider_id", "salt")
    .agg(F.sum("amount").alias("partial_amount")))
result = (partial.groupBy("provider_id")
    .agg(F.sum("partial_amount").alias("total_amount")))
```

Think **divide → parallel partial sums → combine**. The final SUM is unchanged. Salt values may hash into the same physical partition; even salting is not a guarantee of perfect balance. Spark already performs map-side partial aggregation, so benchmark before using manual salting.

For skewed joins, salting requires **both sides to match on (key, salt)**, often duplicating matching rows on the smaller side; consider broadcast or AQE first.

## Catalyst optimization terminology
- **Empty relation propagation:** if the optimizer can prove a query subtree returns zero rows (e.g. `WHERE 1=0`), it propagates that fact to simplify or skip unnecessary operators.
- **Short-circuit:** avoid executing steps whose outcome is already known.

## Measuring improvements
- **Wall-clock duration:** actual elapsed time from start to finish; not sum of CPU or Task times.
- **Jobs matrix view:** compares runs and task statuses at Job level; not the same as a Pipeline DAG or one Run's Graph/Timeline/List tab.
- **Spark UI Stages:** task count, median vs max duration, shuffle read/write, memory/disk spill.
- **Large Max vs Median:** investigate Skew; **high spill:** oversized partitions, memory pressure or inefficient operators.
- On Classic Compute, inspect cluster **Spark UI → Stages → Stage details**; Serverless may expose Query Profile and run metrics instead.

### Study question
**If 8 task slots are all busy, can AQE split a skewed partition into 4 new tasks?** Yes. Slots limit simultaneous execution, not total queued tasks. Freed slots run the new tasks. The skewed partition is generally subdivided *before the downstream Stage executes*, not while its original Task is underway.

## Glossary mini-review
`propagation` = spreading information through a plan; `honour a hint` = respect the optimization hint; `spill` = intermediate data written out when it cannot remain in memory; `skew` = uneven data/task distribution.

## Mock Exam 1 — Concept Clarifications (questions raised, not necessarily mistakes)

### SQL UNION vs PySpark union (Q18 — missed)

| API | Duplicate rows | Column matching |
|---|---|---|
| SQL `UNION` | Removes duplicates | By ordinal position |
| SQL `UNION ALL` | Retains duplicates | By ordinal position |
| PySpark `df1.union(df2)` | **Retains** duplicates | By ordinal position |
| PySpark `df1.unionByName(df2)` | **Retains** duplicates | By column name |
| PySpark `df1.union(df2).distinct()` | Removes duplicates | By ordinal position |

`union()` concatenates rows **vertically**; `join()` combines datasets **horizontally** according to join conditions. Python DataFrame `union()` corresponds to SQL `UNION ALL`, **not** SQL `UNION`.

### Complex Spark SQL types: ARRAY vs MAP vs STRUCT

| Type | Description | Example | Access |
|---|---|---|---|
| ARRAY | Ordered elements of a compatible type | `[10,20,30]` | `items[0]` |
| MAP | Key–value dictionary; keys and values each have their own defined type | `map('CA',100,'US',200)` | `sales['CA']` |
| STRUCT | Record with named fields, potentially of different types | `named_struct('city','Toronto','zip','M5V')` | `address.city` |

These are **Apache Spark SQL** types (not Databricks-exclusive). JSON text in STRING is *not automatically a STRUCT*; use JSON path `raw:items[0].sku` or parse it using `from_json(raw, schema)`.

### flatten vs explode vs struct expansion

```python
from pyspark.sql import functions as F
df = spark.createDataFrame([([[1, 2], [3, 4]],)], ["nested"])
df.select(F.flatten("nested")).show(truncate=False)  # [1,2,3,4] in ONE row
df.select(F.explode(F.flatten("nested")).alias("n")).show()  # FOUR rows
```

- `flatten`: removes **one level** of array nesting (`ARRAY<ARRAY<T>>` → `ARRAY<T>`); does not create new rows.
- `explode`: creates one row per element of an array/map; other columns are replicated.
- `explode_outer`: preserves a NULL output row for null/empty input.
- `select("address.*")`: expands STRUCT fields into **columns**.
- `array_join`: combines array elements into one string.

### Python UDF (User-Defined Function) (Q16)

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(StringType())
def normalize_name(name):
    return name.strip().lower().replace(" ", "_") if name else None

result = df.withColumn("name_clean", normalize_name(F.col("name")))
```

- Spark built-ins (`F.lower`, `F.trim`, etc.) are generally preferred when equivalent.
- A **Python UDF** enables custom per-row Python logic within distributed Spark execution; declare return type and ensure dependencies are available on compute.
- **Pandas UDF** / Arrow processing can vectorize supported operations, often reducing overhead.
- Avoid `toPandas()` on very large datasets; it collects data to the driver.
- Ordinary `withColumn` needs a Spark Column expression, not an arbitrary Python lambda.

### Lakeflow Declarative Pipelines / DLT Expectations (Q22)

**Delta Live Tables (DLT)** is the legacy name for capabilities now described under **Lakeflow Spark Declarative Pipelines**. A Pipeline defines streaming tables, materialized views, flows and data-quality expectations; **Lakeflow Jobs** orchestrates when tasks and pipelines run.

| Expectation policy | Invalid rows | Update behavior |
|---|---|---|
| `EXPECT (...)` (default **WARN**) | Kept | Continues, records metrics |
| `EXPECT (...) ON VIOLATION DROP ROW` | Dropped | Continues with valid rows |
| `EXPECT (...) ON VIOLATION FAIL UPDATE` | Update fails | Requires remediation/retry |

```sql
CREATE OR REFRESH STREAMING TABLE orders_clean (
  CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM(main.bronze.orders);
```

**Mock Exam 1 classification:** Q18 was a missed question. Q16, Q17, Q22 and the type/function questions were *clarification requests*, not errors.
