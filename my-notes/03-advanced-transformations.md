---
title: Advanced Transformations - Study Notes and FAQ
type: study-notes
tags: [pyspark, arrays, maps, structs, pivot, nulls, udf]
status: published
---

# Advanced Transformations — Questions, Examples, and Exam Notes

These notes expand [04 — Advanced Transformations](../certifications/data-engineer-associate/03-data-transformation-and-modeling/04-advanced-transformations.md) with the concepts that needed extra explanation. Examples use `from pyspark.sql import functions as F` unless stated otherwise.

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

## Quick exam checklist

- `lit` = literal constant; `col` = existing column reference.
- `explode(array)` = 1 row per element; `explode(map)` = 1 row per key/value pair.
- `collect_list` makes an Array **in the DataFrame**; `collect()` brings all rows to the driver.
- `array_contains` tests membership; `size` counts elements; `map_keys` / `map_values` return arrays.
- Struct fields via `struct_col.field`, even after exploding an Array of Structs.
- `pivot` = group rows + turn categorical values into columns + aggregate; `SUM(CASE WHEN...)` works too.
- `isnull` checks NULL; `isnan` checks NaN; `fillna` replaces NULL (and numeric NaN).
- Forward fill = `last(..., ignorenulls=True)` on an ordered running window.
- `from_json` parses strings to Struct/Map/Array using a schema.
- `cache` requires an action to populate and `unpersist` to release.
- Prefer Spark built-ins, then Pandas UDF if custom vectorized Python logic is needed, then regular Python UDF.
