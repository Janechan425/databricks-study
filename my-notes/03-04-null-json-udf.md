---
type: study-notes
status: published
---

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
