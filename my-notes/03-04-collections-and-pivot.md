---
type: study-notes
status: published
---

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
