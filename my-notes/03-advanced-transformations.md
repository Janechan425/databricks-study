---
type: study-notes
status: published
---

# Domain 3.04 — Advanced Transformations: Study Index

The previous long Q&A has been reorganized without discarding worked examples:

1. [Collections, maps, structs, explode, lit, and PIVOT](./03-04-collections-and-pivot.md) — original sections 1–5.
2. [NULL/NaN, forward fill, JSON, cache, and UDFs](./03-04-null-json-udf.md) — original sections 6–10.

Read alongside [04 — Advanced Transformations](../certifications/data-engineer-associate/03-data-transformation-and-modeling/04-advanced-transformations.md).

## Fast distinctions

| Easily confused | Correct meaning |
|---|---|
| `explode` / `collect_list` | Expand collection into rows / group row values into array |
| `collect_list` / `collect()` | Distributed aggregation / return rows to driver |
| `lit` / `col` | Constant / column reference |
| ARRAY / MAP / STRUCT | Indexed elements / key-value pairs / named typed fields |
| `isnull` / `isnan` | SQL NULL / floating-point NaN |
| `cache()` / Delta Table | Temporary reusable cache / durable table |
| Python UDF / Pandas UDF / built-ins | Row-wise Python / Arrow batches / optimized native expression |
