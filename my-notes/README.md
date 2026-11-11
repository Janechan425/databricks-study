# My Databricks Learning Notes

Personal study notes for the Databricks Certified Data Engineer Associate exam. The original course materials remain unchanged.

## Notes

### Domain 3 — Data Transformation and Modeling (first pass complete)

| Section | Study note |
|---|---|
| 01–02 Spark SQL and DataFrames | [SQL, DataFrame, CTE/CTAS, windows and actions](./03-01-02-sql-dataframes.md) |
| 03 Joins and Aggregations | [Joins, broadcast vs sort-merge, deduplication](./03-03-joins-aggregations.md) |
| 04 Advanced Transformations | [Overview/index](./03-advanced-transformations.md) |
| 04A Collections and Pivot | [Arrays, maps, structs, explode, pivot](./03-04-collections-and-pivot.md) |
| 04B NULL, JSON and UDFs | [NULL/NaN, forward fill, JSON, cache, UDFs](./03-04-null-json-udf.md) |
| 05 Cleaning and Quality | [Bronze to Silver, profiling, expectations, quarantine](./03-05-cleaning-quality.md) |
| 06 Gold Layer Objects | [Table/View/MV/ST, CDC, AUTO CDC, dbt](./03-06-gold-cdc-dbt.md) |
| 07 Spark Tuning | [JVM/RDD/GC, partitions, joins, AQE, salting, Spark UI](./03-07-spark-tuning-aqe.md) |

### Domain 6 — Troubleshooting, Monitoring and Optimization (first pass complete)

| Section | Study note |
|---|---|
| 01–05 Job monitoring, Spark UI, Delta optimization, liquid clustering, predictive optimization and compute troubleshooting | [Domain 6 complete notes, Q&A and exam traps](./06-troubleshooting-monitoring-optimization.md) |

### Other references

- [Glossary](Glossary.md) — Terms and concise explanations.
- [Synapse vs Databricks](Synapse-vs-Databricks.md) — Concepts and migration comparisons.
- [Mistakes](Mistakes.md) — Practice-question corrections and review points.

## Workflow

1. Read the source material in `certifications/data-engineer-associate/`.
2. Add annotations while reading; export useful annotations as Markdown.
3. Consolidate verified concepts into these notes without copying entire chapters.
4. Keep company information, credentials, and API keys out of this repository.
