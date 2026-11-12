---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-6, optimization, troubleshooting]
---

# Domain 6 — Troubleshooting, Monitoring and Optimization

Source: [Chapter 06](../certifications/data-engineer-associate/06-troubleshooting-monitoring-and-optimization/README.md). Personal revision notes, with exam vocabulary, SQL/PySpark examples, and clarifications to source examples.

## 6.1 Job Monitoring — Where to investigate

| Symptom / question | First place to inspect | English term |
|---|---|---|
| Which upstream task caused a failure? | Jobs & Pipelines → Job → Runs → Run → task graph | Dependency graph / upstream failure |
| Which task is slow across multiple runs? | Jobs Matrix view / run duration history | Run duration / trend |
| Is the delay queueing, provisioning, installation or execution? | Job run/task timeline and details | Queued / setup / execution time |
| Why did a task fail? | Task output, driver logs, error details | Task failure / driver logs |
| SQL query is slow? | SQL Query History → Query Profile | Query profile / execution plan |
| Need scheduled notifications? | Job notifications / alerts | Failure notification / alert |
| Recover only failed tasks? | Repair run where supported | Repair run |

Run states include PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR, and result states such as SUCCESS, FAILED, TIMEDOUT, CANCELED (exact availability depends on API/UI). Retry policies and run concurrency can affect scheduling. Use Job API and metrics for programmatic monitoring. Log useful row counts, checkpoints and errors, not credentials.

**Practical UI:** Jobs & Pipelines → Jobs → Job → Runs → select run. For classic compute, Spark UI may be available from the compute or Spark task. For SQL Warehouse, use Query History and Query Profile instead of assuming a traditional Spark UI. Query Profile shows operator timing, scans, shuffle and other metrics when applicable. A trivial `SHOW TABLES` profile can show only a single operator.

## 6.2 Spark UI and System Tables

**Spark execution hierarchy:** Job → Stage → Task. Each Stage has Tasks for partitions; available task slots limit concurrent Tasks, not total task count. The Spark UI includes Jobs, Stages, SQL/DataFrame, Storage, Executors and Environment tabs.

| Evidence | Interpretation | Next step |
|---|---|---|
| Max Task Duration >> Median | Straggler or data skew | Check per-task shuffle and data distribution |
| High Shuffle Read/Write | Expensive exchange / join / aggregation | Reduce data, tune joins/partitions |
| High Memory/Disk Spill | Intermediate data exceeds available memory | Reduce partition size, skew, or wide rows |
| High GC Time | JVM memory pressure | Examine memory use, caching, object churn |
| Small join side | Broadcast may avoid a large-side shuffle | Check broadcast threshold and memory |
| One dominant group key | Aggregation skew | Consider salting and two-stage aggregation |

**Distributed aggregation:** workers calculate partial results in parallel and then combine them (e.g. partial SUM → final SUM). Spark often already performs map-side partial aggregation.

```python
from pyspark.sql import functions as F
salted = df.withColumn("salt", (F.rand() * 4).cast("int"))
partial = salted.groupBy("customer_id", "salt").agg(F.sum("amount").alias("part"))
result = partial.groupBy("customer_id").agg(F.sum("part").alias("total"))
```

**AQE (Adaptive Query Execution):** coalesces small shuffle partitions, may switch to broadcast join, and can split skewed *join* partitions based on runtime stats. It does not automatically fix every skewed GROUP BY. Reducing `spark.sql.shuffle.partitions` makes average partitions bigger, potentially worsening spill. Use `auto` when supported.

**System tables** (access/availability depend on workspace and privileges):

```sql
SELECT * FROM system.lakeflow.jobs LIMIT 10;
SELECT * FROM system.lakeflow.job_run_timeline LIMIT 10;
SELECT * FROM system.lakeflow.job_task_run_timeline LIMIT 10;
SELECT * FROM system.access.audit LIMIT 10;
SELECT * FROM system.access.table_lineage LIMIT 10;
SELECT * FROM system.billing.usage LIMIT 10;
SELECT * FROM system.storage.predictive_optimization_operations_history LIMIT 10;
```

Timeline tables may contain multiple slices for one logical run: avoid naive `COUNT(*)` as a run count. Use documented run IDs and time windows. Billing usage is not a direct substitute for per-task runtime metrics.

**Synapse comparison:** `dm_pdw_*` names originate from Dynamic Management Views (DMVs) and Parallel Data Warehouse (PDW); Synapse Dedicated SQL Pool DMVs show distributed request/step/worker information. Databricks instead exposes Spark UI, SQL Query Profile, Jobs UI and system tables. Spark physical partitions are logical task data units; they are not necessarily Delta table directory partitions or permanently tied to one node.

## 6.3 Delta Lake Optimization

| Operation | What it does | Does not mean |
|---|---|---|
| `OPTIMIZE` | Compact many small Delta data files | Delete old files immediately |
| `OPTIMIZE ... ZORDER BY (...)` | Rewrite layout for multi-column locality and better file skipping | Create a B-tree index or fix execution skew |
| `VACUUM` | Remove eligible unreferenced old data files | Directly delete all transaction logs |
| `ANALYZE TABLE ... COMPUTE STATISTICS` | Gather optimizer statistics | Compact data files |

```sql
OPTIMIZE main.silver.orders;
OPTIMIZE main.silver.orders ZORDER BY (customer_id, order_date);
VACUUM main.silver.orders DRY RUN;
VACUUM main.silver.orders RETAIN 30 DAYS;
ANALYZE TABLE main.silver.orders COMPUTE STATISTICS;
ANALYZE TABLE main.silver.orders COMPUTE STATISTICS FOR COLUMNS customer_id;
DESCRIBE DETAIL main.silver.orders;
DESCRIBE EXTENDED main.silver.orders;
EXPLAIN EXTENDED SELECT * FROM main.silver.orders WHERE customer_id = 50;
```

```python
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "main.silver.orders")
dt.optimize().executeCompaction()
# For an unclustered table when Z-ORDER is appropriate:
dt.optimize().zorderBy("customer_id", "order_date").executeCompaction()
dt.vacuum(retention_hours=168)
```

**Z-order Curve / bit interleaving:** interleave binary bits of several coordinates to create a one-dimensional ordering that tends to keep nearby multi-dimensional values together. Rewriting files with this layout improves the locality of file-level min/max statistics, allowing **Data Skipping / File Pruning**. A Z-curve does not guarantee all nearby points are adjacent. Z-ORDER is physical file layout, **not** Spark shuffle partition placement, and not a traditional index. Use actual filter patterns; high-cardinality IDs can be useful if frequently filtered.

**High Cardinality:** many distinct values, e.g. `customer_id` or `email`. **Low Cardinality:** few distinct values, e.g. `is_active`. There is no universal numerical cutoff. **Data Skew** is *uneven frequency* across values and is distinct from cardinality. A high-cardinality key may be evenly distributed; a low-cardinality key may be extremely skewed.

**Retention:** `delta.deletedFileRetentionDuration` controls deleted-file retention (commonly 7 days default); `delta.logRetentionDuration` controls log history retention (commonly 30 days default). A successful VACUUM can make historical Time Travel impossible if old versions reference deleted files. Very short retention risks concurrent readers and streaming jobs; default safety checks generally prevent dangerously short retention.

```sql
ALTER TABLE main.silver.orders SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);
```

**Optimized Writes** and **Auto Compaction** are not the same as running full `OPTIMIZE` on every write:
```sql
ALTER TABLE main.silver.orders SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

**File statistics:** min/max/null counts support data skipping; optimizer table/column statistics support query planning. `delta.dataSkippingNumIndexedCols` limits indexed statistics columns on supported tables; it does not create conventional indexes. Bloom filters are probabilistic membership structures (false positives possible); do not memorize the source's `delta.bloomFilter.enabled` snippet as standard Databricks SQL. Avoid blindly scheduling OPTIMIZE/VACUUM if predictive optimization handles maintenance.

**Maintenance:** inspect `DESCRIBE DETAIL` (`numFiles`, `sizeInBytes`), Query Profile, actual filter patterns, and query plans; benchmark before/after. Compression: Delta data files are generally Parquet, commonly Snappy; gzip trades CPU for compression, LZ4 favors speed. Never assume uncompressed is fastest end-to-end. The source's `delta.detail('employees')` and `SHOW TABLES EXTENDED employees` examples are not valid standard inspection SQL. For partitioned tables, partition predicates can restrict OPTIMIZE; do not confuse that with row-level filtering. Choose maintenance frequency based on workload, not fixed daily/weekly/monthly rules.

## 6.4 Liquid Clustering and Predictive Optimization

**Liquid Clustering** is the modern flexible data-layout approach; it replaces traditional partitioning and Z-ORDER *on a given table*. Up to **four** clustering keys. Changing keys does not immediately rewrite all old files. `OPTIMIZE` maintains clustering incrementally; `OPTIMIZE FULL` reclusters historical data on supported runtimes.

```sql
CREATE TABLE main.silver.events (
  event_id BIGINT, customer_id BIGINT, event_ts TIMESTAMP
) CLUSTER BY (customer_id, event_ts);

ALTER TABLE main.silver.events CLUSTER BY (event_ts);
ALTER TABLE main.silver.events CLUSTER BY AUTO;
ALTER TABLE main.silver.events CLUSTER BY NONE;
OPTIMIZE main.silver.events;
OPTIMIZE main.silver.events FULL;
```

`CLUSTER BY AUTO` automatically selects/evolves keys based on workload and requires Predictive Optimization. Key changes can be beneficial for shifting access patterns, skewed partition layouts and high-cardinality filters. It is not combined with traditional partitioning or `ZORDER`. Converting a legacy partitioned table requires supported runtime/syntax (source cites DBR 18.1+ `REPLACE PARTITIONED BY WITH CLUSTER BY`).

**Predictive Optimization** automates **OPTIMIZE + VACUUM + ANALYZE** on supported **Unity Catalog managed tables** (not ordinary external tables or shared-in tables). It may collect statistics during writes. It uses serverless jobs compute with associated billing. The source notes default-on for accounts created from 2024-11-11, with rollout for earlier accounts; check actual workspace settings.

```sql
ALTER CATALOG main ENABLE PREDICTIVE OPTIMIZATION;
ALTER SCHEMA main.scratch DISABLE PREDICTIVE OPTIMIZATION;
ALTER TABLE main.silver.events INHERIT PREDICTIVE OPTIMIZATION;

SELECT * FROM system.storage.predictive_optimization_operations_history LIMIT 20;
```

**ANALYZE is not empty:** it collects statistics for the query optimizer (e.g. table size/cardinality estimates to inform join planning); it does not rewrite files. **INHERIT** uses the parent catalog/schema setting. If AUTO never chooses keys, check predictive optimization and workload eligibility. If an external table has no predictive optimization history, it is outside the supported managed-table scope. The source lists Premium+, supported region, and suitable runtime/warehouse prerequisites; exact platform requirements evolve.

## 6.5 Compute Troubleshooting

**Where to look:** Compute Event Log = lifecycle/termination codes; Driver Logs = stdout/stderr/Log4j; Init Script Logs = initialization failure; Spark UI Executors/Stages = memory, GC, failed tasks, spill; Job Run Details = queue/setup/install/execution durations. Event-log retention and Spark UI session history are limited; configure log delivery if long-term evidence is needed.

| Code / symptom | Meaning | First action |
|---|---|---|
| `CLOUD_PROVIDER_LAUNCH_FAILURE` | VM launch failure | Retry, check provider capacity |
| `CLOUD_PROVIDER_RESOURCE_STOCKOUT_DUE_TO_MISCONFIG` | Restricted node type/zone unavailable | Relax node/zone restrictions |
| `AWS_RESOURCE_QUOTA_EXCEEDED` | Cloud quota exceeded | Raise quota/change instance |
| `AWS_INSUFFICIENT_FREE_ADDRESSES_IN_SUBNET_FAILURE` | Subnet IP exhaustion | Fix subnet/zone |
| `INIT_SCRIPT_FAILURE` | Init script failed | Inspect init script logs |
| `SPARK_STARTUP_FAILURE` | Spark driver startup failed | Check custom configs/init scripts |
| `INVALID_ARGUMENT` | Bad config, secrets or permissions | Correct flagged parameter |
| Library installation timeout | Package/network issue | Check repository/network, pin versions, retry |

AWS-prefixed codes are examples; Azure cloud error codes differ. Serverless avoids much user-managed cluster setup, but not all failures.

**Library precedence (key exam rule):** notebook-scoped `%pip` > compute-scoped libraries > Databricks Runtime built-ins. In Git folders, working directory and repo root may have even higher Python import precedence. Lower precedence includes non-Git current working directory and workspace files appended to `sys.path`. Pin dependencies; use `requirements.txt`, notebook-scoped libraries, or serverless environments as appropriate. Avoid init-script-based package installation and deprecated DBFS library storage.

```python
%pip install pandas==2.2.2
dbutils.library.restartPython()
```

**OOM diagnosis:**
- **Driver OOM:** huge `collect()`, `toPandas()`, or broadcast; do distributed filtering/aggregation first. Adding workers does not increase driver memory.
- **Executor OOM / spill:** skew, oversized shuffle partitions, wide rows, excess cache; tune partitions (`auto` where supported), address skew, select fewer columns, `unpersist()`, or choose memory-appropriate workers.
- **Python Worker OOM:** UDF/Pandas UDF batch or group too large; use Spark built-in functions or reduce batch/group size.

```python
# BAD: pulls huge dataset to the driver
pdf = spark.table("main.silver.events").toPandas()

# BETTER: distributed aggregation; collect only if resulting groups are small
summary = (spark.table("main.silver.events")
           .groupBy("event_type").count().toPandas())
```

**Common cases:** nightly Pending → subnet exhaustion; dev works/prod fails → mismatched Runtime/package versions; init-script edit breaks startup → init script logs; a join loses executors → skew/memory pressure; `toPandas()` crashes driver → keep processing distributed.

## Questions we discussed

1. **What is `dm_pdw`?** Synapse Dedicated SQL Pool DMV family (Dynamic Management / Parallel Data Warehouse), useful for distributed step diagnostics; Databricks has different monitoring surfaces.
2. **Where are Lakeflow Jobs and Spark UI?** Jobs & Pipelines → Jobs → Runs; classic compute → Spark UI; SQL Warehouse → Query History / Query Profile.
3. **Why is a Query Profile almost empty for `SHOW TABLES`?** Simple metadata commands have very little physical execution work to visualize.
4. **What is a Z-order Curve?** Bit-interleaved multi-dimensional ordering; physical data colocation improves Delta file-level data skipping, not physical Spark task partition assignment.
5. **What is High Cardinality?** Many distinct values; separate from skew. Use `COUNT(DISTINCT col)` to inspect, but there is no fixed threshold.
6. **What is Distributed Aggregation?** Parallel partial aggregates followed by a final combine; it reduces data moved across the cluster when supported.
7. **Is ANALYZE an empty operation?** No: it gathers optimizer statistics; Predictive Optimization may invoke it automatically.
8. **Why can adding workers fail to fix OOM?** Driver memory is separate; a single skewed task may remain oversized.
9. **How does OPTIMIZE differ from VACUUM?** Compaction vs cleanup of eligible old files.
10. **Why is Liquid Clustering preferred for new tables?** Flexible keys, incremental maintenance and support for automatic key selection.

## Exam traps / corrections

- **Z-ORDER ≠ partitioning;** files are reorganized, not permanently assigned to one node or Spark partition.
- **High cardinality ≠ bad Z-ORDER choice;** frequently filtered IDs may benefit.
- **AQE skew join handling ≠ automatic GROUP BY skew solution.**
- **OPTIMIZE ≠ VACUUM;** compaction can leave old files until retention cleanup.
- **ANALYZE ≠ file-level compaction;** it supports optimizer statistics.
- **Predictive Optimization applies to UC managed tables**, not ordinary external tables.
- **Notebook-scoped dependency can override runtime built-in** and break Python imports.
- **Never assume `VACUUM RETAIN 0 HOURS` is safe** or available without retention safety checks.
- **Source examples may be conceptual rather than executable**; prefer `DESCRIBE DETAIL` and supported SQL/API forms.

## Official documentation

- [Job performance](https://docs.databricks.com/aws/en/jobs/diagnose-job-performance)
- [Spark UI guide](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide)
- [OPTIMIZE](https://docs.databricks.com/aws/en/delta/optimize)
- [VACUUM](https://docs.databricks.com/aws/en/delta/vacuum)
- [Liquid Clustering](https://docs.databricks.com/aws/en/delta/clustering)
- [Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)
- [Library management](https://docs.databricks.com/aws/en/libraries/)
- [Compute termination codes](https://docs.databricks.com/aws/en/compute/troubleshooting/cluster-error-codes)

## Mock Exam 1 — Concept Clarification: Broadcast Join (Q35; answered correctly)

**Broadcast Hash Join** duplicates a **small** dimension table on executors to avoid shuffling the large fact table for that join.

```sql
-- orders: ~10M rows; customers: ~1K rows
SELECT /*+ BROADCAST(c) */
    o.order_id, o.amount, c.region
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

PySpark equivalent:

```python
from pyspark.sql.functions import broadcast
result = orders.join(broadcast(customers), on="customer_id", how="inner")
```

View the physical plan with `EXPLAIN FORMATTED SELECT ...` or `result.explain("formatted")`; look for `BroadcastHashJoin`.

**Exam distinctions:** 
- Broadcast ≠ repartitioning both big tables for a Shuffle Join.
- Raising `spark.sql.shuffle.partitions` changes the number/size of shuffle partitions, not the underlying total join volume.
- `CROSS JOIN` is not a performance substitute.
- A Broadcast Hint is safe only if a side is small enough to broadcast; the Mock Exam Q35 wording mentions three large tables but assumes some *small dimension* tables exist. Check input sizes/stats before applying the hint.
- This is Spark SQL/PySpark, **not C++ syntax**.
