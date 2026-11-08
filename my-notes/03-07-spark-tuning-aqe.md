---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-3]
---

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
