---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-3]
---

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
