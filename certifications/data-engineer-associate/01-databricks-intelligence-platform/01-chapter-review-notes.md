# Databricks Intelligence Platform — Chapter 1 Review Notes

> Personal Q&A review for the Databricks Certified Data Engineer Associate exam. This supplements the chapter notes rather than replacing them.

## 1. Lakehouse Architecture and Governance

**Data lake vs. data warehouse vs. lakehouse.** A data lake stores raw and varied data flexibly, usually in cloud object storage. A data warehouse emphasizes structured analytics, SQL, and managed reliability. A lakehouse combines flexible storage with transactional tables and support for BI, data engineering, and ML/AI. It can reduce unnecessary copies between systems, but cost and performance depend on the workload.

**Delta Lake vs. Spark vs. Unity Catalog.** Delta Lake is an open table/storage layer built on Parquet data files and a Delta transaction log (`_delta_log`). Apache Spark executes distributed transformations and queries. Unity Catalog (UC) provides centralized governance such as permissions, auditing, and lineage. A **catalog** is a logical namespace container, not the entire Unity Catalog governance service.

**Three-level namespace:** `catalog.schema.table`, for example `production.silver.orders`. Catalogs and schemas are governed objects, not DBFS mount paths. DBFS stands for Databricks File System; prefer Unity Catalog volumes and external locations for governed file access rather than legacy mounts.

**Cloud object storage:** S3 = Amazon Simple Storage Service (AWS); ADLS Gen2 = Azure Data Lake Storage (Azure); GCS = Google Cloud Storage (Google Cloud). These are storage services, not databases or compute clusters. They store the Parquet files and Delta logs.

**Data lineage clarification:** Delta transaction history records changes to a table. Cross-table and cross-workload data lineage is generally provided by Unity Catalog, not by Delta Lake alone.

## 2. Medallion Architecture

| Layer | Main purpose | Typical work |
| --- | --- | --- |
| Bronze | Preserve ingested source data | Raw ingestion, replay, auditability |
| Silver | Clean and integrate reliable detailed data | Deduplication, validation, standardization, source joins |
| Gold | Serve business-ready analytical data | KPIs, dimensional models, reporting tables, aggregates |

**Silver vs. Gold:** Both may use joins, transformations, and business rules. The distinction is **purpose**, not SQL syntax. Silver focuses on data quality and integration; Gold focuses on business consumption. Gold can contain detailed fact and dimension tables, not only aggregates.

## 3. Delta Lake Transactions and Schema

**ACID:** Atomicity (all-or-nothing commits), Consistency (valid states), Isolation (concurrent transaction behavior), Durability (committed changes persist).

**MVCC (Multi-Version Concurrency Control):** Readers use a consistent table snapshot while writers commit new versions. Delta uses transaction logs, immutable data-file references, and optimistic concurrency control (OCC). Conflicting concurrent writes can fail and require retries. MVCC depends on older referenced files remaining available.

**DML (Data Manipulation Language):** `INSERT`, `UPDATE`, `DELETE`, `MERGE`. **DDL (Data Definition Language):** `CREATE`, `ALTER`, `DROP`. `SELECT` is often classified as DQL, though SQL classifications vary.

**Schema enforcement vs. evolution:** Enforcement rejects incompatible writes by default. Evolution allows intentional, compatible changes such as adding columns using `mergeSchema`. Evolution does not disable all schema checks.

```python
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save(path)
```

**Data skipping:** `delta.dataSkippingNumIndexedCols = 32` means statistics are collected for the first 32 eligible columns in schema order (for applicable table/runtime configurations). It does **not** create 32 conventional indexes. Min/max/null statistics help skip files that cannot match a filter. Changing the setting does not automatically backfill all old file statistics.

**Column mapping:** `delta.columnMapping.mode = 'name'` separates logical column names from physical Parquet column names, enabling metadata-only rename/drop operations in supported environments. Check protocol and reader/streaming compatibility before enabling it.

```sql
ALTER TABLE employees
SET TBLPROPERTIES (
  'delta.dataSkippingNumIndexedCols' = '32',
  'delta.columnMapping.mode' = 'name'
);
```

## 4. VACUUM, Retention, and Time Travel

**Logical vs. physical deletion:** A `DELETE` or `UPDATE` changes the active Delta snapshot, potentially leaving obsolete Parquet files in storage. `VACUUM` physically removes eligible unreferenced files after the configured retention period.

**Long-running query risk:** A query reading an older snapshot may still need files that a newer table version no longer references. If an unsafe short `VACUUM` retention removes them, the query can fail with `FileNotFoundException`. Retention must account for the longest-running reads/writes and streaming delays. A seven-day default is generally safer than very short retention, but is not a universal guarantee.

```sql
VACUUM employees RETAIN 168 HOURS;
```

| Property | Typical default | What it controls |
| --- | --- | --- |
| `delta.logRetentionDuration` | `interval 30 days` | Delta transaction-log history retention |
| `delta.deletedFileRetentionDuration` | `interval 7 days` | Retention of logically removed data files relevant to cleanup |

**Time Travel requires both** historical transaction-log information **and** the historical data files. Keeping logs for 30 days does not guarantee 30 days of readable versions.

```sql
ALTER TABLE employees
SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days'
);

SELECT *
FROM orders
WHERE order_date >= CURRENT_DATE() - INTERVAL 30 DAYS;
```

**INTERVAL** denotes a duration (e.g., 30 days), not a recurring schedule.

## 5. Clone Operations and Metadata

| | Shallow clone | Deep clone |
| --- | --- | --- |
| Table metadata and Delta log | Independent | Independent |
| Existing data files | References source files | Copies data files |
| Creation and storage | Usually fast and lightweight | Usually slower and larger |
| Physical independence | Limited | Stronger |

```sql
CREATE TABLE employees_clone SHALLOW CLONE employees;
CREATE TABLE employees_backup DEEP CLONE employees;
```

**Shallow clone is more than a shortcut:** It has its own transaction log and snapshot. Deleting rows from the source does not automatically delete them from the clone. However, the clone may depend on source data-file retention. In supported Unity Catalog managed-table scenarios, Databricks can track and protect referenced files from source cleanup; do not assume every shallow clone has that protection.

**Metadata** describes the table: schema, properties, partitioning, file references, and transaction records. **Parquet files** contain the actual rows. Cloning does not copy the source's entire transaction history.

## 6. Compute Types, Configuration, and Cost

**Two different classification dimensions:**
- **Workload:** All-purpose compute (interactive development), jobs compute (automated execution), SQL warehouses (SQL/BI).
- **Management:** Classic/user-configured compute vs. Databricks-managed serverless compute.

Thus, **all-purpose and serverless are not mutually exclusive**: interactive notebooks can run on serverless compute. Classic jobs clusters are typically created for a run and terminated afterward; serverless jobs provision resources behind the scenes. SQL warehouse types include Serverless, Pro, and Classic; features and scaling behavior differ.

**Driver node:** Coordinates Spark execution. **Worker nodes:** Execute distributed tasks. **Single-node compute:** Runs without separate worker nodes.

**DBR (Databricks Runtime):** The Spark-based runtime. **LTS:** Long-Term Support release, often preferred for stable production workloads.

**Photon:** Databricks' native C++ vectorized query execution engine for supported SQL/DataFrame operations. It processes columnar batches, reduces per-row overhead, and optimizes CPU/cache and memory usage. Some operations can benefit from SIMD (single instruction, multiple data), but vectorization does not mean an entire batch runs in one CPU instruction. Spark still handles planning and unsupported execution paths; not every Python UDF benefits.

**Autoscaling vs. auto termination:** Autoscaling adjusts the number of worker nodes based on demand. Auto termination shuts down an idle cluster. Spark dynamic executor allocation is not the same thing as Databricks cluster autoscaling.

**Spot worker:** A worker running on discounted, interruptible cloud capacity. Spot can reduce cost, but reclaimed workers may cause task retries, delays, or failures. **Spot** is an instance-pricing/interruption choice; **autoscaling** controls capacity.

**DBU (Databricks Unit):** A Databricks billing unit for compute usage, not a direct CPU or RAM measure. Simplified platform charge = DBUs consumed × price per DBU. Classic compute generally also incurs underlying cloud VM charges; serverless pricing generally bundles underlying compute. DBUs are not directly interchangeable with Synapse DWUs.

**Instance pools:** Keep cloud instances ready to reduce classic cluster startup time, with possible idle-instance cost. **SLA (Service Level Agreement):** An agreed service target, such as finishing a data pipeline by 6:00 a.m. Faster startup can help meet a deadline but does not guarantee it.

**Right-sizing:** Choose suitable instance types, worker ranges, and idle termination based on measured workload and cost, rather than always selecting the largest cluster.

## 7. Notebooks and Export Formats

**Magic commands:** Special commands, generally placed at the beginning of a cell, that control how that cell is interpreted or execute notebook-specific actions.

| Command | Purpose |
| --- | --- |
| `%sql`, `%python`, `%scala`, `%r` | Select a cell language |
| `%md` | Render Markdown |
| `%run` | Run another notebook |
| `%pip` | Manage notebook Python packages |
| `%sh` | Run shell commands |
| `%fs` | File-system operations |

Different languages in the same notebook do not automatically share ordinary language variables.

**DBC (Databricks Archive):** A Databricks-specific archive for exporting/importing notebooks, including multiple notebooks. **HTML/PDF** are for static viewing; **IPYNB** is a Jupyter-compatible notebook format; **SOURCE** exports source text. DBC does not automatically back up compute, jobs, or data tables.

## 8. Terminology and Pronunciation

| Term | Meaning |
| --- | --- |
| Governance | Policies, permissions, audit, lineage, and oversight |
| UC | Unity Catalog |
| DBFS | Databricks File System |
| EDW | Enterprise Data Warehouse |
| DML / DDL | Data Manipulation / Definition Language |
| MVCC / OCC | Multi-Version / Optimistic Concurrency Control |
| DBR / LTS | Databricks Runtime / Long-Term Support |
| DBU | Databricks Unit |
| SLA | Service Level Agreement |
| DBC | Databricks Archive |
| Interval | A time duration or gap |
| Complement | To supplement or work well with; distinct from *compliment* (praise) |
| Apache | /əˈpætʃi/ |

## 9. Exam Self-Check

- [ ] Explain lakehouse vs. lake vs. warehouse, and identify Delta Lake, Spark, and Unity Catalog roles.
- [ ] Identify `catalog.schema.table` and distinguish UC from an individual catalog.
- [ ] Distinguish Bronze, Silver, and Gold by their **purpose**.
- [ ] Explain ACID, MVCC, OCC, and Delta's transaction log.
- [ ] Distinguish DML from DDL, and schema enforcement from evolution.
- [ ] Explain data skipping and column mapping.
- [ ] Explain `VACUUM`, retention, Time Travel dependencies, and `FileNotFoundException` risk.
- [ ] Compare shallow/deep clones and explain metadata vs. data files.
- [ ] Distinguish compute workload types from serverless/classic management.
- [ ] Explain Photon vectorization, spot workers, autoscaling, auto termination, DBUs, and SLAs.
- [ ] Recognize notebook magic commands and DBC export.
