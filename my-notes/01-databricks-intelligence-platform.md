---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-1]
---
# Domain 1 — Databricks Intelligence Platform

Source: [Domain 1 materials](../certifications/data-engineer-associate/01-databricks-intelligence-platform/README.md).

## Concepts to connect
- **Lakehouse** combines data lake flexibility with warehouse management. Medallion layers define data refinement, not file formats.
- **Unity Catalog** is governance / namespace: `catalog.schema.table`. **Volume** is a governed logical place for non-tabular files in cloud storage; it is not a disk drive or a database storage quota.
- **Compute:** all-purpose/interactive vs jobs vs serverless; SQL warehouses serve SQL workloads. **Photon** accelerates supported queries with a vectorized execution engine; DBUs measure Databricks compute consumption.
- **Delta Lake** is a table format implemented with Parquet files and a `_delta_log` transaction log. A **Delta table** is not the same thing as an incremental load. Delta supports ACID, MERGE, table history and time travel.
- **Managed vs external** = who controls storage lifecycle; **Delta vs Parquet** = storage format; **Bronze/Silver/Gold** = architectural layer; **full vs incremental** = ingestion method. These are separate axes.
- **Time travel** requires transaction history **and** data files. `VACUUM` can remove obsolete files, restricting recoverable versions.

### Quick SQL examples
```sql
CREATE TABLE main.bronze.events (id BIGINT, payload STRING) USING DELTA;
DESCRIBE HISTORY main.bronze.events;
SELECT * FROM main.bronze.events VERSION AS OF 0;
```

### Exam traps
- A **Delta table can receive full overwrites or incremental MERGEs**; Delta does not by itself initiate incremental ingestion.
- A temporary view is session-scoped; a persistent view stores a query, not physical result rows.
- A shallow clone refers to source data files, whereas a deep clone copies them; neither is an excuse to ignore retention.
- `delta.logRetentionDuration` differs from `delta.deletedFileRetentionDuration`.

## Consolidated personal Q&A

> Personal study notes for the Databricks Certified Data Engineer Associate exam.

## Unity Catalog (UC) vs DBFS

**Unity Catalog (UC)** is Databricks' centralized governance layer for data and AI assets. Tables use the three-level namespace `catalog.schema.table`. Catalogs and schemas are logical governance objects, not filesystem mount paths.

**DBFS** stands for Databricks File System. Legacy DBFS mounts provided access to cloud storage through mounted paths. For new governed workloads, prefer Unity Catalog external locations and volumes instead of legacy DBFS mounts.

A common organization is to use catalogs for environments (`dev`, `prod`) and schemas for layers (`bronze`, `silver`, `gold`), although conventions vary.

## Shallow clone vs deep clone

A **shallow clone** creates a new Delta table with its own metadata and transaction history but references existing data files from the source. It is fast and storage-efficient, but it is not a fully independent backup.

A **deep clone** copies data files as well as the necessary metadata, making it more independent of the source.

### What if the source table deletes rows?

A SQL `DELETE` on a Delta table generally records removals in the Delta transaction log; existing Parquet files may still be physically present. A shallow clone can continue reading referenced files while those files remain available. However, if source files are later physically removed (for example, by `VACUUM`), the shallow clone may fail to read them. Do not rely on a shallow clone for long-term backup or data isolation.

## What does “metadata” mean in Delta Lake?

Metadata is information **about** the table and its data, rather than the actual row contents. Examples include:

- Table schema and column definitions
- Table properties and protocol information
- Delta transaction-log actions describing active data files
- Partition information and file statistics
- Transaction history and commit information

A shallow clone has its own Delta log and table identity, but can point to the source table's physical data files.

## Delta retention settings

`delta.logRetentionDuration` controls how long Delta transaction-log history is retained for cleanup, with a default of 30 days. It does **not** guarantee that every old table version can be queried for 30 days: time travel also depends on whether the required data files still exist.

```sql
ALTER TABLE employees
SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days'
);
```

Use the documented `interval 30 days` format. `interval` means a duration or time interval, not a scheduled action.

Distinguish this from `delta.deletedFileRetentionDuration`, which controls retention of logically removed data files relevant to `VACUUM`.

## Exam reminders

- **UC** governs assets and permissions; **DBFS** is a filesystem abstraction.
- **Shallow clone** shares physical data-file dependencies; **deep clone** copies data.
- **Metadata** describes schema, files, properties, and transactions; it is not the row data.
- **DELETE** is a logical table change; **VACUUM** can physically remove obsolete files.
- **Log retention** and **deleted-file retention** are different settings.
- **Time travel** needs both sufficient log history and available data files.

## Mock Exam 1 — Concept Clarifications (not missed questions; Q2/Q23)

### Compute selection: where a task runs vs how it is scheduled

| Concept | Responsibility | Typical use |
|---|---|---|
| Lakeflow Jobs | Orchestration: task DAG, triggers, dependencies, retries | Scheduled production workflows |
| Serverless Jobs compute | Compute for supported job tasks; platform manages capacity | Preferred default for many production jobs |
| Classic Jobs compute | Job-scoped configured compute | When specific cluster/runtime settings are required |
| All-purpose / interactive compute | Interactive notebooks and exploratory data analysis (EDA) | Development, exploration |
| SQL warehouse (including serverless) | SQL execution and BI | SQL queries / SQL tasks |
| Serverless pipeline compute | Runs Lakeflow Spark Declarative Pipelines | Streaming tables and materialized views |

A **Scheduled trigger** belongs to a Job; **Serverless Jobs compute** executes its tasks. A Job can also invoke a **Pipeline task**. Never confuse *trigger* (when to start), *compute* (where to execute), and *ingestion* (how new data is read).

**EDA hand-off example:** explore raw data interactively in a notebook; persist reusable output to a Delta table or governed volume; package stable transformation logic as a Job notebook or Pipeline; schedule it with Lakeflow Jobs.

**Exam clue:** interactive EDA → interactive/all-purpose compute; automated Notebook ETL → Lakeflow Jobs + serverless Jobs compute where supported. Modern serverless options mean classic Job clusters are not always the first choice.
