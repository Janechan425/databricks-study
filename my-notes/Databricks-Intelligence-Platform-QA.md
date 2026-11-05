# Databricks Intelligence Platform — Questions and Clarifications

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
