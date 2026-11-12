# Glossary

> Databricks Certified Data Engineer Associate — concise English terminology reference. Grouped by topic for revision.

## Platform, storage, and governance

| Term | Definition | Source |
| --- | --- | --- |
| Databricks Intelligence Platform | Unified platform for data engineering, analytics, governance, and AI workloads. | [Databricks](https://docs.databricks.com/) |
| Lakehouse | Architecture combining data lake flexibility with data warehouse reliability and governance. | [Lakehouse](https://docs.databricks.com/aws/en/lakehouse/) |
| Delta Lake | Open table format/storage layer providing ACID transactions and reliable data management on object storage. | [Delta](https://docs.databricks.com/aws/en/delta/) |
| Delta table | Table backed by Delta Lake data files and a transaction log. | [Delta](https://docs.databricks.com/aws/en/delta/) |
| Parquet | Columnar file format commonly used for analytics; Delta data files typically use Parquet. | [Delta](https://docs.databricks.com/aws/en/delta/) |
| Unity Catalog (UC) | Centralized governance system for data and AI assets, access control, and lineage. | [UC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) |
| Catalog | Top-level namespace in the three-part table name `catalog.schema.table`. | [UC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) |
| Schema (database) | Namespace within a catalog that groups tables, views, and other objects. | [UC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) |
| UC Volume | Governed storage for non-tabular files within Unity Catalog. | [Volumes](https://docs.databricks.com/aws/en/volumes/) |
| External location | UC securable associating a cloud-storage path with a storage credential. | [UC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) |
| DBFS | Databricks File System; legacy DBFS mounts are generally superseded by UC-governed access patterns. | [DBFS](https://docs.databricks.com/aws/en/dbfs/) |
| ADLS | Azure Data Lake Storage; Azure cloud storage commonly used as a lakehouse data source. | [Azure](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) |
| S3 | Amazon Simple Storage Service, AWS object storage. | [AWS](https://aws.amazon.com/s3/) |
| GCS | Google Cloud Storage, Google Cloud object storage. | [Google](https://cloud.google.com/storage) |
| Medallion architecture | Layered data design using Bronze (raw), Silver (validated/cleaned), and Gold (business-ready) datasets. | [Medallion](https://docs.databricks.com/aws/en/lakehouse/medallion) |
| Bronze | Raw or minimally transformed ingested data. | [Medallion](https://docs.databricks.com/aws/en/lakehouse/medallion) |
| Silver | Cleaned, validated, and enriched data. | [Medallion](https://docs.databricks.com/aws/en/lakehouse/medallion) |
| Gold | Curated data optimized for reporting and business consumption. | [Medallion](https://docs.databricks.com/aws/en/lakehouse/medallion) |
| Staging area | Intermediate landing area where data waits for further processing. | [Ingestion](https://docs.databricks.com/aws/en/ingestion/) |

## Delta Lake and SQL

| Term | Definition | Source |
| --- | --- | --- |
| ACID | Atomicity, Consistency, Isolation, Durability: transaction reliability properties. | [Delta](https://docs.databricks.com/aws/en/delta/) |
| MVCC | Multi-version concurrency control: readers can access consistent versions while writes occur. | [Delta](https://docs.databricks.com/aws/en/delta/) |
| Transaction log (`_delta_log`) | Delta metadata and commit history used to track table versions and files. | [Delta](https://docs.databricks.com/aws/en/delta/) |
| Metadata | Descriptive information about data, such as schema, table properties, file references, and transaction records. | [Delta](https://docs.databricks.com/aws/en/delta/) |
| Time travel | Querying earlier Delta table versions or timestamps while required history/data files remain available. | [Delta](https://docs.databricks.com/aws/en/delta/history) |
| VACUUM | Removes eligible obsolete data files; can limit historical time travel and affect shallow clones. | [VACUUM](https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum) |
| `delta.logRetentionDuration` | Table property controlling retention of Delta transaction-log history (default commonly 30 days). | [Properties](https://docs.databricks.com/aws/en/delta/table-properties) |
| `interval 30 days` | SQL-style duration literal meaning a 30-day interval. | [Properties](https://docs.databricks.com/aws/en/delta/table-properties) |
| Shallow clone | Delta clone with separate metadata that references source data files. | [Clone](https://docs.databricks.com/aws/en/delta/clone) |
| Deep clone | Delta clone that copies source data files as well as relevant table metadata. | [Clone](https://docs.databricks.com/aws/en/delta/clone) |
| Schema enforcement | Validation that incoming writes conform to a table's schema. | [Schema](https://docs.databricks.com/aws/en/tables/schema-enforcement) |
| Schema evolution | Controlled addition or modification of schema elements as new data arrives. | [Schema](https://docs.databricks.com/aws/en/delta/update-schema) |
| `mergeSchema` | Option enabling supported schema merging; meaning depends on whether it is applied to file reading or Delta writes. | [Schema](https://docs.databricks.com/aws/en/delta/update-schema) |
| DML | Data Manipulation Language: commands such as INSERT, UPDATE, DELETE, and MERGE. | [SQL](https://docs.databricks.com/aws/en/sql/language-manual/) |
| MERGE INTO | SQL operation to conditionally insert, update, or delete target rows using a source. | [SQL](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into) |
| CTAS | CREATE TABLE AS SELECT: create and populate a table from a query. | [SQL](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using) |
| Idempotent | Repeating an operation does not change the final result beyond its first successful application. | [COPY INTO](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/) |

## Ingestion and connectors

| Term | Definition | Source |
| --- | --- | --- |
| Batch ingestion | Processes a finite dataset during a run; full batch may reread the entire source. | [Ingestion](https://docs.databricks.com/aws/en/ingestion/) |
| Incremental batch | Periodically processes new/changed data, then terminates. | [Ingestion](https://docs.databricks.com/aws/en/ingestion/) |
| Streaming | Continuously running query that processes incoming data, typically in micro-batches. | [Streaming](https://docs.databricks.com/aws/en/structured-streaming/) |
| Micro-batch | Small bounded batch executed by a running streaming query. | [Streaming](https://docs.databricks.com/aws/en/structured-streaming/) |
| `availableNow=True` | Trigger that processes currently available data and then terminates the streaming query. | [Streaming](https://docs.databricks.com/aws/en/structured-streaming/triggers) |
| `processingTime` trigger | Trigger for a long-running stream to check for data at a configured interval; not a strict completion SLA. | [Streaming](https://docs.databricks.com/aws/en/structured-streaming/triggers) |
| Auto Loader | Databricks incremental cloud-file ingestion mechanism using `cloudFiles`. | [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) |
| `read_files()` | SQL table-valued function for reading files; `STREAM read_files()` supports incremental ingestion in streaming tables. | [read_files](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) |
| `COPY INTO` | SQL command for idempotently loading new files into a Delta table. | [COPY INTO](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/) |
| `FORMAT_OPTIONS` | COPY INTO settings controlling how source files are parsed/read. | [COPY INTO](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into) |
| `COPY_OPTIONS` | COPY INTO settings controlling load/write behavior, including supported target schema evolution. | [COPY INTO](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into) |
| Source schema merging | Combining compatible schemas from multiple source files into a unified read schema, e.g. for Parquet. | [COPY INTO](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into) |
| Checkpoint | Durable state/progress information used to recover a streaming query after interruption. | [Streaming](https://docs.databricks.com/aws/en/structured-streaming/) |
| RocksDB | Embedded key-value store used by Auto Loader to track file discovery state within checkpointing. | [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) |
| Exactly-once processing | Guarantee that data has a single committed processing effect under supported source/sink and checkpoint configurations. | [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) |
| SFTP | SSH File Transfer Protocol: secure file transfer over SSH, not a database. | [Ingestion](https://docs.databricks.com/aws/en/ingestion/) |
| Kafka | Distributed event-streaming platform often read through Spark Structured Streaming. | [Streaming](https://docs.databricks.com/aws/en/structured-streaming/) |
| Message bus | Infrastructure for publishing and consuming events/messages, such as Kafka or Kinesis. | [Ingestion](https://docs.databricks.com/aws/en/ingestion/) |
| Standard connector | Customizable ingestion approach using SQL/Spark APIs or integration code. | [Ingestion](https://docs.databricks.com/aws/en/ingestion/) |
| Managed connector | Prebuilt, Databricks-managed ingestion integration for supported sources (Lakeflow Connect). | [Lakeflow Connect](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/) |
| Partner connector | Third-party integration product connecting sources to Databricks. | [Ingestion](https://docs.databricks.com/aws/en/ingestion/) |
| CDC | Change Data Capture: identifying source inserts, updates, and deletes for downstream replication. | [Lakeflow Connect](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/) |
| JDBC | Java Database Connectivity: standard interface for connecting to relational databases. | [JDBC](https://docs.databricks.com/aws/en/connect/external-systems/jdbc) |
| REST API | HTTP-based application interface commonly used to extract data from services. | [Ingestion](https://docs.databricks.com/aws/en/ingestion/) |
| Lakeflow Connect | Databricks offering for managed data ingestion from supported external sources. | [Lakeflow Connect](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/) |
| Lakeflow Spark Declarative Pipelines | Declarative framework for building and managing batch/streaming data pipelines. | [Lakeflow](https://docs.databricks.com/aws/en/ldp/) |
| Streaming table | Pipeline-managed table incrementally populated from streaming sources. | [Lakeflow](https://docs.databricks.com/aws/en/ldp/) |
| Materialized view | Stored result of a query, refreshed to reflect changes in underlying data. | [Lakeflow](https://docs.databricks.com/aws/en/ldp/) |
| Lakeflow Jobs | Scheduling and orchestration service for running notebooks, pipelines, and other tasks. | [Jobs](https://docs.databricks.com/aws/en/jobs/) |
| Lakehouse Federation | Query external data sources without first copying them into Databricks. | [Federation](https://docs.databricks.com/aws/en/query-federation/) |
| Delta Sharing | Open protocol for secure data sharing across organizations/platforms. | [Sharing](https://docs.databricks.com/aws/en/delta-sharing/) |

## Compute, performance, and development

| Term | Definition | Source |
| --- | --- | --- |
| SQL Warehouse | Compute resource optimized for running Databricks SQL queries and BI workloads. | [SQL Warehouses](https://docs.databricks.com/aws/en/compute/sql-warehouse/) |
| Serverless compute | Databricks-managed compute that reduces infrastructure administration and startup overhead. | [Compute](https://docs.databricks.com/aws/en/compute/) |
| All-purpose compute | Interactive compute typically used for development and exploration. | [Compute](https://docs.databricks.com/aws/en/compute/) |
| Jobs compute | Compute provisioned for scheduled or automated workloads. | [Jobs](https://docs.databricks.com/aws/en/jobs/) |
| DBU | Databricks Unit: normalized measure used for Databricks compute billing. | [Pricing](https://www.databricks.com/product/pricing) |
| Photon | Databricks native vectorized query engine accelerating supported SQL/DataFrame workloads. | [Photon](https://docs.databricks.com/aws/en/compute/photon) |
| Vectorized execution | Processing batches of values together rather than row-by-row. | [Photon](https://docs.databricks.com/aws/en/compute/photon) |
| SLA | Service Level Agreement: committed service performance/availability targets. | [Compute](https://docs.databricks.com/aws/en/compute/) |
| Spot worker | Lower-cost interruptible cloud compute capacity, suitable for fault-tolerant workloads. | [Compute](https://docs.databricks.com/aws/en/compute/) |
| Spark stage | Group of tasks in a Spark execution plan, typically separated by shuffle boundaries; not a staging storage area. | [Spark](https://spark.apache.org/docs/latest/job-scheduling.html) |
| Notebook | Interactive document containing executable code and markdown cells. | [Notebooks](https://docs.databricks.com/aws/en/notebooks/) |
| Magic commands | Notebook directives such as `%sql`, `%python`, `%md`, and `%run`. | [Notebooks](https://docs.databricks.com/aws/en/notebooks/) |
| DBC | Databricks archive format for exporting/importing notebooks. | [Notebooks](https://docs.databricks.com/aws/en/notebooks/) |

## Quick exam contrasts

- **Standard vs Managed vs Partner:** build/manage custom ingestion logic vs Databricks-managed integrations vs third-party integrations.
- **Auto Loader vs COPY INTO:** scalable incremental cloud-file discovery and streaming integration vs idempotent SQL file loading.
- **Batch vs Streaming:** finite run vs continuously running query; both may process only incremental data.
- **Source vs target schema evolution:** reading a changing source schema is distinct from altering a Delta target schema.
- **RocksDB vs checkpoint:** RocksDB is a state-store engine; checkpoint is the broader recovery/progress location.
- **Shallow vs deep clone:** references source data files vs copies data files.
- **UC vs DBFS:** governance and securables vs legacy filesystem abstraction.

## Additional ingestion terms — schema, discovery, JDBC, and nested data

| Term | Definition | Source |
| --- | --- | --- |
| `Schema inference` | Automatic discovery of initial column names and data types from source files; Auto Loader persists inferred schemas at schemaLocation. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema) |
| `cloudFiles.inferColumnTypes` | Auto Loader option to infer specific types for JSON/CSV/XML rather than treating inferred columns as strings. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema) |
| `cloudFiles.schemaLocation` | Path storing Auto Loader's inferred schema and schema evolution history. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema) |
| `cloudFiles.schemaEvolutionMode` | Controls Auto Loader's response to new source columns: addNewColumns, addNewColumnsWithTypeWidening, rescue, failOnNewColumns, or none. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema) |
| `UnknownFieldException` | Auto Loader exception on an unexpected new field in addNewColumns mode; restart after its tracked schema updates. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema) |
| `_rescued_data` | Column containing unexpected or mismatched source fields when rescued data handling is enabled. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema) |
| `Directory listing` | Auto Loader file discovery mode that lists source directories to find new files. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) |
| `File notification` | Auto Loader discovery using cloud storage event notifications and queue infrastructure. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode) |
| `Managed file events` | Unity Catalog-based, Databricks-managed file-event discovery for supported Auto Loader workloads. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) |
| `Queue service` | Messaging infrastructure (e.g. Azure Queue Storage or Amazon SQS) storing event notifications; does not itself start a Databricks Job. | [Docs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode) |
| `Event Grid` | Azure service routing storage events such as BlobCreated to downstream handlers or queues. | [Docs](https://learn.microsoft.com/en-us/azure/event-grid/overview) |
| `REST` | Representational State Transfer: API architectural style commonly implemented over HTTP, not a wire protocol itself. | [Docs](https://www.rfc-editor.org/rfc/rfc9110) |
| `JDBC driver` | Implementation of JDBC database access that communicates using a database-specific protocol. | [Docs](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) |
| `dbtable` | Spark JDBC option specifying a source database table or aliased subquery; supports partitioned reads. | [Docs](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) |
| `partitionColumn` | Spark JDBC option naming the column used to partition parallel reads. | [Docs](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) |
| `lowerBound / upperBound` | Spark JDBC partition stride bounds; not filters excluding out-of-range rows. | [Docs](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) |
| `numPartitions` | Spark JDBC setting for number of read partitions and maximum concurrent JDBC connections. | [Docs](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) |
| `fetchSize` | JDBC fetch batch size, not a total row limit. | [Docs](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) |
| `dbutils` | Databricks Utilities APIs for secrets, filesystem operations, notebook widgets, and workflow helpers. | [Docs](https://docs.databricks.com/aws/en/dev-tools/databricks-utils) |
| `Secret scope` | Named group of secrets retrievable with Databricks utilities, used for credentials and tokens. | [Docs](https://docs.databricks.com/aws/en/security/secrets/) |
| `Watermark (incremental extraction)` | Last-seen value (often updated_at) used to extract only newer/changed source rows; deletes may be missed. | [Docs](https://docs.databricks.com/aws/en/ingestion/) |
| `Struct` | Spark complex data type containing named fields; access nested fields with dot notation. | [Docs](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) |
| `Array` | Spark complex data type containing ordered elements; often used for JSON lists. | [Docs](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) |
| `explode()` | PySpark function expanding each array element into a separate row; null/empty arrays produce no rows. | [Docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html) |
| `explode_outer()` | Like explode(), but preserves a row with a null expanded value for null/empty arrays. | [Docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode_outer.html) |

### Example: explode vs explode_outer

Input: `order_id=101, items=[A001, B002]` → `explode(items)` produces **two rows**: `101 | A001` and `101 | B002`. For `items=[]` or `items=NULL`, `explode` produces **zero rows**, whereas `explode_outer` produces **one row with NULL**.

See [Data Ingestion Follow-up Q&A](./Data-Ingestion-Follow-up-QA.md) for examples and exam comparisons.

## Domain 6 — Monitoring, Optimization and Troubleshooting

| Term | Definition | Source |
| --- | --- | --- |
| Job run | One execution instance of a Lakeflow Job. | [Jobs](https://docs.databricks.com/aws/en/jobs/) |
| Task dependency / upstream failure | A downstream task depends on successful or configured completion of an upstream task. | [Jobs](https://docs.databricks.com/aws/en/jobs/) |
| Repair run | Retry failed or skipped tasks of an existing job run where supported. | [Jobs](https://docs.databricks.com/aws/en/jobs/) |
| Job Matrix view | Jobs UI view for comparing task performance/status across runs. | [Jobs](https://docs.databricks.com/aws/en/jobs/) |
| Spark UI | Spark execution diagnostics for jobs, stages, tasks, executors, storage and SQL plans. | [Spark UI](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide) |
| Query Profile | SQL query execution/operator diagnostics, especially for SQL warehouses. | [Query Profile](https://docs.databricks.com/aws/en/sql/user/queries/query-profile) |
| System tables | Databricks-provided tables for operational, billing, audit and lineage data. | [System Tables](https://docs.databricks.com/aws/en/admin/system-tables/) |
| Dynamic Management View (DMV) | Synapse/SQL Server diagnostic views for requests and engine state; `dm_pdw_*` refers to Parallel Data Warehouse. | [Synapse](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-manage-monitor) |
| Distributed aggregation | Parallel partial aggregations followed by combining intermediate results. | [Spark](https://spark.apache.org/docs/latest/sql-performance-tuning.html) |
| Shuffle | Redistribution of data between Spark partitions, often for joins or aggregations. | [Spark](https://spark.apache.org/docs/latest/sql-performance-tuning.html) |
| Spill | Writing intermediate task data to disk when memory is insufficient. | [Spark](https://spark.apache.org/docs/latest/web-ui.html) |
| Data skew | Uneven data distribution causing some partitions/tasks to process much more than others. | [Spark](https://spark.apache.org/docs/latest/sql-performance-tuning.html) |
| Salting | Add a synthetic key component to split hot keys across aggregation/join tasks. | [Spark](https://spark.apache.org/docs/latest/sql-performance-tuning.html) |
| AQE | Adaptive Query Execution; runtime plan adaptation such as coalescing partitions and skewed join handling. | [Spark](https://spark.apache.org/docs/latest/sql-performance-tuning.html) |
| High cardinality | Column with many distinct values; not synonymous with skew. | [Notes](./06-troubleshooting-monitoring-optimization.md) |
| Low cardinality | Column with relatively few distinct values. | [Notes](./06-troubleshooting-monitoring-optimization.md) |
| Cardinality ratio | Distinct-value count divided by row count; a context-dependent descriptive metric. | [Notes](./06-troubleshooting-monitoring-optimization.md) |
| Z-order Curve | Space-filling ordering that interleaves coordinate bits to improve multidimensional locality. | [Delta](https://docs.databricks.com/aws/en/delta/data-skipping) |
| Bit interleaving | Alternating bits from multiple dimensions to form a sortable one-dimensional key. | [Notes](./06-troubleshooting-monitoring-optimization.md) |
| Z-ORDER | Delta file-layout optimization for data colocation and effective data skipping on eligible tables. | [Delta](https://docs.databricks.com/aws/en/delta/data-skipping) |
| Data skipping / file pruning | Avoid reading files whose statistics show they cannot satisfy a filter. | [Delta](https://docs.databricks.com/aws/en/delta/data-skipping) |
| OPTIMIZE | Delta maintenance command for compaction and supported data layout optimization. | [OPTIMIZE](https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize) |
| Compaction | Rewriting many small data files into fewer, larger files. | [OPTIMIZE](https://docs.databricks.com/aws/en/delta/optimize) |
| ANALYZE TABLE | Collect or update statistics used by the query optimizer. | [ANALYZE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-analyze-table) |
| Table statistics | Row counts, sizes or column statistics supporting query planning. | [ANALYZE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-analyze-table) |
| Optimized Writes | Write-time file-size/distribution optimization. | [Delta](https://docs.databricks.com/aws/en/delta/tune-file-size) |
| Auto Compaction | Automatic compaction of small files after writes under supported conditions. | [Delta](https://docs.databricks.com/aws/en/delta/tune-file-size) |
| Bloom filter | Probabilistic membership structure that may produce false positives but not false negatives. | [Delta](https://docs.databricks.com/aws/en/optimizations/bloom-filters) |
| Liquid Clustering | Flexible table data layout using up to four clustering keys, replacing traditional partitioning/Z-ORDER on the table. | [Clustering](https://docs.databricks.com/aws/en/delta/clustering) |
| Clustering key | Column used to organize related records for data skipping. | [Clustering](https://docs.databricks.com/aws/en/delta/clustering) |
| OPTIMIZE FULL | Recluster all eligible historical data using current liquid clustering keys on supported runtimes. | [Clustering](https://docs.databricks.com/aws/en/delta/clustering) |
| CLUSTER BY AUTO | Automatically select and evolve clustering keys; requires predictive optimization. | [Clustering](https://docs.databricks.com/aws/en/delta/clustering) |
| Predictive Optimization | Automated OPTIMIZE, VACUUM and ANALYZE for supported Unity Catalog managed tables. | [Predictive](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) |
| INHERIT PREDICTIVE OPTIMIZATION | Revert table/schema predictive optimization behavior to the parent setting. | [Predictive](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) |
| Compute Event Log | Cluster lifecycle events and termination reasons used for startup diagnostics. | [Compute](https://docs.databricks.com/aws/en/compute/clusters-manage) |
| Init script | Script executed during classic compute startup; failure can terminate cluster startup. | [Compute](https://docs.databricks.com/aws/en/init-scripts/) |
| Library precedence | Python package/import priority; notebook-scoped libraries generally override compute-scoped and runtime built-ins. | [Libraries](https://docs.databricks.com/aws/en/libraries/) |
| Version pinning | Specify exact package versions (e.g. pandas==2.2.2) for reproducibility. | [Libraries](https://docs.databricks.com/aws/en/libraries/) |
| Driver OOM | Driver out-of-memory, often from collecting too much data locally. | [Notes](./06-troubleshooting-monitoring-optimization.md) |
| Executor OOM | Executor out-of-memory, often from large partitions, skew or cached data. | [Notes](./06-troubleshooting-monitoring-optimization.md) |
| Python Worker OOM | Python worker memory exhaustion, often from oversized UDF batches/groups. | [Notes](./06-troubleshooting-monitoring-optimization.md) |


## Domain 7 — Governance and Security

| Term | Definition | Source |
| --- | --- | --- |
| Metastore | Regional root governance container for UC catalogs; not part of the three-level table name. | [Notes](./07-governance-and-security.md) |
| Fully qualified name | Three-level UC object reference: `catalog.schema.object`. | [Notes](./07-governance-and-security.md) |
| Securable object | UC object on which privileges can be granted, e.g. catalog, schema, table, view, volume or external location. | [UC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) |
| Storage credential | UC object representing a cloud access identity used with external locations. | [UC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) |
| Managed table | UC-managed data storage and lifecycle; DROP allows a configured recovery window before data cleanup. | [Tables](https://docs.databricks.com/aws/en/tables/managed) |
| External table | UC-registered table using a designated external storage path; DROP removes metadata, not cloud files. | [Tables](https://docs.databricks.com/aws/en/tables/external) |
| SET MANAGED | Supported Delta table conversion from external to UC-managed storage. | [Convert](https://docs.databricks.com/aws/en/tables/convert-external-managed) |
| UNSET MANAGED | Conversion rollback within its supported window (14 days per study source). | [Convert](https://docs.databricks.com/aws/en/tables/convert-external-managed) |
| UNDROP TABLE | Recover a dropped UC managed table inside its recovery period (7 days default in study source). | [Notes](./07-governance-and-security.md) |
| Table property (TBLPROPERTIES) | Table-level key/value metadata; custom keys are descriptive, system Delta keys can change behavior. | [Notes](./07-governance-and-security.md) |
| Table comment / column comment | Descriptive table/column metadata visible in UC; not row data or transformation execution. | [Notes](./07-governance-and-security.md) |
| TSD | Common abbreviation for Technical Specification Document or Technical Solution Design; company-specific business/technical logic specification. | [Notes](./07-governance-and-security.md) |
| dbt persist_docs | dbt configuration to publish supported model/column descriptions as database relation/column comments. | [Notes](./07-governance-and-security.md) |
| Principal | User, group or service principal receiving UC privileges. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| Service principal | Non-human identity for running automation, CI/CD and production jobs. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| Least privilege | Grant only the permissions necessary for a task at the narrowest practical scope. | [Notes](./07-governance-and-security.md) |
| Privilege inheritance | Catalog/schema grants also cover eligible current and future descendant objects; USE does not turn into SELECT. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| USE CATALOG | Required access gate to interact with objects under a catalog; not permission to read table rows. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| USE SCHEMA | Required access gate to interact with schema objects; not SELECT. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| SELECT | Data-reading privilege; usually needs USE CATALOG and USE SCHEMA as well. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| BROWSE | Discover permissible object names/descriptions without reading rows or requiring parent USE privileges. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| MODIFY | Privilege to insert/update/delete table data, subject to operation-specific requirements. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| MANAGE | Administrative privilege for supported object permissions/ownership operations; excluded from ALL PRIVILEGES. | [Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) |
| GRANT / REVOKE | Add/remove explicit privileges; REVOKE is not a persistent deny override and inherited/group grants may remain. | [Notes](./07-governance-and-security.md) |
| DENY (classic) | Legacy Hive-metastore privilege-blocking SQL; not supported as classic DENY on Unity Catalog. | [DENY](https://docs.databricks.com/aws/en/sql/language-manual/security-deny) |
| Effective privileges | Permissions remaining after considering directly granted, group and inherited privileges. | [Notes](./07-governance-and-security.md) |
| Row filter | Table-attached SQL UDF returning BOOLEAN per row; TRUE rows visible, FALSE/NULL rows filtered. | [Filters](https://docs.databricks.com/aws/en/tables/row-and-column-filters) |
| Column mask | Table-attached SQL UDF returning a value compatible with masked column type, conditional on identity. | [Masks](https://docs.databricks.com/aws/en/tables/row-and-column-filters) |
| is_account_group_member() | SQL function checking whether the querying principal belongs to a specified account group. | [Notes](./07-governance-and-security.md) |
| Dynamic view | SQL view that dynamically masks/filters or transforms results based on query context/identity. | [Notes](./07-governance-and-security.md) |
| Governed tag | Centrally controlled object/column classification metadata with governed assignment and allowed values. | [ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/) |
| Tag key / tag value | Metadata classification name/value, e.g. `pii=email`; neither is a row value or column name. | [ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/) |
| ABAC | Attribute-Based Access Control: applies centrally defined masks/filters using attributes such as governed tags. | [ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/) |
| has_tag() / has_tag_value() | ABAC tag predicates for tag presence versus a specified key/value. | [ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/) |
| Provider / Share / Recipient / Consumer | Provider exposes datasets in a share; recipient is authorized identity; consumer reads the shared data. | [Sharing](https://docs.databricks.com/aws/en/delta-sharing/) |
| OpenSharing | Open cross-workspace/organization data-sharing mechanism, formerly called Delta Sharing in course materials. | [Sharing](https://docs.databricks.com/aws/en/delta-sharing/) |
| REST | Representational State Transfer; architectural style for APIs, often implemented over HTTP. | [Notes](./07-governance-and-security.md) |
| API | Application Programming Interface; wider concept that includes REST APIs, JDBC interfaces and other integration styles. | [Notes](./07-governance-and-security.md) |
| Materialized view vs view | MV persists query results and refreshes them; ordinary view stores SQL definition without materialized result storage. | [Notes](./07-governance-and-security.md) |
