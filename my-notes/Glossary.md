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
