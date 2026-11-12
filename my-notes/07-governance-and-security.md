---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-7, unity-catalog, governance, security]
---

# Domain 7 — Governance and Security (15%)

Source: [Chapter 07](../certifications/data-engineer-associate/07-governance-and-security/README.md). Personal exam-oriented consolidation covering **all five source sections**, practical SQL, questions raised during study, use cases, common errors and exam tips. Keep platform/version-specific restrictions in mind.

## 7.1 Unity Catalog Basics

**Unity Catalog (UC)** centralizes metadata, authorization, lineage and audit across governed data/AI assets.

```text
Metastore (regional governance container)
└── Catalog: prod
    └── Schema: silver
        ├── Table: patients
        ├── View: patient_summary
        └── Volume: documents
```

A **fully qualified name** is `catalog.schema.object` (e.g. `prod.silver.patients`); *metastore* is not part of the three-level SQL name. Catalogs can separate environments (`dev`/`prod`), schemas data layers (`bronze`/`silver`/`gold`), but neither arrangement is mandatory. UC securables also include functions, registered models, connections, external locations and storage credentials.

```sql
CREATE CATALOG IF NOT EXISTS prod;
CREATE SCHEMA IF NOT EXISTS prod.silver;
CREATE TABLE prod.silver.orders (order_id BIGINT, amount DECIMAL(12,2)) USING DELTA;
SHOW CATALOGS;
SHOW SCHEMAS IN prod;
SHOW TABLES IN prod.silver;
DESCRIBE CATALOG prod;
DESCRIBE SCHEMA prod.silver;
USE CATALOG prod;
USE SCHEMA silver;
SELECT * FROM orders; -- resolves to prod.silver.orders
CREATE VOLUME IF NOT EXISTS prod.silver.documents;
-- Governed file path: /Volumes/prod/silver/documents/...
```

**Storage credential vs external location:** credential = *identity/authorization for cloud storage* (e.g. Azure managed identity); external location = *cloud URL + credential* registered as a UC securable. An external table or external volume uses a path covered by a suitable location; UC permissions and cloud access must both work. `/Volumes/...` is for files in volumes, not a recommended external-table LOCATION.

**UC vs legacy Hive metastore:** UC has a three-part namespace, cross-workspace governance, fine-grained permissions, lineage and external-location controls. Legacy Hive often uses a two-part `schema.table` name with workspace-focused controls. Metastore attachment/automatic UC enablement varies by workspace vintage.

**Common issues:** (1) table appears in wrong catalog → use fully qualified name or set current catalog/schema; (2) `INSUFFICIENT_PERMISSIONS` despite SELECT → check parent USE CATALOG and USE SCHEMA; (3) missing UC assets → inspect metastore assignment, workspace bindings and effective permissions rather than assuming one cause.

**Exam tips:** `catalog.schema.object`; managed vs external storage; `/Volumes/catalog/schema/volume/`; credential → external location → external table/volume; **USE privileges do not themselves grant SELECT**.

## 7.2 Managed vs External Tables

This is the **management/lifecycle type**, distinct from the **file/table format**. Both managed and external tables can be **Delta tables**. An external *Delta* table still has `_delta_log`, Parquet data files, ACID, MERGE, schema enforcement/evolution, history and time travel *subject to retention*. External is **not** synonymous with non-Delta or incremental loading.

| Property | UC managed | UC external |
|---|---|---|
| Create | No LOCATION (default) | Explicit cloud LOCATION governed by external location |
| Data lifecycle/layout | UC managed | User manages underlying cloud path |
| Formats | Delta (default) or supported Iceberg | Delta and supported file formats |
| DROP TABLE | Metadata removed; UNDROP recovery window, then data removed | UC metadata removed, **files remain** |
| Predictive optimization | Eligible managed tables | Not eligible |
| Best fit | Most new Bronze/Silver/Gold tables | Existing lake, external file writers/clients |

```sql
CREATE TABLE prod.silver.customers (
  customer_id BIGINT, email STRING, country STRING
) USING DELTA;

CREATE TABLE prod.gold.revenue AS
SELECT region, SUM(amount) AS revenue
FROM prod.silver.orders GROUP BY region;

CREATE TABLE prod.bronze.vendor_feed (id BIGINT, payload STRING)
USING DELTA
LOCATION 'abfss://raw@storageaccount.dfs.core.windows.net/vendor_feed/';

ALTER TABLE prod.silver.customers ADD COLUMN signup_date DATE;
ALTER TABLE prod.silver.customers RENAME COLUMN email TO email_address;
ALTER TABLE prod.silver.customers SET TBLPROPERTIES ('source_system'='crm');
COMMENT ON TABLE prod.silver.customers IS 'One row per customer after deduplication';
ALTER TABLE prod.silver.customers OWNER TO `data-platform-admins`;

DROP TABLE prod.silver.customers;
UNDROP TABLE prod.silver.customers; -- default managed-table recovery period: 7 days
DROP TABLE prod.bronze.vendor_feed; -- external files remain

-- For supported external Delta tables:
ALTER TABLE prod.bronze.vendor_feed SET MANAGED;
-- Roll back conversion within the supported 14-day window:
ALTER TABLE prod.bronze.vendor_feed UNSET MANAGED;
```

**External CREATE privileges:** parent `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, plus `CREATE EXTERNAL TABLE` on covering external location. On external metadata changed by another compatible system, inspect whether `MSCK REPAIR TABLE ... SYNC METADATA` is needed; not a routine substitute for Delta commits.

**Conversion details (source guide):** `SET MANAGED` needs a supported Delta table and suitable runtime (DBR 17.3 LTS+ or serverless in source); copies files/log and catches up commits, preserving table name, permissions and history; there may be a short write pause. Existing read/write streams need restarting. UniForm/Iceberg-read configurations may need `SET MANAGED TRUNCATE UNIFORM HISTORY`. `UNSET MANAGED` (within 14 days) points back to original location, preserving post-conversion writes; streams must restart. Special version-vs-timestamp time-travel constraints apply to parts of conversion/rollback history and older runtimes; avoid concurrent OPTIMIZE during conversion.

**UNDROP vs RESTORE vs UNSET MANAGED:** `UNDROP TABLE` recovers a dropped managed table; `RESTORE TABLE ... VERSION AS OF` restores a **data version**; `UNSET MANAGED` reverses a recent **table type conversion**. Neither RESTORE nor VACUUM changes a table from managed to external.

**Table metadata and documentation:** `TBLPROPERTIES` supports custom key/value annotations like `source_system=crm`, but reserved/system Delta properties such as `delta.logRetentionDuration` have real behavior. Table and column `COMMENT` are UC metadata, not rows. Comments can expose short business definitions, while dbt SQL/YAML and a Technical Specification/Technical Solution Design (TSD) retain full transformation rules and source-to-target mapping. With supported adapters and `persist_docs`, dbt model/column descriptions can populate UC comments. A custom `contains_pii=true` property does **not** automatically establish a governed tag or mask.

**Common issues:** external files survive DROP → delete cloud files separately if appropriate; accidentally dropped managed table → UNDROP within recovery window (7 days default; configurable where supported); LOCATION permission denied → missing CREATE EXTERNAL TABLE; streams fail after SET/UNSET MANAGED → restart.

**Exam tips:** distinguish storage management from Delta format; managed default/recommended and eligible for predictive optimization; external DROP leaves data; know SET/UNSET MANAGED (14-day rollback); distinguish RESTORE vs UNDROP.

## 7.3 Access Control and Permissions

**Principals:** user, **group** (preferred for people), **service principal** (production Jobs, CI/CD, bundles' `run_as`). Use backticks around special-character principal names.

**Privileges and inheritance:** grants on catalog/schema can apply to **current and future children**. Traversal and reading are different privileges. To read `prod.gold.revenue` you normally need **USE CATALOG + USE SCHEMA + SELECT**. Granting USE CATALOG **does not automatically grant** USE SCHEMA or SELECT, although multiple privilege *types* can be granted at catalog level and inherited.

```sql
-- Read one table (least privilege)
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA ON SCHEMA prod.gold TO `analysts`;
GRANT SELECT ON TABLE prod.gold.revenue TO `analysts`;

-- Read every existing and future table/view in Gold:
GRANT SELECT ON SCHEMA prod.gold TO `analysts`;

-- Broad catalog-wide access can be granted, but is often too permissive:
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG prod TO `analysts`;

-- Discover metadata but NOT row data:
GRANT BROWSE ON CATALOG prod TO `all_employees`;

-- Example write and create privileges:
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA prod.silver TO `engineers`;

REVOKE SELECT ON TABLE prod.gold.revenue FROM `analysts`;
SHOW GRANTS ON TABLE prod.gold.revenue;
SHOW GRANTS `analysts` ON SCHEMA prod.gold;
```

| Privilege | What it allows |
|---|---|
| USE CATALOG / USE SCHEMA | Required parent traversal; neither alone reads data |
| BROWSE | Discover names/metadata even without parent USE; no row access |
| SELECT | Read rows of table/view/MV (with parent USE) |
| MODIFY | Insert/update/delete data where supported |
| CREATE SCHEMA / CREATE TABLE / CREATE MATERIALIZED VIEW | Create respective objects |
| CREATE VOLUME / CREATE FUNCTION | Create respective objects |
| READ VOLUME / WRITE VOLUME | Read/write volume files |
| CREATE EXTERNAL TABLE | Register external table at authorized external location |
| READ FILES / WRITE FILES | Direct cloud-path file operations on authorized external location |
| EXECUTE / REFRESH | Run function/model or refresh materialized view |
| MANAGE | Administer grants/ownership and other management operations, subject to rules |
| ALL PRIVILEGES | Broad applicable grants, **not** ownership; excludes MANAGE and certain external-use/metadata privileges |

**GRANT vs REVOKE:** GRANT adds; REVOKE removes the grant for a specified principal at a specified level. REVOKE is **not a lasting deny override**. User can retain access via another group or inherited schema/catalog grant. To revoke access, identify *effective privileges* from all paths. A REVOKE can succeed even when no corresponding direct grant existed.

**DENY trap:** classic `DENY SELECT ...` is for legacy `hive_metastore`, not UC SQL access control. UC's ABAC DENY policy (Beta per source) concerns `MANAGE ACCESS CONTROL`, not a generic `DENY SELECT`. To fully hide a table, withhold/revoke SELECT (including inherited/group routes); to hide rows/values, use row filters, column masks, dynamic views or ABAC.

**Who can grant/transfer:** object owner, parent owner, qualifying MANAGE holder, metastore admin, subject to object-specific conditions. In Catalog Explorer use Object → Permissions → Grant/Revoke, and Owner on Overview. `ALTER TABLE ... OWNER TO `group`` changes ownership, not merely SELECT access. Least privilege, grouping, prod/dev separation, service principals for Jobs, and BROWSE for discovery are recommended.

**Audit:** `system.access.audit` can record UC permission and metadata actions (e.g. `updatePermissions`, `getTable`); metadata `getTable` is not proof of an actual data read. Inspect actual environment's schema and audit coverage.

**Common issues:** SELECT granted but USE parent missing; user retains access after REVOKE due to group/ancestor grant; classic DENY errors in UC; legacy privilege spelling (e.g. USAGE, CREATE_TABLE) instead of `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`.

**Exam tips:** choose *minimum privileges*; hierarchy **inherits identical grant types**, not every other privilege; BROWSE ≠ SELECT; ALL PRIVILEGES ≠ MANAGE or ownership; principals = users/groups/service principals.

## 7.4 Row Filters, Column Masks, Dynamic Views and ABAC

**Table-level row filter** is a SQL UDF returning `BOOLEAN`. For each candidate row, TRUE keeps row, FALSE/NULL filters it out; the function does **not** return a row. `OR` means **any** eligible group/region condition suffices; `AND` means both must hold.

```sql
CREATE OR REPLACE FUNCTION prod.security.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('admins')
    OR (is_account_group_member('canada_team') AND region = 'CA')
    OR (is_account_group_member('us_team') AND region = 'US');

ALTER TABLE prod.silver.patients
SET ROW FILTER prod.security.region_filter ON (country);
-- 'country' = actual table column; 'region' = function parameter name.
-- They need not share a name; passed value/type must match.
ALTER TABLE prod.silver.patients DROP ROW FILTER;
```

**Worked truth table:** For current user `canada_team` and row `country='CA'`: admin=FALSE, (CanadaGroup AND CA)=TRUE, (USGroup AND US)=FALSE → `FALSE OR TRUE OR FALSE = TRUE`, keep row. For same user on `country='US'`, all conditions FALSE → hide row. An admin gets TRUE for all rows. Group membership is user context, while country changes row-by-row. This is platform-enforced, unlike a user-controlled WHERE.

**Column mask:** SQL UDF returns original or obfuscated column value; return type must match or be castable to the column type. One directly assigned mask per column; may take other columns as input.

```sql
CREATE OR REPLACE FUNCTION prod.security.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('privacy_team') THEN email
  ELSE concat('***@', split(email, '@')[1])
END;

ALTER TABLE prod.silver.patients
ALTER COLUMN email SET MASK prod.security.mask_email;

ALTER TABLE prod.silver.patients
ALTER COLUMN email DROP MASK;
```

**Dynamic view** saves a SELECT expression that conditionally masks/filters or joins/aggregates; grant SELECT on the view (and parent USE) instead of base table. Views are not themselves directly assigned table-level row filters/masks.

```sql
CREATE OR REPLACE VIEW prod.gold.v_patients AS
SELECT patient_id,
       CASE WHEN is_account_group_member('privacy_team')
            THEN email ELSE '***' END AS email,
       country
FROM prod.silver.patients
WHERE is_account_group_member('admins') OR country = 'CA';
GRANT SELECT ON VIEW prod.gold.v_patients TO `analysts`;
```

**ABAC = Attribute-Based Access Control:** centrally apply policies to matching **governed tags** at catalog/schema/table level (metastore-level policies are Beta per source). A tag is **object metadata**, not a row or an additional column. `pii=email` consists of tag **key** `pii` and configured tag **value** `email`; neither equals a column's actual value `alice@example.com` or the mask output. Governed tags have centrally controlled definitions/allowed values/assignment permissions; a tag alone does not mask anything.

```sql
-- Assuming governed tag 'pii' and permitted value 'email' already exist:
ALTER TABLE prod.silver.patients
  ALTER COLUMN email SET TAGS ('pii' = 'email');

CREATE POLICY mask_pii_email
ON SCHEMA prod.silver
COMMENT 'Mask tagged email columns for analysts'
COLUMN MASK prod.security.mask_email
TO `analysts`
EXCEPT `privacy_admins`
FOR TABLES
MATCH COLUMNS has_tag_value('pii','email') AS email
ON COLUMN email;

SHOW EFFECTIVE POLICIES ON TABLE prod.silver.patients;
```

Policy clauses: `WHEN` matches tagged tables; `MATCH COLUMNS` selects tagged columns; `ON COLUMN` selects the masked column alias; `USING COLUMNS` passes matched columns to a row-filter UDF. `has_tag('region')` checks tag presence; `has_tag_value('pii','email')` checks key/value. A higher-scope policy is controlled by admins at that scope; table owners cannot just remove it.

**Example row-filter ABAC** (pre-existing BOOLEAN function `prod.security.us_only(region STRING)`):
```sql
CREATE POLICY us_rows_only
ON CATALOG prod
ROW FILTER prod.security.us_only
TO `contractors`
FOR TABLES
WHEN has_tag_value('sensitivity', 'high')
MATCH COLUMNS has_tag('region') AS region
USING COLUMNS (region);
```

**Requirements / limitations from the source guide:** table-level filters/masks require supported runtimes (below DBR 12.2 LTS may fail secure with no rows); dedicated compute has additional serverless FGAC/runtime constraints (15.4 LTS+, writes supported at later runtime). Time travel, cloning, OpenSharing and AI Search have restrictions for **table-level policies**; ABAC exemptions and supported sharing modes can differ. ABAC uses DBR 16.4+ or serverless in the source and requires policy management privileges plus UDF EXECUTE. Check current docs for version-specific support.

**Which to choose?** Specific table/column → table-level mask/filter. Many tagged datasets → ABAC. Curated transformed subset and no base-table access → dynamic view. Hide all data → do not grant SELECT.

**Common issues:** masked DECIMAL column with STRING-returning UDF → incompatible type; old runtime returns no rows → upgrade; time travel/clone on directly policy-protected table fails → plan history/security accordingly; table owner cannot drop ABAC mask inherited from schema/catalog → modify higher-level policy with appropriate permission.

**Exam tips:** Row filter RETURNS BOOLEAN and uses `SET ROW FILTER ... ON (column)`; column mask uses `SET MASK` and matching return type; `is_account_group_member()`; **ABAC = policy + governed tags**; no classic DENY SELECT in UC.

## 7.5 Data Sharing — Delta Sharing / OpenSharing (supplemental)

Source's August 2026 Associate outline does not separately test this section; still review vocabulary and practical scenarios. **OpenSharing** (formerly Delta Sharing in source materials) exposes governed data across organizations/workspaces via open HTTPS/REST interfaces, usually **without upfront copying an entire dataset**. Consumer access is read-only; data transfer/caching may occur while queries run, and freshness is not zero-latency.

| Role | Meaning |
|---|---|
| Provider | Organization exposing datasets |
| Share | Explicit collection of shared tables/eligible views |
| Recipient | Identity authorized to receive a share |
| Consumer | Organization/client querying it |
| Databricks-to-Databricks | UC sharing identifier and integrated workspace/catalog experience |
| Open/token sharing | Compatible external client plus secure activation/credential flow |

```sql
-- Provider
CREATE SHARE research_data COMMENT 'Approved anonymized dataset';
ALTER SHARE research_data ADD TABLE prod.gold.anonymized_encounters;
ALTER SHARE research_data ADD VIEW prod.gold.v_research_summary;
SHOW SHARES;
SHOW ALL IN SHARE research_data;
-- After a recipient is registered:
GRANT SELECT ON SHARE research_data TO `research_partner`;
REVOKE SELECT ON SHARE research_data FROM `research_partner`;
SHOW RECIPIENTS;
DESCRIBE RECIPIENT research_partner;
ALTER SHARE research_data REMOVE TABLE prod.gold.anonymized_encounters;
DROP RECIPIENT research_partner;

-- Consumer: requires provider/share identifier registered in recipient environment
CREATE CATALOG research_shared USING SHARE provider_name.research_data;
SELECT * FROM research_shared.gold.anonymized_encounters;
```

Share *specific* tables/views; new provider schema tables are **not automatically added** to a share. Use a curated aggregate/filtered view to avoid exposing raw PII. Version share interfaces intentionally (`partner_v1`/`partner_v2`); document data definitions using UC comments and dbt; audit with documented `system.access.audit` fields rather than inventing column names or system tables.

**Common issues:** protected table with direct table-level row filter/mask cannot be shared in unsupported modes → use an eligible view or validate ABAC sharing support; inactive/expired recipient activation → reconfigure recipient; new source table absent → `ALTER SHARE ... ADD TABLE`.

**Exam contrasts:** OpenSharing = share data for consumption elsewhere; Lakehouse Federation = *query* external database in place. REST = **Representational State Transfer**, an API architecture style; API = **Application Programming Interface**; a REST API commonly uses HTTP methods GET/POST/PUT/PATCH/DELETE.

## View vs Materialized View vs Delta Table — study follow-up

| Object | Stored query definition | Stored query result/data | Source change behavior |
|---|---|---|---|
| View | Yes | No result copy | Re-evaluates SQL at query time |
| Materialized view (MV) | Yes | Yes, precomputed results | Refresh required/scheduled; may be incremental or full |
| Regular table created via CTAS | Not automatically a maintained query | Yes | Remains snapshot until ETL updates it |
| Delta table | May be created by query, but normal table is not automatically query-maintained | Yes; Delta log + Parquet files | DML/stream/Job modifies data; ACID/time travel |

```sql
CREATE VIEW prod.gold.v_sales AS
SELECT region, SUM(amount) total_sales FROM prod.silver.orders GROUP BY region;

CREATE MATERIALIZED VIEW prod.gold.mv_sales AS
SELECT region, SUM(amount) total_sales FROM prod.silver.orders GROUP BY region;
REFRESH MATERIALIZED VIEW prod.gold.mv_sales;

CREATE TABLE prod.gold.sales_snapshot AS
SELECT region, SUM(amount) total_sales FROM prod.silver.orders GROUP BY region;
-- CTAS snapshot does not follow source changes automatically.
```

A materialized view may use Delta-backed storage but is a managed refreshable query result, not a normal writable Delta table. `REFRESH` can run incremental or full. Dynamic views are ordinary views with user-dependent filtering/masking.

## Final Exam Decision Table

| Asked for | Choose |
|---|---|
| Read a single UC table | USE CATALOG + USE SCHEMA + SELECT |
| Discover object names/descriptions but not rows | BROWSE |
| User retains access after REVOKE | Check group and inherited privileges |
| Prevent unauthorized table access | Withhold/revoke effective SELECT |
| Fine-grained row security | BOOLEAN-returning row-filter SQL UDF |
| Only finance can see unmasked column values | Column mask UDF with group check |
| One enterprise-wide rule for tagged PII columns | ABAC with governed tags |
| Partner gets curated result without base access | Dynamic view / eligible shared view |
| Recover mistakenly dropped managed table | UNDROP within configured window |
| Return external table type after SET MANAGED | UNSET MANAGED within conversion rollback window |
| Query live external DB | Lakehouse Federation |
| Share datasets to external recipient | OpenSharing / Delta Sharing |

## Official links
- [Unity Catalog privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/)
- [Managed/external tables](https://docs.databricks.com/aws/en/tables/managed)
- [Convert external to managed](https://docs.databricks.com/aws/en/tables/convert-external-managed)
- [Row filters and column masks](https://docs.databricks.com/aws/en/tables/row-and-column-filters)
- [ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/)
- [Delta Sharing](https://docs.databricks.com/aws/en/delta-sharing/)

## Mock Exam 1 — Concept Clarification: SHOW GRANTS and effective permissions (Q43; answered correctly)

**Authentication** = verifies identity; **Authorization** = determines permissions. **GRANT** adds permissions; **REVOKE** removes a grant; **SHOW GRANTS** inspects grants.

```sql
SHOW GRANTS ON TABLE main.silver.orders;
SHOW GRANTS `user@example.com` ON TABLE main.silver.orders;
SHOW GRANTS ON SCHEMA main.silver;
SHOW GRANTS ON CATALOG main;
```

Minimum read path for a specific table: **USE CATALOG + USE SCHEMA + SELECT**. Investigating a user's *effective access* also requires considering group membership, inherited privileges from schema/catalog and ownership. A direct table grant listing alone does not always tell the whole story.

**Classic SQL DENY SELECT is not a general UC alternative to REVOKE.** If a user still receives SELECT through a parent object or another group, revoking just the direct table grant will not necessarily block access. For per-row/per-column restrictions, use row filters / column masks or relevant governed policies.

**Not the same:** `SHOW SHARES` concerns Delta Sharing shares; it is not the normal command for table privileges.
