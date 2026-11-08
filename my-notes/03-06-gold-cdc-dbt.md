---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-3]
---

# Domain 3.06 — Gold Objects, CDC, dbt and Migration Choices

Source: [Gold Layer Objects](../certifications/data-engineer-associate/03-data-transformation-and-modeling/06-gold-layer-objects.md).

## Choosing Gold object types

| Object | Stored? | When computed / refreshed | Typical use |
|---|---|---|---|
| Delta Table | Yes | Custom job / INSERT / MERGE / CTAS | Fact/dimension with late corrections |
| View | No precomputed data | Recomputed when read | Light projection / semantic abstraction |
| Materialized View (MV) | Yes | Managed refresh, scheduled or triggered | Frequently queried aggregates |
| Streaming Table (ST) | Yes | Incremental pipeline processing | New events and managed CDC |

A Gold layer is a **business-ready layer**, not a requirement to use MV/ST. Delta Table means Delta transaction-log format; **not** Synapse's colloquial use of “delta load” for incremental ingestion.

## Streaming and historical UPDATE/DELETE
- A plain `STREAM(source)` design processes new input. It does **not** automatically reconcile arbitrary historical corrections in the source Delta table.
- A streaming Delta read may fail on upstream modifications without suitable change-handling configuration. Do not assume old rows are silently re-read.
- For source CDC events, use **AUTO CDC**, or use a batch Delta Table + custom `MERGE`; MV is another option when managed query refresh is appropriate.
- Full refresh can rebuild when the complete source history is available, but is costly.

## AUTO CDC event processing (SCD Type 1)

Source `main.bronze.claim_changes` must provide event records with keys, order and deletion metadata.

```sql
-- In a Lakeflow Spark Declarative Pipeline SQL source file
CREATE OR REFRESH STREAMING TABLE main.gold.fact_claim;

CREATE FLOW claim_cdc_flow AS AUTO CDC INTO main.gold.fact_claim
FROM STREAM(main.bronze.claim_changes)
KEYS (claim_id, line_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 1;
```

- **Composite keys:** `KEYS (claim_id, line_id)` matches both columns.
- **INSERT/UPDATE:** normal non-delete events upsert by key and sequence automatically; no separate INSERT/UPDATE clauses are required.
- **DELETE:** explicitly define which events remove rows via `APPLY AS DELETE WHEN`.
- **SCD Type 1:** latest state only; `STORED AS SCD TYPE 2` keeps historical versions with `__START_AT` / `__END_AT`.
- **IMPORTANT:** AUTO CDC applies **provided** change events; it does not detect source database UPDATE/DELETE by itself. Watermark extraction on `modified_at` may not capture DELETEs.
- Partially populated updates require careful field/NULL semantics; consider `IGNORE NULL UPDATES` only when NULL means “not supplied,” not “clear the value.”
- Typically one CDC flow per target table, but multiple flows can live in one Pipeline. One-time historic loads do not require AUTO CDC.

## MV refresh vs ST refresh
- **View:** latest source data reflected at query time.
- **MV:** precomputed result is updated when `REFRESH MATERIALIZED VIEW`, schedule or `TRIGGER ON UPDATE` runs; not instantaneous on every source write.
- **ST:** pipeline checkpoint tracks new stream input; automatic CDC needs explicit CDC processing rules.

## Synapse → Databricks with dbt
- **dbt Model:** SQL defining a target relation; choose `table`, `view`, `incremental` or adapter-supported MV/ST.
- **dbt Macro:** reusable Jinja SQL code generator; expanded at compilation rather than being a data-processing table.
- **dbt `ref()`:** resolves upstream model and registers DAG dependencies.
- **dbt incremental + MERGE:** works well for SQL models that receive changed rows; handle deletes/late arrivals explicitly.

```sql
-- models/gold/fact_claim.sql (illustrative dbt model)
{{ config(materialized='incremental',
          unique_key=['claim_id','line_id'],
          incremental_strategy='merge') }}
SELECT claim_id, line_id, amount, modified_at
FROM {{ ref('silver_claims') }}
{% if is_incremental() %}
WHERE modified_at >= (
    SELECT COALESCE(MAX(modified_at), TIMESTAMP '1900-01-01')
    FROM {{ this }}
)
{% endif %}
```

This watermark pattern is illustrative, not a general-purpose CDC replacement. For hard deletes, missing timestamps or late-arriving changes, design explicit handling.

**Exam distinction:** AUTO CDC belongs to Lakeflow pipeline CDC processing; a dbt incremental model that compiles a `MERGE` is a different way to update a Delta target.
