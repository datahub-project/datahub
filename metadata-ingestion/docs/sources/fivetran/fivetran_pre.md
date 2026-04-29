### Overview

The `fivetran` module ingests metadata from Fivetran into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

#### Integration Details

This source extracts the following:

- Connectors in fivetran as Data Pipelines and Data Jobs to represent data lineage information between source and destination.
- Connector sources - DataJob input Datasets.
- Connector destination - DataJob output Datasets.
- Connector runs - DataProcessInstances as DataJob runs.

#### Configuration Notes

**Prerequisites:**

1. Set up and complete initial sync of the [Fivetran Platform Connector](https://fivetran.com/docs/logs/fivetran-platform/setup-guide)
2. Enable automatic schema updates (default) to avoid sync inconsistencies
3. Configure the destination platform (Snowflake, BigQuery, Databricks, or Managed Data Lake) in your recipe

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

To use the Fivetran REST API integration, you need:

**Required API Permissions**:

- **Read access** to connection details (`GET /v1/connections/{connection_id}`)
- The API key must be associated with a user or service account that has access to the connectors you want to ingest
- The API key inherits permissions from the user or service account it's associated with

#### Fivetran Managed Data Lake Service

The [Fivetran Managed Data Lake Service](https://fivetran.com/docs/destinations/managed-data-lake-service) replicates data to S3 as Iceberg tables and exposes them through an Iceberg REST Catalog (Polaris / Snowflake Open Catalog) or AWS Glue.

**Use `log_source: rest_api` for Managed Data Lake destinations.** The REST mode reads the Fivetran log via API and discovers each destination's `service` per-call — no Snowflake catalog-linked database setup is required. By default, REST-discovered MDL destinations emit Iceberg URNs:

```
urn:li:dataset:(urn:li:dataPlatform:iceberg, <schema>.<table>, <env>)
```

The namespace is the Fivetran connector schema verbatim (no `fivetran_` prefix). This matches DataHub's iceberg source convention so URNs align if the same Iceberg / Polaris catalog is also ingested directly via the [Iceberg source connector](https://docs.datahub.com/docs/generated/ingestion/sources/iceberg).

##### Example recipe

```yaml
source:
  type: fivetran
  config:
    log_source: rest_api
    api_config:
      api_key: "${FIVETRAN_API_KEY}"
      api_secret: "${FIVETRAN_API_SECRET}"

    # Optional: align URNs with a separate Iceberg source connector
    destination_to_platform_instance:
      my_fivetran_destination_id:
        platform_instance: "polaris_us_west" # must match the Iceberg source recipe
        env: PROD
```

##### Managed Data Lake routing (Iceberg / Glue / S3 / GCS / ADLS)

Fivetran's REST API reports `service: managed_data_lake` for every Managed Data Lake destination, regardless of whether the underlying object storage is AWS S3, Google Cloud Storage, or Azure Data Lake Storage Gen2, and regardless of whether it's fronted by an Iceberg REST catalog (Polaris) or AWS Glue. The default URN routing is `iceberg` — correct for Polaris / Iceberg-REST setups across any of the three clouds. Override per destination by pinning `platform` in `destination_to_platform_instance`:

```yaml
config:
  destination_to_platform_instance:
    polaris_warehouse_a:
      platform: iceberg # default; emit `iceberg.<schema>.<table>` URNs
    glue_warehouse_b:
      platform: glue # emit `glue.fivetran_<schema>.<table>` URNs
      platform_instance: "glue_us_west" # match the Glue source recipe
      env: PROD
    s3_lake_c:
      platform: s3 # emit `s3.<bucket>/<prefix_path>/<schema>/<table>` URNs
    gcs_lake_d:
      platform: gcs # emit `gcs.<bucket>/<prefix_path>/<schema>/<table>` URNs
    adls_lake_e:
      platform: abs # emit `abs.<storage_account>/<container>/<prefix_path>/<schema>/<table>` URNs
```

**Iceberg (default, or `platform: iceberg`).** Emits `urn:li:dataset:(iceberg, <schema>.<table>, env)`. No extra config needed for Polaris / Iceberg-REST destinations — this is the fallback default for any MDL destination whose config doesn't trigger another auto-detect rule.

**Glue (`platform: glue`, or auto-detected).** Emits `urn:li:dataset:(glue, <database>.<schema>.<table>, env)` aligned with DataHub's [Glue source](https://docs.datahub.com/docs/generated/ingestion/sources/glue). **Auto-detected**: when Fivetran's `should_maintain_tables_in_glue: true` toggle is set on the destination (visible via `/v1/destinations/{id}`) and the user hasn't pinned `platform`, the connector defaults to `glue` automatically — no explicit `platform: glue` needed.

**You must supply `database` yourself for Glue routing.** Fivetran's REST API does not expose the actual Glue database name it creates, and the Fivetran docs do not document the Glue-table-naming convention (Fivetran shares one Glue database per region across all destinations in that region). Inspect your AWS Glue console to find the actual database name and configure it on the destination entry:

```yaml
destination_to_platform_instance:
  glue_warehouse_a:
    # platform: glue can be auto-detected from the MDL toggle, but you can
    # also pin it explicitly.
    database: fivetran_managed_data_lake_us_west_2 # actual Glue database name
    platform_instance: "glue_us_west" # match the Glue source recipe
```

The connector then composes `urn:li:dataset:(glue, <database>.<schema>.<table>, env)` using the `<schema>.<table>` from Fivetran's lineage record verbatim as the Glue table name. **Verify against your Glue catalog** that Fivetran's table names are formatted this way; if they aren't, the URN won't align with DataHub's Glue source URNs and lineage won't render.

Until `database` is set, Glue lineage edges are skipped with a structured warning (one per destination, not per edge — repeated edges on a misconfigured destination are silently skipped after the first warning).

**Object-storage routing (`platform: s3`, `gcs`, or `abs`).** Emits a path-style URN aligned with DataHub's [S3](https://docs.datahub.com/docs/generated/ingestion/sources/s3), [GCS](https://docs.datahub.com/docs/generated/ingestion/sources/gcs), or [Azure Blob / ADLS](https://docs.datahub.com/docs/generated/ingestion/sources/abs) sources. The path prefix is composed from fields in the Fivetran destination response (`/v1/destinations/{id}`) — no extra recipe configuration required:

| Platform | URN shape                                                                                 | Source of prefix in `/v1/destinations/{id}.config`                    |
| -------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `s3`     | `urn:li:dataset:(s3, <bucket>/<prefix_path>/<schema>/<table>, env)`                       | `bucket` + `prefix_path` (AWS-backed MDL)                             |
| `gcs`    | `urn:li:dataset:(gcs, <bucket>/<prefix_path>/<schema>/<table>, env)`                      | `bucket` + `prefix_path` (GCS-backed MDL)                             |
| `abs`    | `urn:li:dataset:(abs, <storage_account>/<container>/<prefix_path>/<schema>/<table>, env)` | `storage_account_name` + `container_name` + `prefix_path` (ADLS Gen2) |

For example, an AWS-backed MDL destination with `bucket: example-fivetran-lake` and `prefix_path: fivetran` writing the `sales.orders` table emits `urn:li:dataset:(s3, example-fivetran-lake/fivetran/sales/orders, PROD)`. To point lineage at a different layout (e.g., the same data mirrored under a different prefix in DataHub's storage source), override `database` on the same `destination_to_platform_instance` entry; the override is used as the URN prefix verbatim.

**Storage-source URN alignment (important).** Fivetran's MDL writes **Iceberg-format tables** to S3 / GCS / ADLS — meaning each `<schema>/<table>/` folder contains an Iceberg `metadata/` directory plus Parquet data files under `data/`. The Fivetran connector emits one URN per logical table at folder-level granularity (`<bucket>/<prefix>/<schema>/<table>`). For lineage to render against the dataset URNs produced by DataHub's [S3](https://docs.datahub.com/docs/generated/ingestion/sources/s3), [GCS](https://docs.datahub.com/docs/generated/ingestion/sources/gcs), or [ABS](https://docs.datahub.com/docs/generated/ingestion/sources/abs) source, that source must be configured to produce table-level URNs that match this shape — typically by setting `path_specs` to treat each `<schema>/<table>/` directory as a single dataset (e.g., `path: s3://example-fivetran-lake/fivetran/{table}/` with `table_name` resolved from the directory name). If the data-lake source is instead configured to emit one URN per Parquet file, the Fivetran-emitted URN will not align and lineage won't render. When in doubt, prefer `platform: iceberg` (default) and ingest the Polaris / Iceberg REST catalog with DataHub's [Iceberg source](https://docs.datahub.com/docs/generated/ingestion/sources/iceberg) — the URNs align by construction without requiring additional path-spec coordination.

##### Overriding the URN platform per destination

For non-MDL destinations, or to override the platform with something other than glue / iceberg (e.g. align with a Unity Catalog source connector), declare the platform explicitly in `destination_to_platform_instance`:

```yaml
destination_to_platform_instance:
  my_fivetran_destination_id:
    platform: unity_catalog
    platform_instance: "unity_us_west"
    env: PROD
```

A pinned `platform` skips REST destination discovery for that destination — useful when discovery is wrong or returns a service we don't recognize.

##### Migrating from `managed_data_lake_destination_config`

`destination_platform: managed_data_lake` and `managed_data_lake_destination_config` were removed in favour of REST mode. To migrate:

1. Replace the entire `fivetran_log_config` block with `log_source: rest_api` and an `api_config`.
2. Drop the old `catalog_type`, `glue_database_prefix`, and `preserve_case` fields from inside `managed_data_lake_destination_config` — REST mode handles all catalog backings via `service` discovery and does not need SQL identifier transformations.
3. If you want to pin the URN platform for a destination (e.g. `glue` instead of `iceberg`), use `destination_to_platform_instance.<destination_id>.platform`. For Glue routing, also set `destination_to_platform_instance.<destination_id>.database` to the actual Glue database name from your AWS Glue console — the connector no longer derives the Glue database name automatically (the Fivetran-prefix-the-schema convention turned out to be unverifiable).

#### Hybrid deployments and destination discovery

If your Fivetran setup has a single account-level Fivetran Platform Connector delivering log data to one destination (typically Snowflake) but actual data is spread across destinations of different types (e.g., Snowflake for some connectors, Managed Data Lake for others), the per-recipe `destination_platform` field can only describe one destination's type at a time.

Whenever `api_config` is set, the connector automatically consults the [Fivetran REST API](https://fivetran.com/docs/rest-api/api-reference/destinations) for each destination whose `platform` isn't pinned in `destination_to_platform_instance`, and emits URNs based on the discovered `service`:

```yaml
source:
  type: fivetran
  config:
    fivetran_log_config:
      destination_platform: snowflake # where the log lives
      snowflake_destination_config:
        # ... your Snowflake log destination details ...
    api_config:
      api_key: "${FIVETRAN_API_KEY}"
      api_secret: "${FIVETRAN_API_SECRET}"
```

Discovery results are cached per-ingest, so each unique `destination_id` triggers at most one REST call.

**Precedence:** declarative entries in `destination_to_platform_instance` always win over discovery — use them to override an inaccurate REST result or fix one destination without touching the rest. Pinning `platform` on an entry also skips the REST round-trip for that destination.

**Failures:** if the REST call fails for a destination, the connector logs a structured warning and falls back to the recipe's default `destination_platform`. The ingest does not abort. Set the override explicitly via `destination_to_platform_instance` to bypass discovery for that destination.

**MDL destinations:** REST-discovered Managed Data Lake destinations default to `iceberg` URN routing. Override per-destination via `destination_to_platform_instance.<id>.platform` if you need a different platform (e.g. `glue`).

#### Choosing between `log_database` and `rest_api` modes

The connector reads metadata from two possible providers — the Fivetran Platform Connector log warehouse (DB) and the Fivetran REST API. Each provider supplies a different set of capabilities; the connector composes them based on which credentials you provide.

##### Capability matrix

| Feature                                                          | DB log                        | REST API                                                                                                                                                                                                                                               |
| ---------------------------------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Connector list / metadata                                        | ✅ (1 SQL query)              | ✅ (paginated per group)                                                                                                                                                                                                                               |
| Source platform (from connector type)                            | ✅                            | ✅                                                                                                                                                                                                                                                     |
| Destination platform routing (Snowflake / BigQuery / Databricks) | ✅ via `destination_platform` | ✅ via `/v1/destinations/{id}` discovery                                                                                                                                                                                                               |
| Managed Data Lake → Iceberg URN (Polaris / Iceberg REST)         | ❌                            | ✅ default for `service: managed_data_lake`                                                                                                                                                                                                            |
| Managed Data Lake → Glue / S3 / GCS / ABS URN routing            | ❌                            | ✅ via `destination_to_platform_instance.<id>.platform` (covers AWS, GCS, ADLS Gen2 backings)                                                                                                                                                          |
| Read Fivetran log when log destination is Managed Data Lake      | ❌                            | n/a — REST does not read a log database                                                                                                                                                                                                                |
| Table lineage (with historical, including disabled tables)       | ✅ historical                 | ⚠️ current enabled config only                                                                                                                                                                                                                         |
| Column lineage with source/destination column names              | ✅                            | ⚠️ table-only by default. Schemas-config endpoint only returns user-modified columns; full per-column metadata lives at a separate per-table endpoint (per-table API fanout, future enhancement). Use DB-primary hybrid for full column lineage today. |
| User / owner emails                                              | ✅ (1 SQL query)              | ✅ (paginated per group)                                                                                                                                                                                                                               |
| Sync history → DataProcessInstance events                        | ✅                            | ❌ (Fivetran REST has no sync-history endpoint; restored in REST-primary hybrid via DB log)                                                                                                                                                            |
| Rich sync-failure detail (`end_message_data` JSON)               | ✅                            | ❌                                                                                                                                                                                                                                                     |
| Hashed / PII column flags                                        | ✅                            | ⚠️ partial                                                                                                                                                                                                                                             |
| Google Sheets connection config (`sheet_id`, `named_range`)      | ❌                            | ✅ (REST is the only source)                                                                                                                                                                                                                           |

##### Credential coverage — what's available per config combination

| Configuration                               | What you get                                                                                                                                                                                                                                                                                                         | What's not available                                                                                                                                                                                                                                                             |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fivetran_log_config` only                  | Full happy path for Snowflake / BigQuery / Databricks log setups: connectors, lineage with historical view, owner emails, DPI events, rich failure detail.                                                                                                                                                           | Managed Data Lake destinations (Iceberg / Polaris); per-destination URN routing for hybrid accounts with mixed destination types; Google Sheets connection config.                                                                                                               |
| `api_config` only                           | All structural metadata for any destination type, including Managed Data Lake (Iceberg / Glue URN routing). Table-level lineage from `/v1/connections/{id}/schemas`. Works without warehouse credentials.                                                                                                            | Full column lineage (the schemas-config endpoint only returns user-modified columns; full columns require a per-table API fanout, a future enhancement). DataProcessInstance events (no REST sync-history endpoint); rich failure detail; historical lineage of disabled tables. |
| Both `fivetran_log_config` and `api_config` | Recommended for full coverage. DB-primary by default (DB owns connectors / lineage / users / jobs; REST owns destination routing and Google Sheets details). Set `log_source: rest_api` for REST-primary hybrid — REST owns connectors / lineage / routing, DB log fills in DPI events plus higher-fidelity lineage. | Connectors visible only in destinations other than the configured `fivetran_log_config` warehouse won't get DPI events (run history requires log access).                                                                                                                        |

##### Which `log_source` value to pick

`log_source` is optional — leave it unset and the connector infers the right
value from the credential blocks you supply:

| Credentials provided                 | Inferred `log_source`       | What you get                                                                                                                                                                                                                                                                                                     |
| ------------------------------------ | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fivetran_log_config` only           | `log_database`              | DB owns everything: connectors, table+column lineage, users, jobs, run history. No destination discovery.                                                                                                                                                                                                        |
| `api_config` only                    | `rest_api`                  | REST owns connectors, table-level lineage, users, destination routing, Google Sheets. Full column lineage requires DB credentials (use DB-primary or hybrid). No DataProcessInstance run-history.                                                                                                                |
| Both blocks                          | `log_database` (DB-primary) | DB owns connectors / lineage / users / jobs. REST fills in destination routing + Google Sheets details. Recommended for full coverage.                                                                                                                                                                           |
| Both blocks + `log_source: rest_api` | `rest_api` (REST-primary)   | REST owns connectors / schemas / users / destination routing. DB log fills in per-run DataProcessInstance events (REST has no sync-history endpoint) AND lineage (DB log carries explicit `source_column_name` / `destination_column_name` from sync events, slightly higher fidelity than REST schemas-config). |

Set `log_source` explicitly only when you have both blocks and want REST-primary
routing — the explicit value overrides the DB-primary default.

##### Hybrid mode (REST API + DB log)

REST mode by itself emits structural metadata (DataFlow, DataJob, datasets, full table+column lineage from `/v1/connections/{id}/schemas`) but no per-run `DataProcessInstance` events — Fivetran's REST API doesn't expose a sync-history endpoint.

REST-primary hybrid plugs that gap. Provide both blocks with `log_source: rest_api` and the connector queries the DB log warehouse for two things:

- **Sync history** — DB log's `sync_logs` query produces `DataProcessInstance` events, one per recent sync run (controlled by `history_sync_lookback_period`).
- **Higher-fidelity lineage** — the DB log's `column_lineage` table carries explicit `source_column_name` / `destination_column_name` written by the Fivetran Platform Connector during each sync. When the DB lineage reader is wired in, the REST reader prefers it over the schemas-config endpoint. If the DB lineage query fails transiently, the REST reader falls back to the schemas-config endpoint and emits a one-shot warning.

The fallback chain in REST-primary hybrid is: DB log lineage → REST schemas-config endpoint. Both produce full column lineage; the DB log is preferred when available.

```yaml
source:
  type: fivetran
  config:
    log_source: rest_api
    api_config:
      api_key: "${FIVETRAN_API_KEY}"
      api_secret: "${FIVETRAN_API_SECRET}"

    # When this block is present alongside `log_source: rest_api`, REST owns
    # connectors / schemas / users / destination routing, and the log
    # warehouse fills in two things: per-run sync history (DataProcessInstance
    # events) and higher-fidelity lineage from the column_lineage table.
    fivetran_log_config:
      destination_platform: snowflake
      snowflake_destination_config:
        account_id: "${SNOWFLAKE_ACCOUNT_ID}"
        warehouse: "${SNOWFLAKE_WAREHOUSE}"
        username: "${SNOWFLAKE_USER}"
        password: "${SNOWFLAKE_PASS}"
        role: "${SNOWFLAKE_ROLE}"
        database: "${SNOWFLAKE_LOG_DB}"
        log_schema: "fivetran_log"
```

REST mode reads `connections`, `schemas` (for table+column lineage), `sync_history`, `users`, and `destinations` directly via the Fivetran API. No database connection. URN platform routing happens automatically per destination via the same REST endpoint.

```yaml
source:
  type: fivetran
  config:
    log_source: rest_api
    api_config:
      api_key: "${FIVETRAN_API_KEY}"
      api_secret: "${FIVETRAN_API_SECRET}"
    # destination_to_platform_instance still works as a per-destination override.
    destination_to_platform_instance:
      g1:
        platform_instance: "polaris_us_west"
        env: PROD
```

**Tradeoffs:** REST mode makes one or more API calls per connector instead of bulk SQL queries. For accounts with hundreds of connectors, expect noticeably more API requests during ingest (typically still well under Fivetran's per-minute rate limits). Sync-history detail is constrained to what the REST endpoint exposes — generally sufficient for run-status MCEs.

##### Performance and rate limits (REST mode)

Per-connector schema and sync-history fetches run in parallel. Two knobs control the trade-off between wall-clock time and Fivetran rate-limit pressure:

- **`rest_api_max_workers`** (default `4`, range `1`–`32`) — number of worker threads issuing concurrent HTTP calls. Higher values issue more requests/sec against the Fivetran API and reduce wall-clock time on accounts with many connectors. Set to `1` for fully sequential behaviour.
- **`rest_api_per_connector_timeout_sec`** (default `300`) — hard cap per connector. If a single REST call hangs, that connector is skipped with a warning instead of stalling the whole run.

If you start hitting Fivetran rate limits (HTTP 429s in the ingest log), **lower** `rest_api_max_workers` rather than raising it. The retry logic backs off on 429s, but reducing concurrency avoids the retries entirely.

For accounts with very large connectors that legitimately take minutes per call, raise `rest_api_per_connector_timeout_sec`. For smaller accounts where you'd rather fail fast on a hung request, lower it.

The per-connector limits — `max_jobs_per_connector`, `max_table_lineage_per_connector`, `max_column_lineage_per_connector` — apply equally in REST mode and bound the per-connector lineage payload to match the DB reader's behaviour.

#### Fivetran REST API Configuration

The Fivetran REST API configuration is **required** for Google Sheets connectors and optional for other use cases. It provides access to connection details that aren't available in the Platform Connector logs.

##### Setup

To obtain API credentials:

1. Log in to your Fivetran account
2. Go to **Settings** → **API Config**
3. Create or use an existing API key and secret

```yaml
api_config:
  api_key: "your_api_key"
  api_secret: "your_api_secret"
  base_url: "https://api.fivetran.com" # Optional, defaults to this
  request_timeout_sec: 30 # Optional, defaults to 30 seconds
```

#### Google Sheets Connector Support

Google Sheets connectors require special handling because Google Sheets is not yet natively supported as a DataHub source. As a workaround, the Fivetran source creates Dataset entities for Google Sheets and includes them in the lineage.

##### Requirements

- **Fivetran REST API configuration** (`api_config`) is required for Google Sheets connectors
- The API is used to fetch connection details that aren't available in Platform Connector logs

##### What Gets Created

For each Google Sheets connector, two Dataset entities are created:

1. **Google Sheet Dataset**: Represents the entire Google Sheet

   - Platform: `google_sheets`
   - Subtype: `GOOGLE_SHEETS`
   - Contains the sheet ID extracted from the Google Sheets URL

2. **Named Range Dataset**: Represents the specific named range being synced
   - Platform: `google_sheets`
   - Subtype: `GOOGLE_SHEETS_NAMED_RANGE`
   - Contains the named range identifier
   - Has upstream lineage to the Google Sheet Dataset

##### Limitations

- **Column lineage is disabled** for Google Sheets connectors due to stale metadata issues in the Fivetran Platform Connector (as of October 2025)
- This is a workaround that will be removed once DataHub natively supports Google Sheets as a source
- If the Fivetran API is unavailable or the connector details can't be fetched, the connector will be skipped with a warning

##### Example Configuration

```yaml
source:
  type: fivetran
  config:
    # Required for Google Sheets connectors
    api_config:
      api_key: "your_api_key"
      api_secret: "your_api_secret"

    # ... other configuration ...
```
