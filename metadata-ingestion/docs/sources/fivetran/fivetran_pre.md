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

#### Fivetran Managed Data Lake Service (AWS Glue + Snowflake catalog-linked database)

The [Fivetran Managed Data Lake Service](https://fivetran.com/docs/destinations/managed-data-lake-service) replicates data to S3 as Iceberg tables and registers them in AWS Glue. The Fivetran Platform Connector log lands in the same Glue data lake, and Snowflake exposes the lake to analysts through a [catalog-linked database (CLD)](https://docs.snowflake.com/en/sql-reference/sql/create-database-catalog-linked) — a read-only Snowflake database whose schemas and tables are virtual surfaces over Glue objects.

To ingest a Fivetran Managed Data Lake destination:

1. Set `destination_platform: managed_data_lake` and supply `managed_data_lake_destination_config`.
2. Provide the Snowflake connection details that point at the catalog-linked database surfacing the Fivetran Platform Connector log.

The connector emits the destination dataset URN against the AWS Glue platform: `urn:li:dataset:(urn:li:dataPlatform:glue, <glue_database_prefix><schema>.<table>, <env>)`. The Glue database name follows the Fivetran convention `<glue_database_prefix><connector_schema>` (default prefix `fivetran_`).

##### `preserve_case` and catalog-linked databases

Snowflake auto-uppercases unquoted identifiers, but catalog-linked databases retain the original case-preserving identifiers from the underlying catalog (Glue/Iceberg). Recipes targeting Managed Data Lake default `preserve_case: true` so the log query reads from `"fivetran_metadata_<suffix>"` rather than `"FIVETRAN_METADATA_<SUFFIX>"`. The same flag is available on `snowflake_destination_config` for any Snowflake-CLD setup.

##### Matching the Glue source connector's `platform_instance`

Emitted destination URNs use the Glue platform. If you also ingest the same AWS Glue catalog with DataHub's [Glue source connector](https://docs.datahub.com/docs/generated/ingestion/sources/glue), both connectors must agree on `platform_instance` for the URNs to refer to the same datasets and for lineage to render end-to-end.

Use the existing `destination_to_platform_instance` mapping (keyed by the Fivetran destination ID, visible in the Fivetran UI under **Destinations → Overview**) to set the Glue-side `platform_instance` and `env`:

```yaml
destination_to_platform_instance:
  my_fivetran_destination_id:
    platform_instance: "glue_us_west" # must match what the Glue source recipe uses
    env: PROD
```

If you don't run a Glue source connector, the Fivetran-emitted Glue URNs simply stand alone — no `platform_instance` is required, but downstream entities you wire up later must use the same convention.

##### Example recipe

```yaml
source:
  type: fivetran
  config:
    fivetran_log_config:
      destination_platform: managed_data_lake
      managed_data_lake_destination_config:
        # Snowflake CLD coordinates
        account_id: "abc48144"
        warehouse: "COMPUTE_WH"
        database: "lh_source_fivetran_usw2"
        log_schema: "fivetran_metadata_<suffix>"

        # Credentials
        username: "${SNOWFLAKE_USER}"
        password: "${SNOWFLAKE_PASS}"
        role: "fivetran_log_reader"

        # Glue catalog settings
        glue_database_prefix: "fivetran_" # Fivetran default

    # Match the Glue source connector's platform_instance so URNs align.
    destination_to_platform_instance:
      my_fivetran_destination_id:
        platform_instance: "glue_us_west"
        env: PROD
```

##### Lineage scope

The destination URN points at the Glue table, not at the Snowflake CLD table that analysts query. The rendered lineage shows `<source> → <Glue Iceberg table>`; the corresponding Snowflake CLD table is ingested by the Snowflake source as a separate node.

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
