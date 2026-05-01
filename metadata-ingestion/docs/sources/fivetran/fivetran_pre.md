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
3. Configure the destination platform (Snowflake, BigQuery, or Databricks) in your recipe

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

To use the Fivetran REST API integration, you need:

**Required API Permissions**:

- **Read access** to connection details (`GET /v1/connections/{connection_id}`)
- The API key must be associated with a user or service account that has access to the connectors you want to ingest
- The API key inherits permissions from the user or service account it's associated with

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
