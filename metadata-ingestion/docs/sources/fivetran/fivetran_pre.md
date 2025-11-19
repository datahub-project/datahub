## Integration Details

This source extracts the following:

- Connectors in fivetran as Data Pipelines and Data Jobs to represent data lineage information between source and destination.
- Connector sources - DataJob input Datasets.
- Connector destination - DataJob output Datasets.
- Connector runs - DataProcessInstances as DataJob runs.

## Configuration Notes

1. Fivetran supports the [fivetran platform connector](https://fivetran.com/docs/logs/fivetran-platform) to dump the log events and connectors, destinations, users and roles metadata in your destination.
2. You need to setup and start the initial sync of the fivetran platform connector before using this source. Refer [link](https://fivetran.com/docs/logs/fivetran-platform/setup-guide).
3. Once initial sync up of your fivetran platform connector is done, you need to provide the fivetran platform connector's destination platform and its configuration in the recipe.
4. We expect our users to enable automatic schema updates (default) in fivetran platform connector configured for DataHub, this ensures latest schema changes are applied and avoids inconsistency data syncs.

### Database and Schema Name Handling

The Fivetran source uses **quoted identifiers** for database and schema names to properly handle special characters and case-sensitive names. This follows Snowflake's quoted identifier convention, which is then transpiled to the target database dialect (Snowflake, BigQuery, or Databricks).

**Important Notes:**

- **Database names** are automatically wrapped in double quotes (e.g., `use database "my-database"`)
- **Schema names** are automatically wrapped in double quotes (e.g., `"my-schema".table_name`)
- This ensures proper handling of database and schema names containing:
  - Hyphens (e.g., `my-database`)
  - Spaces (e.g., `my database`)
  - Special characters (e.g., `my.database`)
  - Case-sensitive names (e.g., `MyDatabase`)

**Migration Impact:**

- If you have database or schema names with special characters, they will now be properly quoted in SQL queries
- This change ensures consistent behavior across all supported destination platforms
- No configuration changes are required - the quoting is handled automatically

**Case Sensitivity Considerations:**

- **Important**: In Snowflake, unquoted identifiers are automatically converted to uppercase when stored and resolved (e.g., `mydatabase` becomes `MYDATABASE`), while double-quoted identifiers preserve the exact case as entered (e.g., `"mydatabase"` stays as `mydatabase`). See [Snowflake's identifier documentation](https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers) for details.
- **Backward Compatibility**: The system automatically handles backward compatibility for valid unquoted identifiers (identifiers containing only letters, numbers, and underscores). These identifiers are automatically uppercased before quoting to match Snowflake's behavior for unquoted identifiers. This means:
  - If your database/schema name is a valid unquoted identifier (e.g., `fivetran_logs`, `MY_SCHEMA`), it will be automatically uppercased to match existing Snowflake objects created without quotes
  - No configuration changes are required for standard identifiers (letters, numbers, underscores only)
- **Recommended**: For best practices and to ensure consistency, maintain the exact case of your database and schema names in your configuration to match what's stored in Snowflake

## Concept mapping

| Fivetran        | Datahub                                                                                               |
| --------------- | ----------------------------------------------------------------------------------------------------- |
| `Connector`     | [DataJob](https://docs.datahub.com/docs/generated/metamodel/entities/datajob/)                        |
| `Source`        | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)                        |
| `Destination`   | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)                        |
| `Connector Run` | [DataProcessInstance](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance) |

Source and destination are mapped to Dataset as an Input and Output of Connector.

## Current limitations

### Supported Destinations

Works only for:

- Snowflake destination
- Bigquery destination
- Databricks destination

### Ingestion Limits

To prevent excessive data ingestion, the following limits apply per connector:

- **Sync History**: Maximum of 500 sync runs per connector (controlled by `history_sync_lookback_period`)
- **Table Lineage**: Maximum of 120 table lineage entries per connector
- **Column Lineage**: Maximum of 1000 column lineage entries per connector

When these limits are exceeded, only the most recent entries are ingested. Warnings will be logged during ingestion to notify you when truncation occurs.

## Snowflake destination Configuration Guide

1. If your fivetran platform connector destination is snowflake, you need to provide user details and its role with correct privileges in order to fetch metadata.
2. Snowflake system admin can follow this guide to create a fivetran_datahub role, assign it the required privileges, and assign it to a user by executing the following Snowflake commands from a user with the ACCOUNTADMIN role or MANAGE GRANTS privilege.

```sql
create or replace role fivetran_datahub;

// Grant access to a warehouse to run queries to view metadata
grant operate, usage on warehouse "<your-warehouse>" to role fivetran_datahub;

// Grant access to view database and schema in which your log and metadata tables exist
// Note: Database and schema names are automatically quoted, so use quoted identifiers if your names contain special characters
grant usage on DATABASE "<fivetran-log-database>" to role fivetran_datahub;
grant usage on SCHEMA "<fivetran-log-database>"."<fivetran-log-schema>" to role fivetran_datahub;

// Grant access to execute select query on schema in which your log and metadata tables exist
grant select on all tables in SCHEMA "<fivetran-log-database>"."<fivetran-log-schema>" to role fivetran_datahub;

// Grant the fivetran_datahub to the snowflake user.
grant role fivetran_datahub to user snowflake_user;
```

## Bigquery destination Configuration Guide

1. If your fivetran platform connector destination is bigquery, you need to setup a ServiceAccount as per [BigQuery docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and select BigQuery Data Viewer and BigQuery Job User IAM roles.
2. Create and Download a service account JSON keyfile and provide bigquery connection credential in bigquery destination config.

## Databricks destination Configuration Guide

1. Get your Databricks instance's [workspace url](https://docs.databricks.com/workspace/workspace-details.html#workspace-instance-names-urls-and-ids)
2. Create a [Databricks Service Principal](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#what-is-a-service-principal)
   1. You can skip this step and use your own account to get things running quickly, but we strongly recommend creating a dedicated service principal for production use.
3. Generate a Databricks Personal Access token following the following guides:
   1. [Service Principals](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#personal-access-tokens)
   2. [Personal Access Tokens](https://docs.databricks.com/dev-tools/auth.html#databricks-personal-access-tokens)
4. Provision your service account, to ingest your workspace's metadata and lineage, your service principal must have all of the following:
   1. One of: metastore admin role, ownership of, or `USE CATALOG` privilege on any catalogs you want to ingest
   2. One of: metastore admin role, ownership of, or `USE SCHEMA` privilege on any schemas you want to ingest
   3. Ownership of or `SELECT` privilege on any tables and views you want to ingest
   4. [Ownership documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/ownership.html)
   5. [Privileges documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
5. Check the starter recipe below and replace `workspace_url` and `token` with your information from the previous steps.

## Configuration Options

### Fivetran REST API Configuration

The Fivetran REST API configuration is **required** for Google Sheets connectors and optional for other use cases. It provides access to connection details that aren't available in the Platform Connector logs.

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

**Note**: If you're using Google Sheets connectors, you must provide `api_config`. Without it, Google Sheets connectors will be skipped with a warning.

## Google Sheets Connector Support

Google Sheets connectors require special handling because Google Sheets is not yet natively supported as a DataHub source. As a workaround, the Fivetran source creates Dataset entities for Google Sheets and includes them in the lineage.

### Requirements

- **Fivetran REST API configuration** (`api_config`) is required for Google Sheets connectors
- The API is used to fetch connection details that aren't available in Platform Connector logs

### What Gets Created

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

### Limitations

- **Column lineage is disabled** for Google Sheets connectors due to stale metadata issues in the Fivetran Platform Connector (as of October 2025)
- This is a workaround that will be removed once DataHub natively supports Google Sheets as a source
- If the Fivetran API is unavailable or the connector details can't be fetched, the connector will be skipped with a warning

### Example Configuration

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

## Advanced Configurations

### Working with Platform Instances

If you have multiple instances of source/destination systems that are referred in your `fivetran` setup, you'd need to configure platform instance for these systems in `fivetran` recipe to generate correct lineage edges. Refer the document [Working with Platform Instances](https://docs.datahub.com/docs/platform-instances) to understand more about this.

While configuring the platform instance for source system you need to provide connector id as key and for destination system provide destination id as key.
When creating the connection details in the fivetran UI make a note of the destination Group ID of the service account, as that will need to be used in the `destination_to_platform_instance` configuration.
I.e:

<p align="center">
  <img width="70%"  src="https://github.com/datahub-project/static-assets/raw/main/imgs/integrations/bigquery/bq-connection-id.png"/>
</p>

In this case the configuration would be something like:

```yaml
destination_to_platform_instance:
  greyish_positive: <--- this comes from bigquery destination - see screenshot
    database: <big query project ID>
    env: PROD
```

#### Example - Multiple Postgres Source Connectors each reading from different postgres instance

```yml
# Map of connector source to platform instance
sources_to_platform_instance:
  postgres_connector_id1:
    platform_instance: cloud_postgres_instance
    env: PROD

  postgres_connector_id2:
    platform_instance: local_postgres_instance
    env: DEV
```

#### Example - Multiple Snowflake Destinations each writing to different snowflake instance

```yml
# Map of destination to platform instance
destination_to_platform_instance:
  snowflake_destination_id1:
    platform_instance: prod_snowflake_instance
    env: PROD

  snowflake_destination_id2:
    platform_instance: dev_snowflake_instance
    env: PROD
```
