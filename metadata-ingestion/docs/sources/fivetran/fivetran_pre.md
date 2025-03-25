## Integration Details

This source extracts the following:

- Connectors in fivetran as Data Pipelines and Data Jobs to represent data lineage information between source and destination.
- Connector sources - DataJob input Datasets.
- Connector destination - DataJob output Datasets.
- Connector runs - DataProcessInstances as DataJob runs.

## Configuration Notes

The Fivetran connector supports two operational modes:

### Enterprise Mode (Log-based)
1. Fivetran supports the fivetran platform connector to dump the log events and connectors, destinations, users and roles metadata in your destination.
2. You need to setup and start the initial sync of the fivetran platform connector before using this source. Refer [link](https://fivetran.com/docs/logs/fivetran-platform/setup-guide).
3. Once initial sync up of your fivetran platform connector is done, you need to provide the fivetran platform connector's destination platform and its configuration in the recipe.

### Standard Mode (API-based)
1. For users without access to the fivetran platform connector logs, you can use the standard mode which uses Fivetran's REST API.
2. You'll need to generate API credentials (API key and API secret) from the Fivetran dashboard.
3. Configure the `api_config` section in your recipe with these credentials.

## Concept mapping 

| Fivetran		   | Datahub												    |
|--------------------------|--------------------------------------------------------------------------------------------------------|
| `Connector`              | [DataJob](https://datahubproject.io/docs/generated/metamodel/entities/datajob/)       	            |
| `Source`                 | [Dataset](https://datahubproject.io/docs/generated/metamodel/entities/dataset/)                        |
| `Destination`            | [Dataset](https://datahubproject.io/docs/generated/metamodel/entities/dataset/)                        |
| `Connector Run`          | [DataProcessInstance](https://datahubproject.io/docs/generated/metamodel/entities/dataprocessinstance) |

Source and destination are mapped to Dataset as an Input and Output of Connector.

## Mode Selection

The connector supports three modes:
- `enterprise`: Uses Fivetran log tables for ingestion (requires log tables setup in your destination)
- `standard`: Uses Fivetran REST API for ingestion (requires API credentials)
- `auto`: Automatically detects and selects the appropriate mode based on provided configuration (default)

## DataJob Mode Selection

The connector supports two modes for generating DataJobs:
- `consolidated`: Creates one DataJob per connector (default)
- `per_table`: Creates one DataJob per source-destination table pair

## Snowflake destination Configuration Guide (Enterprise Mode)
1. If your fivetran platform connector destination is snowflake, you need to provide user details and its role with correct privileges in order to fetch metadata.
2. Snowflake system admin can follow this guide to create a fivetran_datahub role, assign it the required privileges, and assign it to a user by executing the following Snowflake commands from a user with the ACCOUNTADMIN role or MANAGE GRANTS privilege.

```sql
create or replace role fivetran_datahub;

// Grant access to a warehouse to run queries to view metadata
grant operate, usage on warehouse "<your-warehouse>" to role fivetran_datahub;

// Grant access to view database and schema in which your log and metadata tables exist
grant usage on DATABASE "<fivetran-log-database>" to role fivetran_datahub;
grant usage on SCHEMA "<fivetran-log-database>"."<fivetran-log-schema>" to role fivetran_datahub;

// Grant access to execute select query on schema in which your log and metadata tables exist
grant select on all tables in SCHEMA "<fivetran-log-database>"."<fivetran-log-schema>" to role fivetran_datahub;

// Grant the fivetran_datahub to the snowflake user.
grant role fivetran_datahub to user snowflake_user;
```

## Bigquery destination Configuration Guide (Enterprise Mode)
1. If your fivetran platform connector destination is bigquery, you need to setup a ServiceAccount as per [BigQuery docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and select BigQuery Data Viewer and BigQuery Job User IAM roles. 
2. Create and Download a service account JSON keyfile and provide bigquery connection credential in bigquery destination config.

## API Configuration Guide (Standard Mode)
1. Log in to the Fivetran dashboard.
2. Navigate to the Account Settings > API Config page.
3. Create a new API key and secret.
4. Use these credentials in the `api_config` section of your recipe.

## Advanced Configurations

### Working with Platform Instances
If you've multiple instances of source/destination systems that are referred in your `fivetran` setup, you'd need to configure platform instance for these systems in `fivetran` recipe to generate correct lineage edges. Refer the document [Working with Platform Instances](https://datahubproject.io/docs/platform-instances) to understand more about this.

While configuration of platform instance for source system you need to provide connector id as key and for destination system provide destination id as key.

#### Example - Multiple Postgres Source Connectors each reading from different postgres instance
```yml
    # Map of connector source to platform instance
    sources_to_platform_instance:
      postgres_connector_id1: 
        platform: postgres  # Optional override for platform detection
        platform_instance: cloud_postgres_instance
        env: PROD
        database: postgres_db  # Database name for the source

      postgres_connector_id2:
        platform: postgres  # Optional override for platform detection
        platform_instance: local_postgres_instance
        env: DEV
        database: postgres_db  # Database name for the source
```

#### Example - Multiple Snowflake Destinations each writing to different snowflake instance
```yml
    # Map of destination to platform instance
    destination_to_platform_instance:
      snowflake_destination_id1: 
        platform: snowflake  # Optional override for platform detection
        platform_instance: prod_snowflake_instance
        env: PROD
        database: PROD_SNOWFLAKE_DB  # Database name for the destination

      snowflake_destination_id2:
        platform: snowflake  # Optional override for platform detection
        platform_instance: dev_snowflake_instance
        env: PROD
        database: DEV_SNOWFLAKE_DB  # Database name for the destination
```