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

## Concept mapping

| Fivetran        | Datahub                                                                                               |
| --------------- | ----------------------------------------------------------------------------------------------------- |
| `Connector`     | [DataJob](https://docs.datahub.com/docs/generated/metamodel/entities/datajob/)                        |
| `Source`        | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)                        |
| `Destination`   | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)                        |
| `Connector Run` | [DataProcessInstance](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance) |

Source and destination are mapped to Dataset as an Input and Output of Connector.

## Current limitations

Works only for

- Snowflake destination
- Bigquery destination
- Databricks destination

## Snowflake destination Configuration Guide

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

## Bigquery destination Configuration Guide

1. If your fivetran platform connector destination is bigquery, you need to setup a ServiceAccount as per [BigQuery docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and select BigQuery Data Viewer and BigQuery Job User IAM roles.
2. Create and Download a service account JSON keyfile and provide bigquery connection credential in bigquery destination config.

## Databricks destination Configuration Guide

1. - Get your Databricks instance's [workspace url](https://docs.databricks.com/workspace/workspace-details.html#workspace-instance-names-urls-and-ids)
- Create a [Databricks Service Principal](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#what-is-a-service-principal)
  - You can skip this step and use your own account to get things running quickly,
    but we strongly recommend creating a dedicated service principal for production use.
- Generate a Databricks Personal Access token following the following guides:
  - [Service Principals](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#personal-access-tokens)
  - [Personal Access Tokens](https://docs.databricks.com/dev-tools/auth.html#databricks-personal-access-tokens)
- Provision your service account:
  - To ingest your workspace's metadata and lineage, your service principal must have all of the following:
    - One of: metastore admin role, ownership of, or `USE CATALOG` privilege on any catalogs you want to ingest
    - One of: metastore admin role, ownership of, or `USE SCHEMA` privilege on any schemas you want to ingest
    - Ownership of or `SELECT` privilege on any tables and views you want to ingest
    - [Ownership documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/ownership.html)
    - [Privileges documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
- Check the starter recipe below and replace `workspace_url` and `token` with your information from the previous steps.

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
