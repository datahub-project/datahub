# Metabase

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[metabase]'`.

See documentation for Metabase's API at https://www.metabase.com/learn/administration/metabase-api.html 
for more details on Metabase's login api.


## Capabilities

This plugin extracts Charts, dashboards, and associated metadata. This plugin is in beta and has only been tested
on PostgreSQL and H2 database.

| Capability | Status | Details | 
| -----------| ------ | ---- |
| Platform Instance | ✔️ | [link](../../docs/platform-instances.md) |

### Dashboard

[/api/dashboard](https://www.metabase.com/docs/latest/api-documentation.html#dashboard) endpoint is used to
retrieve the following dashboard information.

- Title and description
- Last edited by
- Owner
- Link to the dashboard in Metabase
- Associated charts

### Chart

[/api/card](https://www.metabase.com/docs/latest/api-documentation.html#card) endpoint is used to
retrieve the following information.

- Title and description
- Last edited by
- Owner
- Link to the chart in Metabase
- Datasource and lineage

The following properties for a chart are ingested in DataHub.

| Name          | Description                                     |
| ------------- | ----------------------------------------------- |
| `Dimensions`  | Column names                                    |
| `Filters`     | Any filters applied to the chart                |
| `Metrics`     | All columns that are being used for aggregation |


## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: metabase
  config:
    # Coordinates
    connect_uri: http://localhost:3000

    # Credentials
    username: user
    password: pass
    
    # Options
    default_schema: public
    database_alias_map:
      h2: sample-dataset.db
    # Optional mapping of platform types to instance ids
    platform_instance_map: # optional
      postgres: test_postgres    # optional

sink:
  # sink configs
```

## Config details


| Field                | Required | Default            | Description                                                            |
| -------------------- | -------- | ------------------ |------------------------------------------------------------------------|
| `connect_uri`        |    ✅     | `"localhost:8088"` | Metabase host URL.                                                     |
| `username`           |    ✅     |                    | Metabase username.                                                     |
| `password`           |    ✅     |                    | Metabase password.                                                     |
| `database_alias_map` |          |                    | Database name map to use when constructing dataset URN.                |
| `engine_platform_map`|          |                    | Custom mappings between metabase database engines and DataHub platforms |
| `default_schema`     |          | `public`           | Default schema name to use when schema is not provided in an SQL query |
| `env`                |          | `"PROD"`           | Environment to use in namespace when constructing URNs.                |

Metabase databases will be mapped to a DataHub platform based on the engine listed in the
[api/database](https://www.metabase.com/docs/latest/api-documentation.html#database) response. This mapping can be
customized by using the `engine_platform_map` config option. For example, to map databases using the `athena` engine to
the underlying datasets in the `glue` platform, the following snippet can be used:
```yml
  engine_platform_map:
    athena: glue
```
DataHub will try to determine database name from Metabase [api/database](https://www.metabase.com/docs/latest/api-documentation.html#database)
payload. However, the name can be overridden from `database_alias_map` for a given database connected to Metabase.

## Compatibility

Metabase version [v0.41.2](https://www.metabase.com/start/oss/)


## Questions

If you've got any questions on configuring this source, feel free to ping us on 
[our Slack](https://slack.datahubproject.io/)!
