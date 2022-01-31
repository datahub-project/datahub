# Mode

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[mode]'`.

See documentation for Mode's API at https://mode.com/developer/api-reference/introduction/


## Capabilities

This plugin extracts Charts, Reports, and associated metadata from a given Mode workspace. This plugin is in beta and has only been tested
on PostgreSQL database.

| Capability | Status | Details | 
| -----------| ------ | ---- |
| Platform Instance | ✔️ | [link](../../docs/platform-instances.md) |

### Report

[/api/{account}/reports/{report}](https://mode.com/developer/api-reference/analytics/reports/) endpoint is used to
retrieve the following report information.

- Title and description
- Last edited by
- Owner
- Link to the Report in Mode for exploration
- Associated charts within the report

### Chart

[/api/{workspace}/reports/{report}/queries/{query}/charts'](https://mode.com/developer/api-reference/analytics/charts/#getChart) endpoint is used to
retrieve the following information.

- Title and description
- Last edited by
- Owner
- Link to the chart in Metabase
- Datasource and lineage information from Report queries.

The following properties for a chart are ingested in DataHub.

#### Chart Information
| Name      | Description                            |
|-----------|----------------------------------------|
| `Filters` | Filters applied to the chart           |
| `Metrics` | Fields or columns used for aggregation |
| `X`       | Fields used in X-axis                  |
| `X2`      | Fields used in second X-axis           |
| `Y`       | Fields used in Y-axis                  |
| `Y2`      | Fields used in second Y-axis           |


#### Table Information
| Name      | Description                  |
|-----------|------------------------------|
| `Columns` | Column names in a table      |
| `Filters` | Filters applied to the table |



#### Pivot Table Information
| Name      | Description                            |
|-----------|----------------------------------------|
| `Columns` | Column names in a table                |
| `Filters` | Filters applied to the table           |
| `Metrics` | Fields or columns used for aggregation |
| `Rows`    | Row names in a table                   |

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: mode
  config:
    # Coordinates
    connect_uri: http://app.mode.com

    # Credentials
    token: token
    password: pass
    
    # Options
    workspace: "datahub"
    default_schema: "public"
    owner_username_instead_of_email: False
    api_options:
      retry_backoff_multiplier: 2
      max_retry_interval: 10
      max_attempts: 5

sink:
  # sink configs
```

## Config details

| Field                             | Required | Default                  | Description                                                                                       |
|-----------------------------------| -------- |--------------------------|---------------------------------------------------------------------------------------------------|
| `connect_uri`                     |    ✅    | `"https://app.mode.com"` | Mode host URL.                                                                                    |
| `token`                           |    ✅    |                          | Mode user token.                                                                                  |
| `password`                        |    ✅    |                          | Mode password for authentication.                                                                 |
| `default_schema`                  |          | `public`                 | Default schema to use when schema is not provided in an SQL query                                 |
| `env`                             |          | `"PROD"`                 | Environment to use in namespace when constructing URNs.                                           |
| `platform_instance_map` |     |     | Platform instance mapping to use when constructing URNs to data sources through lineage. e.g.`platform_instance_map: { "hive": "warehouse" }` |
| `owner_username_instead_of_email` |          | `True`                   | Use username for owner URN instead of Email                                                       |
| `api_options`                     |          |                          | Retry/Wait settings for Mode API to avoid "Too many Requests" error. See Mode API Options below   |

See Mode's [Authentication documentation](https://mode.com/developer/api-reference/authentication/) on how to generate `token` and `password`.

<br/>

#### Mode API Options
| Field                      | Required | Default | Description                                              |
|----------------------------|----------|---------|----------------------------------------------------------|
| `retry_backoff_multiplier` |          | `1`     | Multiplier for exponential backoff when waiting to retry |
| `max_retry_interval`       |          | `10`    | Maximum interval to wait when retrying                   |
| `max_attempts`             |          | `5`     | Maximum number of attempts to retry before failing       |


## Compatibility

N/A


## Questions

If you've got any questions on configuring this source, feel free to ping us on
[our Slack](https://slack.datahubproject.io/)!
