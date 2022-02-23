# Power BI dashboards

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[powerbi]'`.

## Capabilities

This plugin extracts the following:

- Power BI dashboards, tiles, datasets 
- Names, descriptions and URLs of dashboard and tile
- Owners of dashboards

## Configuration Notes

See the 
1.  [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) for the steps to create a app client ID and secret. 
2.  Login to Power BI as Admin and from `Tenant settings` allow below permissions.
    - Allow service principles to use Power BI APIs 
    - Allow service principals to use read-only Power BI admin APIs
    - Enhance admin APIs responses with detailed metadata


## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "powerbi"
  config:
    # Your Power BI tenant identifier
    tenant_id: a949d688-67c0-4bf1-a344-e939411c6c0a
    # Ingest elements of below PowerBi Workspace into Datahub
    workspace_id: 4bd10256-e999-45dd-8e56-571c77153a5f
    # Workspace's dataset environments (PROD, DEV, QA, STAGE)
    env: DEV  
    # Azure AD App client identifier 
    client_id: foo
    # Azure AD App client secret 
    client_secret: bar 
    # dataset_type_mapping is fixed mapping of Power BI datasources type to equivalent Datahub "data platform" dataset
    dataset_type_mapping:
        PostgreSql: postgres
        Oracle: oracle


sink:
  # sink configs
```

## Config details

| Field                     | Required | Default                 | Description                                                                                                  |
| ------------------------- | -------- | ----------------------- | ------------------------------------------------------------------------------------------------------------ |
| `tenant_id`               | ✅       |                         | Power BI tenant identifier.                                                                                   |
| `workspace_id`            | ✅       |                         | Power BI workspace identifier.                                                                                |
| `env`                     | ✅       |                         | Environment to use in namespace when constructing URNs.                                                       |
| `client_id`               | ✅       |                         | Azure AD App client identifier.                                                                               |
| `client_secret`           | ✅       |                         | Azure AD App client secret.
| `dataset_type_mapping`    | ✅       |                         | Mapping of Power BI datasource type to Datahub dataset. 
| `scan_timeout`            | ✅       | 60                      | time in seconds to wait for Power BI metadata scan result.

## Concept mapping 

| Power BI                  | Datahub             |                                                                                               
| ------------------------- | ------------------- |
| `Dashboard`               | `Dashboard`         |
| `Dataset, Datasource`     | `Dataset`           |
| `Tile`                    | `Chart`             |
| `Report.webUrl`           | `Chart.externalUrl` |
| `Workspace`               | `N/A`               |
| `Report`                  | `N/A`               |

If Tile is created from report then Chart.externalUrl is set to Report.webUrl.

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
