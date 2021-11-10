# Grafana

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[grafana]'`.

See documentation for more details on authentication API at https://grafana.com/docs/grafana/latest/http_api/auth/

## Capabilities

This plugin extracts the following:

- Extract Dashboard , Chart and Input Dataset from Grafana API
- Extract all Dashboard
- Extract only Chart contain chart input
(condition : Chart using "raw sql" with sql statement )
- Extract "Chart Input Dataset" from sql statement using sql parser
- Chart type mapping

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: grafana
  config:
    connect_uri: "http://localhost:3000/"
    api_key: GRAFANA_API_KEY

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field         | Required | Default            | Description                                             |
| ------------- | -------- | ------------------ | ------------------------------------------------------- |
| `connect_uri` | ✅        | `"localhost:3000"` | Grafana host URL.                                      |
| `api_key`    | ✅        |                    | Grafana user API key.                                      |
| `env`         |          | `"PROD"`           | Environment to use in namespace when constructing URNs. |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!

