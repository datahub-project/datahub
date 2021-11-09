# Redash

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[redash]'`.

## Capabilities

This plugin extracts the following:

- Redash dashboards and queries/visualization

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "redash"
  config:
    connect_uri: http://localhost:5000/
    api_key: REDASH_API_KEY

    # Optionals
    # api_page_limit: 1 #default: None, no limit on ingested dashboards and charts API pagination
    # skip_draft: true  #default: true, only ingest published dashboards and charts
    # dashboard_patterns:
    #   deny:
    #     - ^denied dashboard.*
    #   allow:
    #     - .*allowed dashboard.*
    # chart_patterns:
    #   deny:
    #     - ^denied chart.*
    #   allow:
    #     - .*allowed chart.*
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                      | Required | Default                | Description                                                      |
| -------------------------- | -------- | ---------------------- | ---------------------------------------------------------------- |
| `connect_uri`              | ✅       | http://localhost:5000/ | Redash base URL.                                                 |
| `api_key`                  | ✅       |                        | Redash user API key.                                             |
| `api_page_limit`           |          | `None`                 | Limit on ingested dashboards and charts API pagination.          |
| `skip_draft`               |          | `true`                 | Only ingest published dashboards and charts.                     |
| `dashboard_patterns.allow` |          |                        | List of regex patterns for dashboards to include in ingestion.   |
| `dashboard_patterns.deny`  |          |                        | List of regex patterns for dashboards to exclude from ingestion. |
| `chart_patterns.allow`     |          |                        | List of regex patterns for charts to include in ingestion.       |
| `chart_patterns.deny`      |          |                        | List of regex patterns for charts to exclude from ingestion.     |
| `env`                      |          | `"PROD"`               | Environment to use in namespace when constructing URNs.          |


## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
