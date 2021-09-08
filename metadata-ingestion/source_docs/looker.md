# Looker dashboards

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[looker]'`.

## Capabilities

This plugin extracts the following:

- Looker dashboards and dashboard elements (charts)
- Names, descriptions, URLs, chart types, input view for the charts

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "looker"
  config:
    # Coordinates
    base_url: https://company.looker.com:19999

    # Credentials
    client_id: admin
    client_secret: password

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                     | Required | Default                 | Description                                                                                                  |
| ------------------------- | -------- | ----------------------- | ------------------------------------------------------------------------------------------------------------ |
| `client_id`               | ✅       |                         | Looker API3 client ID.                                                                                       |
| `client_secret`           | ✅       |                         | Looker API3 client secret.                                                                                   |
| `base_url`                | ✅       |                         | Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. |
| `platform_name`           |          | `"looker"`              | Platform to use in namespace when constructing URNs.                                                         |
| `actor`                   |          | `"urn:li:corpuser:etl"` | Actor to use in ownership properties of ingested metadata.                                                   |
| `dashboard_pattern.allow` |          |                         | List of regex patterns for dashboards to include in ingestion.                                                        |
| `dashboard_pattern.deny`  |          |                         | List of regex patterns for dashboards to exclude from ingestion.                                                      |
| `dashboard_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `chart_pattern.allow`     |          |                         | List of regex patterns for charts to include in ingestion.                                                            |
| `chart_pattern.deny`      |          |                         | List of regex patterns for charts to exclude from ingestion.                                                          |
| `chart_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `include_deleted`         |          | `False`                 | Whether to include deleted dashboards.                                                                       |
| `env`                     |          | `"PROD"`                | Environment to use in namespace when constructing URNs.                                                      |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
