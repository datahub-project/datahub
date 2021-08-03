# Looker dashboards

## Setup

To install this plugin, run `pip install 'acryl-datahub[looker]'`.

## Capabilities

This plugin extracts the following:

- Looker dashboards and dashboard elements (charts)
- Names, descriptions, URLs, chart types, input view for the charts

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: "looker"
  config:
    client_id: # Your Looker API3 client ID
    client_secret: # Your Looker API3 client secret
    base_url: # The url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar.

    platform_name: "looker" # Optional, default is "looker"
    actor: urn:li:corpuser:etl # Optional, defaults to urn:li:corpuser:etl

    # Regex pattern to allow/deny dashboards. If left blank, will ingest all.
    dashboard_pattern:
      deny:
        # ...
      allow:
        # ...

    # Regex pattern to allow/deny charts. If left blank, will ingest all.
    chart_pattern:
      deny:
        # ...
      allow:
        # ...

    env: "PROD" # Optional, default is "PROD"
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                     | Required | Default | Description |
| ------------------------- | -------- | ------- | ----------- |
| `client_id`               |          |         |             |
| `client_secret`           |          |         |             |
| `base_url`                |          |         |             |
| `platform_name`           |          |         |             |
| `actor`                   |          |         |             |
| `dashboard_pattern.allow` |          |         |             |
| `dashboard_pattern.deny`  |          |         |             |
| `chart_pattern.allow`     |          |         |             |
| `chart_pattern.deny`      |          |         |             |
| `env`                     |          |         |             |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
