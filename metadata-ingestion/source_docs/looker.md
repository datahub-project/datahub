# Looker dashboards

To install this plugin, run `pip install 'acryl-datahub[looker]'`.

This plugin extracts the following:

- Looker dashboards and dashboard elements (charts)
- Names, descriptions, URLs, chart types, input view for the charts

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.

```yml
source:
  type: "looker"
  config:
    client_id: # Your Looker API3 client ID
    client_secret: # Your Looker API3 client secret
    base_url: # The url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar.

    platform_name: "looker" # Optional, default is "looker"
    actor: urn:li:corpuser:etl # Optional, defaults to urn:li:corpuser:etl

    # regex pattern to allow/deny dashboards
    dashboard_pattern:
      deny:
        # ...
      allow:
        # ...

    # regex pattern to allow/deny charts
    chart_pattern:
      deny:
        # ...
      allow:
        # ...

    env: "PROD" # Optional, default is "PROD"
```
