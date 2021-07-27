# Looker dashboards

To install this plugin, run `pip install 'acryl-datahub[looker]'`.

Extracts:

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
    dashboard_pattern: # supports allow/deny regexes
    chart_pattern: # supports allow/deny regexes
    actor: urn:li:corpuser:etl # Optional, defaults to urn:li:corpuser:etl
    env: "PROD" # Optional, default is "PROD"
    platform_name: "looker" # Optional, default is "looker"
```
