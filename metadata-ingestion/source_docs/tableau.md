# Tableau

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[tableau]'`.

See documentation for Tableau's metadata API at https://help.tableau.com/current/api/metadata_api/en-us/index.html

## Capabilities

This plugin extracts Charts, Reports, and associated metadata from a given Mode workspace. This plugin is in beta and has only been tested
on PostgreSQL database.



## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: tableau
  config:
    # Coordinates
    connect_uri: https://prod-ca-a.online.tableau.com/

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
