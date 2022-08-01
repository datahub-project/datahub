# Superset

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[superset]'`.

See documentation for superset's `/security/login` at https://superset.apache.org/docs/rest-api for more details on superset's login api.

## Capabilities

This plugin extracts the following:

- Charts, dashboards, and associated metadata

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: superset
  config:
    # Coordinates
    connect_uri: http://localhost:8088

    # Credentials
    username: user
    password: pass
    provider: ldap

sink:
  # sink configs
```

If you were using `database_alias` in one of your other ingestions to rename your databases to something else based on business needs you can rename them in superset also

```yml
source:
  type: superset
  config:
    # Coordinates
    connect_uri: http://localhost:8088

    # Credentials
    username: user
    password: pass
    provider: ldap
    database_alias:
      example_name_1: business_name_1
      example_name_2: business_name_2

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field         | Required | Default            | Description                                             |
| ------------- | -------- | ------------------ | ------------------------------------------------------- |
| `connect_uri` |          | `"localhost:8088"` | Superset host URL.                                      |
| `display_uri` |          | `(connect_uri)`    | Publicly accessible Superset URL, see note below.       |
| `username`    |          |                    | Superset username.                                      |
| `password`    |          |                    | Superset password.                                      |
| `provider`    |          | `"db"`             | Superset provider.                                      |
| `env`         |          | `"PROD"`           | Environment to use in namespace when constructing URNs. |
| `database_alias` |       |                    | Can be used to change mapping for database names in superset to what you have in datahub |

NOTE: `display_uri` can be used when you need to ingest from a private, specially configured instance, but still want dashboard, graph, etc. links to point to the publicly accessible URL.  So, for example, you could set `connect_uri: localhost:xxxx, display_uri: superset.mydomain.com`.  You may need to do this if `superset.mydomain.com` has complex authentication that is not easy to pass through this source config.

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
