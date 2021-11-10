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

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field         | Required | Default            | Description                                             |
| ------------- | -------- | ------------------ | ------------------------------------------------------- |
| `connect_uri` |          | `"localhost:8088"` | Superset host URL.                                      |
| `username`    |          |                    | Superset username.                                      |
| `password`    |          |                    | Superset password.                                      |
| `provider`    |          | `"db"`             | Superset provider.                                      |
| `env`         |          | `"PROD"`           | Environment to use in namespace when constructing URNs. |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
