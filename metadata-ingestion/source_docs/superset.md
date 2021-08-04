# Superset

## Setup

To install this plugin, run `pip install 'acryl-datahub[superset]'`.

See documentation for superset's `/security/login` at https://superset.apache.org/docs/rest-api for more details on superset's login api.

## Capabilities

This plugin extracts the following:

- List of charts and dashboards

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

```yml
source:
  type: superset
  config:
    connect_uri: http://localhost:8088

    username: user
    password: pass
    provider: ldap
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

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
