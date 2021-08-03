

# Superset

## Setup

To install this plugin, run `pip install 'acryl-datahub[superset]'`.

See documentation for superset's `/security/login` at https://superset.apache.org/docs/rest-api for more details on superset's login api.

## Capabilities

This plugin extracts the following:

- List of charts and dashboards

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: superset
  config:
    connect_uri: http://localhost:8088

    username: user
    password: pass
    provider: db | ldap

    env: "PROD" # Optional, default is "PROD"
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field         | Required | Default | Description |
| ------------- | -------- | ------- | ----------- |
| `connect_uri` |          |         |             |
| `username`    |          |         |             |
| `password`    |          |         |             |
| `provider`    |          |         |             |
| `env`         |          |         |             |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
