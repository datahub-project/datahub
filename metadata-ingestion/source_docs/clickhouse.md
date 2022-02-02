# Clickhouse

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[clickhouse]'`.

## Capabilities

This plugin extracts the following:

- Metadata for tables, views and materialized views
- Column types associated with each table (excluding nested types)

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: clickhouse
  config:
    host: localhost
    username: default
    password: ''
    schema_pattern:
      allow:
        - "schema_example"
    table_pattern:
      deny:
        - ".inner_id."

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                       | Required | Default       | Description                                                   |
|-----------------------------| -------- |---------------|---------------------------------------------------------------|
| `username`                  |          |               | Database username.                                            |
| `password`                  |          |               | Database password.                                            |
| `host`                      |          | `"localhost"` | List of IP addresses for nodes in cluster.                    |
| `env`                       |          | `"PROD"`      | Environment to use in namespace when constructing URNs.       |
| `client_kwargs`             |          | `{}`          | Kwargs passed to Client class.                                |
| `table_pattern.allow`       |          |               | List of regex patterns for tables to include in ingestion.    |
| `table_pattern.deny`        |          |               | List of regex patterns for tables to exclude from ingestion.  |
| `table_pattern.ignoreCase`  |          | `True`        | Whether to ignore case sensitivity during pattern matching.   |
| `schema_pattern.allow`      |          |               | List of regex patterns for schemas to include in ingestion.   |
| `schema_pattern.deny`       |          |               | List of regex patterns for schemas to exclude from ingestion. |
| `schema_pattern.ignoreCase` |          | `True`        | Whether to ignore case sensitivity during pattern matching.   |

## Compatibility

Compatible with CLickhouse 21.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
