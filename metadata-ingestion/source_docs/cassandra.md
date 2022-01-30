# Cassandra

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[cassandra]'`.

## Capabilities

This plugin extracts the following:

- Metadata for tables and views
- Column types associated with each table

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: cassandra
  config:
    ips:
      - ip1
      - ip2
    username: user
    password: pass
    keyspace: keyspace_test
    schema_pattern:
      deny:
        - "system"

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                       | Required | Default       | Description                                                                                                                                                                             |
|-----------------------------| -------- |---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username`                  |          |               | Database username.                                                                                                                                                                      |
| `password`                  |          |               | Database password.                                                                                                                                                                      |
| `keyspace`                  | ✅       |               | Host URL and port to connect to.                                                                                                                                                        |
| `ips`                       |          |`["localhost"]`| List of IP addresses for nodes in cluster.                                                                                                                                              |
| `env`                       |          | `"PROD"`      | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `cluster_kwargs`            |          | `{}`          | Kwargs passed to Cluster class.                                                                                                                                                         |
| `table_pattern.allow`       |          |               | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`        |          |               | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`  |          | `True`        | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`      |          |               | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`       |          |               | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase` |          | `True`        | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`        |          |               | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`         |          |               | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`   |          | `True`        | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |

## Compatibility

Compatible with Cassandra 3 and 4.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
