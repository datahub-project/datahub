# Kafka Connect

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[kafka-connect]'`.

## Capabilities

This plugin extracts the following:

- Kafka Connect connector as individual `DataFlowSnapshotClass` entity
- Creating individual `DataJobSnapshotClass` entity using `{connector_name}:{source_dataset}` naming
- Lineage information between source database to Kafka topic

Current limitations:

- works only for 
  - JDBC and Debezium source connectors
  - BigQuery sink connector

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "kafka-connect"
  config:
    # Coordinates
    connect_uri: "http://localhost:8083"
    cluster_name: "connect-cluster"
    provided_configs:     
      - provider: env
        path_key: MYSQL_CONNECTION_URL
        value: jdbc:mysql://test_mysql:3306/librarydb

    # Credentials
    username: admin
    password: password

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                      | Required | Default                    | Description                                             |
| -------------------------- | -------- | -------------------------- | ------------------------------------------------------- |
| `connect_uri`              |    ✅    | `"http://localhost:8083/"` | URI to connect to.                                      |
| `username`                 |          |                            | Kafka Connect username.                                 |
| `password`                 |          |                            | Kafka Connect password.                                 |
| `cluster_name`             |          | `"connect-cluster"`        | Cluster to ingest from.                                 |
| `provided_configs`         |          |                            | Provided Configurations                                 |
| `construct_lineage_workunits`    |    | `True`                     | Whether to create the input and output Dataset entities |
| `connector_patterns.deny`  |          |                            | List of regex patterns for connectors to include in ingestion.   |
| `connector_patterns.allow` |          |                            | List of regex patterns for connectors to exclude from ingestion. |
| `connector_pattern.ignoreCase`  |     | `True`      | Whether to ignore case sensitivity during pattern matching.            |
| `env`                      |          | `"PROD"`                   | Environment to use in namespace when constructing URNs. |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
