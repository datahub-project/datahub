# Kafka Connect

## Setup

To install this plugin, run `pip install 'acryl-datahub[kafka-connect]'`.

## Capabilities

This plugin extracts the following:

- Kafka Connect connector as individual `DataFlowSnapshotClass` entity
- Creating individual `DataJobSnapshotClass` entity using `{connector_name}:{source_dataset}` naming
- Lineage information between source database to Kafka topic

Current limitations:

- Currently works only for Debezium source connectors.

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: "kafka-connect"
  config:
    connect_uri: "http://localhost:8083"
    cluster_name: "connect-cluster"
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                      | Required | Default | Description |
| -------------------------- | -------- | ------- | ----------- |
| `connect_uri`              |          |         |             |
| `cluster_name`             |          |         |             |
| `connector_patterns.deny`  |          |         |             |
| `connector_patterns.allow` |          |         |             |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
