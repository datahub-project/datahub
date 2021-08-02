# Kafka Connect `kafka-connect`

This plugin extracts the following:

- Kafka Connect connector as individual `DataFlowSnapshotClass` entity
- Creating individual `DataJobSnapshotClass` entity using `{connector_name}:{source_dataset}` naming
- Lineage information between source database to Kafka topic

```yml
source:
  type: "kafka-connect"
  config:
    connect_uri: "http://localhost:8083"
    cluster_name: "connect-cluster"
    connector_patterns:
      deny:
        - ^denied-connector.*
      allow:
        - ^allowed-connector.*
```

Current limitations:

- Currently works only for Debezium source connectors.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
