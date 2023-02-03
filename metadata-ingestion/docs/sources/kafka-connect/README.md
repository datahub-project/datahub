## Integration Details

This plugin extracts the following:

- Source and Sink Connectors in Kafka Connect as Data Pipelines
- For Source connectors - Data Jobs to represent lineage information between source dataset to Kafka topic per `{connector_name}:{source_dataset}` combination
- For Sink connectors - Data Jobs to represent lineage information between Kafka topic to destination dataset per `{connector_name}:{topic}` combination

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept              | DataHub Concept                                               | Notes                                                                       |
| --------------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `"kafka-connect"`                 | [Data Platform](../../metamodel/entities/dataPlatform.md)     |                                                                             |
| [Connector](https://kafka.apache.org/documentation/#connect_connectorsandtasks)         | [DataFlow](../../metamodel/entities/dataflow.md)                | |
| Kafka Topic         | [Dataset](../../metamodel/entities/dataset.md)                | |

## Current limitations

Works only for

- Source connectors: JDBC, Debezium, Mongo and Generic connectors with user-defined lineage graph
- Sink connectors: BigQuery
