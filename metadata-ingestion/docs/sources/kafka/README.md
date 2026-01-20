The Kafka connector extracts topic metadata, schemas, and data profiles from Apache Kafka clusters and Confluent Cloud. It provides comprehensive metadata discovery with advanced schema resolution and optional data profiling capabilities.

### Key Features

- **Schema Registry Integration**: Automatically extracts Avro and Protobuf schemas from Confluent Schema Registry
- **Multi-Stage Schema Resolution**: Intelligent fallback strategies for topics without registered schemas, including automatic schema inference
- **Data Profiling**: Optional profiling of message content to generate field-level statistics and sample values
- **Platform Instance Support**: Track multiple Kafka clusters separately with platform instances
- **Meta Mapping**: Automatically extract tags, terms, and ownership from Avro schema metadata

### Concept Mapping

| Kafka Concept    | DataHub Concept                                               | Notes                                  |
| ---------------- | ------------------------------------------------------------- | -------------------------------------- |
| Topic            | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `Topic`                        |
| Schema (Subject) | [Schema Metadata](../../metamodel/entities/dataset.md#schema) | Avro, Protobuf, JSON schemas           |
| Message Fields   | [Dataset Fields](../../metamodel/entities/dataset.md#fields)  | Extracted from schemas or inferred     |
| Kafka Cluster    | [Data Platform Instance](../../../platform-instances.md)      | When `platform_instance` is configured |

### Important Notes

1. **Schema Resolution vs Profiling**: These are independent features that can be enabled separately:
   - **Schema Resolution**: Discovers schemas for topics missing from the schema registry
   - **Data Profiling**: Analyzes message content for data quality statistics
2. **Performance**: The `profiling.max_workers` setting controls parallelization for both profiling AND schema resolution operations
3. **Confluent Cloud**: Requires API credentials for both the Kafka cluster and Schema Registry
4. **Stateful Ingestion**: Available only when a platform instance is assigned to this source

Read on to learn about configuration options, advanced features, and troubleshooting tips.
