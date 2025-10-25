## Integration Details

This plugin extracts the following:

- Source and Sink Connectors in Kafka Connect as Data Pipelines
- For Source connectors - Data Jobs to represent lineage information between source dataset to Kafka topic per `{connector_name}:{source_dataset}` combination
- For Sink connectors - Data Jobs to represent lineage information between Kafka topic to destination dataset per `{connector_name}:{topic}` combination

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                                                                  | DataHub Concept                                                                           | Notes |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ----- |
| `"kafka-connect"`                                                               | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |       |
| [Connector](https://kafka.apache.org/documentation/#connect_connectorsandtasks) | [DataFlow](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow/)          |       |
| Kafka Topic                                                                     | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)            |       |

## Supported Connectors

### Platform Support

- **OSS Kafka Connect Platform**: Full support for self-hosted Kafka Connect clusters
- **Confluent Cloud**: Native support for Confluent Cloud managed connectors with automatic platform detection

### Source Connectors

- **JDBC Source**: OSS Platform (`io.confluent.connect.jdbc.JdbcSourceConnector`)
- **Debezium CDC Sources**: All Debezium connectors (MySQL, PostgreSQL, SQL Server, Oracle, MongoDB, etc.)
- **MongoDB Source**: OSS Platform (`com.mongodb.kafka.connect.MongoSourceConnector`)
- **Confluent Cloud CDC Sources**:
  - PostgreSQL CDC (`PostgresCdcSource`, `PostgresCdcSourceV2`)
  - MySQL CDC (`MySqlCdcSource`)
- **Confluent Cloud Other Sources**:
  - Datagen (`DatagenSource`)
- **Generic Connectors**: User-defined lineage via configuration

### Sink Connectors

- **Database Sinks**:
  - PostgreSQL: Platform (`JdbcSinkConnector`) + Cloud (`PostgresSink`)
  - MySQL: Platform (`JdbcSinkConnector`) + Cloud (`MySqlSink`)
- **Cloud Storage Sinks**:
  - Amazon S3: Platform (`S3SinkConnector`) + Cloud (`S3Sink`)
  - Google BigQuery: Platform (`BigQuerySinkConnector`) + Cloud (`BigQuerySink`)
  - Snowflake: Platform (`SnowflakeSinkConnector`) + Cloud (`SnowflakeSink`)

### Key Features

- **Dual Platform Support**: Seamlessly works with both OSS Platform and Confluent Cloud
- **Automatic Detection**: Automatically detects connector platform type (Platform vs Cloud)
- **Configuration Compatibility**: Handles different configuration formats between platforms
- **API Fallback**: Gracefully handles API differences (e.g., missing `/topics` endpoint in Cloud)
- **Transform Support**: Full support for Kafka Connect transforms including RegexRouter
