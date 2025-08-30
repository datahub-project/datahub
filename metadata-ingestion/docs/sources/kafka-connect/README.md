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

## Supported Connectors and Transforms

### Source Connectors

DataHub currently supports the following source connectors:

#### JDBC Source Connectors

- **Platform JDBC Source**: `io.confluent.connect.jdbc.JdbcSourceConnector`
- **Cloud PostgreSQL CDC Source**: `PostgresCdcSource`
- **Cloud PostgreSQL CDC Source V2**: `PostgresCdcSourceV2`
- **Cloud MySQL Source**: `MySqlSource`
- **Cloud MySQL CDC Source**: `MySqlCdcSource`

#### Debezium Source Connectors

- **MySQL**: `io.debezium.connector.mysql.MySqlConnector` (also supports short name `MySqlConnector`)
- **PostgreSQL**: `io.debezium.connector.postgresql.PostgresConnector`
- **SQL Server**: `io.debezium.connector.sqlserver.SqlServerConnector`
- **Oracle**: `io.debezium.connector.oracle.OracleConnector`
- **DB2**: `io.debezium.connector.db2.Db2Connector`
- **MongoDB**: `io.debezium.connector.mongodb.MongoDbConnector`
- **Vitess**: `io.debezium.connector.vitess.VitessConnector`

#### Other Source Connectors

- **MongoDB**: `com.mongodb.kafka.connect.MongoSourceConnector`
- **Generic Connectors**: Any connector with user-defined lineage mapping via `generic_connectors` config

### Sink Connectors

DataHub currently supports the following sink connectors:

- **BigQuery**: `com.wepay.kafka.connect.bigquery.BigQuerySinkConnector`
- **Amazon S3**: `io.confluent.connect.s3.S3SinkConnector`
- **Snowflake**: `com.snowflake.kafka.connector.SnowflakeSinkConnector`
- **Cloud PostgreSQL Sink**: `PostgresSink`
- **Cloud MySQL Sink**: `MySqlSink`
- **Cloud Snowflake Sink**: `SnowflakeSink`

### Supported Transforms

DataHub can handle the following Kafka Connect transforms for lineage mapping:

#### Topic Routing Transforms

- **RegexRouter**: `org.apache.kafka.connect.transforms.RegexRouter`
- **Cloud RegexRouter**: `io.confluent.connect.cloud.transforms.TopicRegexRouter`
- **Debezium EventRouter**: `io.debezium.transforms.outbox.EventRouter` (Outbox pattern) - _See limitations below_

#### Non-Topic Routing Transforms

DataHub recognizes but passes through these transforms (they don't affect lineage):

- InsertField, ReplaceField, MaskField, ValueToKey, HoistField, ExtractField
- SetSchemaMetadata, Flatten, Cast, HeadersFrom, TimestampConverter
- Filter, InsertHeader, DropHeaders, Drop, TombstoneHandler

## Current limitations

### General Limitations

- For source connectors not listed above, use the `generic_connectors` configuration to provide explicit lineage mappings
- Complex transform chains beyond the supported transforms may require manual configuration
- Some advanced connector-specific features may not be fully supported

### EventRouter Transform Limitations

The Debezium EventRouter transform support has specific limitations:

- **Outbox table detection**: DataHub looks for source tables containing "outbox" in the name. If your outbox table has a different name, lineage mapping may be incomplete
- **Simple patterns only**: DataHub can handle direct `outbox.event.*` topic patterns but struggles with complex transform chains that further modify EventRouter output
- **Complex transform chains**: When EventRouter is combined with additional transforms (like RegexRouter), DataHub may not be able to predict the final topic names accurately
- **Fallback behavior**: If EventRouter output is further transformed and DataHub cannot determine the mapping, it will use manifest topics as-is and recommend using `generic_connectors` for explicit lineage mapping

**Recommendation**: For complex EventRouter scenarios with additional transforms, configure explicit source-to-topic mappings using the `generic_connectors` configuration option.
