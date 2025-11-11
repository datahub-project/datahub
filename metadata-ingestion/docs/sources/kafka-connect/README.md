## Integration Details

This plugin extracts the following:

- Source and Sink Connectors in Kafka Connect as Data Pipelines
- For Source connectors - Data Jobs to represent lineage information between source dataset to Kafka topic per `{connector_name}:{source_dataset}` combination
- For Sink connectors - Data Jobs to represent lineage information between Kafka topic to destination dataset per `{connector_name}:{topic}` combination

### Requirements

**Java Runtime Dependency:**

This source requires Java to be installed and available on the system for transform pipeline support (RegexRouter, etc.). The Java runtime is accessed via JPype to enable Java regex pattern matching that's compatible with Kafka Connect transforms.

- **Python installations**: Install Java separately (e.g., `apt-get install openjdk-11-jre-headless` on Debian/Ubuntu)
- **Docker deployments**: Ensure your DataHub ingestion Docker image includes a Java runtime. The official DataHub images include Java by default.
- **Impact**: Without Java, transform pipeline features will be disabled and lineage accuracy may be reduced for connectors using transforms

**Note for Docker users**: If you're building custom Docker images for DataHub ingestion, ensure a Java Runtime Environment (JRE) is included in your image to support full transform pipeline functionality.

### Environment Support

DataHub's Kafka Connect source supports both **self-hosted** and **Confluent Cloud** environments with automatic detection and environment-specific topic retrieval strategies:

#### Self-hosted Kafka Connect

- **Topic Discovery**: Uses runtime `/connectors/{name}/topics` API endpoint
- **Accuracy**: Returns actual topics that connectors are currently reading from/writing to
- **Benefits**: Most accurate topic information as it reflects actual runtime state
- **Requirements**: Standard Kafka Connect REST API access

#### Confluent Cloud

- **Topic Discovery**: Uses comprehensive Kafka REST API v3 for optimal transform pipeline support with config-based fallback
- **Method**: Gets all topics from Kafka cluster via REST API, applies reverse transform pipeline for accurate mappings
- **Transform Support**: Full support for complex transform pipelines via reverse pipeline strategy using actual cluster topics
- **Fallback**: Falls back to config-based derivation if Kafka API is unavailable

**Environment Detection**: Automatically detects environment based on `connect_uri` patterns containing `confluent.cloud`.

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                                                                  | DataHub Concept                                                                           | Notes |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ----- |
| `"kafka-connect"`                                                               | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |       |
| [Connector](https://kafka.apache.org/documentation/#connect_connectorsandtasks) | [DataFlow](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow/)          |       |
| Kafka Topic                                                                     | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)            |       |

## Supported Connectors and Lineage Extraction

DataHub supports different connector types with varying levels of lineage extraction capabilities depending on the environment (self-hosted vs Confluent Cloud):

### Source Connectors

| Connector Type                                                                   | Self-hosted Support | Confluent Cloud Support | Topic Discovery Method      | Lineage Extraction             |
| -------------------------------------------------------------------------------- | ------------------- | ----------------------- | --------------------------- | ------------------------------ |
| **Platform JDBC Source**<br/>`io.confluent.connect.jdbc.JdbcSourceConnector`     | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Cloud PostgreSQL CDC**<br/>`PostgresCdcSource`                                 | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Cloud PostgreSQL CDC V2**<br/>`PostgresCdcSourceV2`                            | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Cloud MySQL Source**<br/>`MySqlSource`                                         | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Cloud MySQL CDC**<br/>`MySqlCdcSource`                                         | ✅ Full             | ✅ Full                 | Runtime API / Config-based  | Table → Topic mapping          |
| **Debezium MySQL**<br/>`io.debezium.connector.mysql.MySqlConnector`              | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium PostgreSQL**<br/>`io.debezium.connector.postgresql.PostgresConnector` | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium SQL Server**<br/>`io.debezium.connector.sqlserver.SqlServerConnector` | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium Oracle**<br/>`io.debezium.connector.oracle.OracleConnector`           | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium DB2**<br/>`io.debezium.connector.db2.Db2Connector`                    | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Database → Topic CDC mapping   |
| **Debezium MongoDB**<br/>`io.debezium.connector.mongodb.MongoDbConnector`        | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Collection → Topic CDC mapping |
| **Debezium Vitess**<br/>`io.debezium.connector.vitess.VitessConnector`           | ✅ Full             | ✅ Partial              | Runtime API / Config-based  | Table → Topic CDC mapping      |
| **MongoDB Source**<br/>`com.mongodb.kafka.connect.MongoSourceConnector`          | ✅ Full             | 🔧 Config Required      | Runtime API / Manual config | Collection → Topic mapping     |
| **Generic Connectors**                                                           | 🔧 Config Required  | 🔧 Config Required      | User-defined mapping        | Custom lineage mapping         |

### Sink Connectors

| Connector Type                                                                 | Self-hosted Support | Confluent Cloud Support | Topic Discovery Method     | Lineage Extraction        |
| ------------------------------------------------------------------------------ | ------------------- | ----------------------- | -------------------------- | ------------------------- |
| **BigQuery Sink**<br/>`com.wepay.kafka.connect.bigquery.BigQuerySinkConnector` | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **S3 Sink**<br/>`io.confluent.connect.s3.S3SinkConnector`                      | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → S3 object mapping |
| **Snowflake Sink**<br/>`com.snowflake.kafka.connector.SnowflakeSinkConnector`  | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud PostgreSQL Sink**<br/>`PostgresSink`                                   | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud MySQL Sink**<br/>`MySqlSink`                                           | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud Snowflake Sink**<br/>`SnowflakeSink`                                   | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |

**Legend:**

- ✅ **Full**: Complete lineage extraction with accurate topic discovery
- ✅ **Partial**: Lineage extraction supported but topic discovery may be limited (config-based only)
- 🔧 **Config Required**: Requires `generic_connectors` configuration for lineage mapping

### Supported Transforms

DataHub uses an **advanced transform pipeline strategy** that automatically handles complex transform chains by applying the complete pipeline to all topics and checking if results exist. This provides robust support for any combination of transforms.

#### Topic Routing Transforms

- **RegexRouter**: `org.apache.kafka.connect.transforms.RegexRouter`
- **Cloud RegexRouter**: `io.confluent.connect.cloud.transforms.TopicRegexRouter`
- **Debezium EventRouter**: `io.debezium.transforms.outbox.EventRouter` (Outbox pattern)

#### Non-Topic Routing Transforms

DataHub recognizes but passes through these transforms (they don't affect lineage):

- InsertField, ReplaceField, MaskField, ValueToKey, HoistField, ExtractField
- SetSchemaMetadata, Flatten, Cast, HeadersFrom, TimestampConverter
- Filter, InsertHeader, DropHeaders, Drop, TombstoneHandler

#### Transform Pipeline Strategy

DataHub uses an improved **reverse transform pipeline approach** that:

1. **Takes all actual topics** from the connector manifest/Kafka cluster
2. **Applies the complete transform pipeline** to each topic
3. **Checks if transformed results exist** in the actual topic list
4. **Creates lineage mappings** only for successful matches

**Benefits:**

- ✅ **Works with any transform combination** (single or chained transforms)
- ✅ **Handles complex scenarios** like EventRouter + RegexRouter chains
- ✅ **Uses actual topics as source of truth** (no prediction needed)
- ✅ **Future-proof** for new transform types
- ✅ **Works identically** for both self-hosted and Confluent Cloud environments

## Capabilities and Limitations

### Transform Pipeline Support

**✅ Fully Supported:**

- **Any combination of transforms**: RegexRouter, EventRouter, and non-routing transforms
- **Complex transform chains**: Multiple chained transforms automatically handled
- **Both environments**: Self-hosted and Confluent Cloud work identically
- **Future-proof**: New transform types automatically supported

**⚠️ Considerations:**

- For connectors not listed in the supported connector table above, use the `generic_connectors` configuration to provide explicit lineage mappings
- Some advanced connector-specific features may not be fully supported

### Environment-Specific Behavior

#### Self-hosted Kafka Connect

- **Topic Discovery**: Uses runtime `/connectors/{name}/topics` API endpoint for maximum accuracy
- **Requirements**: Standard Kafka Connect REST API access
- **Fallback**: If runtime API fails, falls back to config-based derivation

#### Confluent Cloud

- **Topic Discovery**: Uses comprehensive Kafka REST API v3 to get all topics, with automatic credential reuse
- **Transform Support**: Full support for all transform combinations via reverse pipeline strategy using actual cluster topics
- **Auto-derivation**: Automatically derives Kafka REST endpoint from connector configurations

### Configuration Control

The `use_connect_topics_api` flag controls topic retrieval behavior:

- **When `true` (default)**: Uses environment-specific topic discovery with full transform support
- **When `false`**: Disables all topic discovery for air-gapped environments or performance optimization

### Advanced Scenarios

**Complex Transform Chains:**
The new reverse transform pipeline strategy handles complex scenarios automatically:

```yaml
# Example: EventRouter + RegexRouter chain
transforms: EventRouter,RegexRouter
transforms.EventRouter.type: io.debezium.transforms.outbox.EventRouter
transforms.RegexRouter.type: org.apache.kafka.connect.transforms.RegexRouter
transforms.RegexRouter.regex: "outbox\\.event\\.(.*)"
transforms.RegexRouter.replacement: "events.$1"
```

**Fallback Options:**

- If transform pipeline cannot determine mappings, DataHub falls back to simple topic-based lineage
- For unsupported connector types or complex custom scenarios, use `generic_connectors` configuration

**Performance Optimization:**

- Set `use_connect_topics_api: false` to disable topic discovery in air-gapped environments
- Transform pipeline processing adds minimal overhead and improves lineage accuracy
