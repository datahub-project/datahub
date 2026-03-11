### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Transform Pipeline Support

**✅ Fully Supported:**

- **Any combination of transforms**: RegexRouter, EventRouter, and non-routing transforms
- **Complex transform chains**: Multiple chained transforms automatically handled
- **Both environments**: Self-hosted and Confluent Cloud work identically
- **Future-proof**: New transform types automatically supported

:::warning Considerations

For connectors not listed in the supported connector table above, use the `generic_connectors` configuration to provide explicit lineage mappings.
Some advanced connector-specific features may not be fully supported
:::

#### Environment-Specific Behavior

**Environment Detection**: Automatically detects environment based on `connect_uri` patterns containing `confluent.cloud`.

##### Self-hosted Kafka Connect

- **Topic Discovery**: Uses runtime `/connectors/{name}/topics` API endpoint for maximum accuracy
- **Requirements**: Standard Kafka Connect REST API access
- **Fallback**: If runtime API fails, falls back to config-based derivation

```yml
source:
  type: kafka-connect
  config:
    # Self-hosted Kafka Connect cluster
    connect_uri: "http://localhost:8083"
    # use_connect_topics_api: true  # Default - enables runtime topic discovery
```

##### Confluent Cloud

- **Topic Discovery**: Uses comprehensive Kafka REST API v3 to get all topics, with automatic credential reuse
- **Transform Support**: Full support for all transform combinations via reverse pipeline strategy using actual cluster topics
- **Auto-derivation**: Automatically derives Kafka REST endpoint from connector configurations

**Recommended approach using environment and cluster IDs:**

```yml
source:
  type: kafka-connect
  config:
    # Auto-construct URI from environment and cluster IDs (recommended)
    confluent_cloud_environment_id: "env-xyz123" # Your Confluent Cloud environment ID
    confluent_cloud_cluster_id: "lkc-abc456" # Your Kafka Connect cluster ID

    # Standard credentials for Kafka Connect API
    username: "your-connect-api-key" # API key for Kafka Connect access
    password: "your-connect-api-secret" # API secret for Kafka Connect access

    # Optional: Separate credentials for Kafka REST API (if different from Connect API)
    kafka_api_key: "your-kafka-api-key" # API key for Kafka REST API access
    kafka_api_secret: "your-kafka-api-secret" # API secret for Kafka REST API access

    # Optional: Dedicated Kafka REST endpoint for comprehensive topic retrieval
    kafka_rest_endpoint: "https://pkc-xxxxx.region.provider.confluent.cloud"

    # use_connect_topics_api: true  # Default - enables comprehensive topic retrieval
```

**Alternative approach using full URI (legacy):**

```yml
source:
  type: kafka-connect
  config:
    # Confluent Cloud Connect URI - automatically detected
    connect_uri: "https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-abc456"
    username: "your-connect-api-key" # API key for Kafka Connect
    password: "your-connect-api-secret" # API secret for Kafka Connect
    kafka_api_key: "your-kafka-api-key" # API key for Kafka REST API (if different)
    kafka_api_secret: "your-kafka-api-secret" # API secret for Kafka REST API (if different)

    # Optional: Dedicated Kafka REST endpoint for comprehensive topic retrieval
    kafka_rest_endpoint: "https://pkc-xxxxx.region.provider.confluent.cloud"
```

##### Configuration Control

The `use_connect_topics_api` flag controls topic retrieval behavior:

- **When `true` (default)**: Uses environment-specific topic discovery with full transform support
- **When `false`**: Disables all topic discovery for air-gapped environments or performance optimization

#### Advanced Scenarios: Complex Transform Chains

The new reverse transform pipeline strategy handles complex scenarios automatically:

```yaml
### Example: EventRouter + RegexRouter chain
transforms: EventRouter,RegexRouter
transforms.EventRouter.type: io.debezium.transforms.outbox.EventRouter
transforms.RegexRouter.type: org.apache.kafka.connect.transforms.RegexRouter
transforms.RegexRouter.regex: "outbox\\.event\\.(.*)"
transforms.RegexRouter.replacement: "events.$1"
```

#### Advanced Scenarios: Fallback Options

- If transform pipeline cannot determine mappings, DataHub falls back to simple topic-based lineage
- For unsupported connector types or complex custom scenarios, use `generic_connectors` configuration

#### Advanced Scenarios: Performance Optimization

- Set `use_connect_topics_api: false` to disable topic discovery in air-gapped environments
- Transform pipeline processing adds minimal overhead and improves lineage accuracy

#### Supported Source Connectors

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

#### Supported Sink Connectors

| Connector Type                                                                 | Self-hosted Support | Confluent Cloud Support | Topic Discovery Method     | Lineage Extraction        |
| ------------------------------------------------------------------------------ | ------------------- | ----------------------- | -------------------------- | ------------------------- |
| **BigQuery Sink**<br/>`com.wepay.kafka.connect.bigquery.BigQuerySinkConnector` | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **S3 Sink**<br/>`io.confluent.connect.s3.S3SinkConnector`                      | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → S3 object mapping |
| **Snowflake Sink**<br/>`com.snowflake.kafka.connector.SnowflakeSinkConnector`  | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud PostgreSQL Sink**<br/>`PostgresSink`                                   | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud MySQL Sink**<br/>`MySqlSink`                                           | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Cloud Snowflake Sink**<br/>`SnowflakeSink`                                   | ✅ Full             | ✅ Full                 | Runtime API / Config-based | Topic → Table mapping     |
| **Debezium JDBC Sink**<br/>`io.debezium.connector.jdbc.JdbcSinkConnector`      | ✅ Full             | ✅ Partial              | Runtime API / Config-based | Topic → Table mapping     |
| **Confluent JDBC Sink**<br/>`io.confluent.connect.jdbc.JdbcSinkConnector`      | ✅ Full             | ✅ Partial              | Runtime API / Config-based | Topic → Table mapping     |

**Legend:**

- ✅ **Full**: Complete lineage extraction with accurate topic discovery
- ✅ **Partial**: Lineage extraction supported but topic discovery may be limited (config-based only)
- 🔧 **Config Required**: Requires `generic_connectors` configuration for lineage mapping
- ❌ **Not supported**: Connector class is not used in this environment

:::info
On JDBC Sink connectors in Confluent Cloud:\*\* `io.debezium.connector.jdbc.JdbcSinkConnector` and `io.confluent.connect.jdbc.JdbcSinkConnector` are not Confluent Cloud managed connectors — they can only appear as custom (self-managed) connectors deployed against a Confluent Cloud Kafka cluster. When present, DataHub supports lineage extraction for them, but with one limitation: the target platform (e.g. `postgres`, `mysql`, `oracle`, `mssql`) must be auto-detected from the `connection.url` field in the connector configuration. If `connection.url` is absent or uses an unrecognised JDBC scheme, platform detection will fail and a warning will be emitted. For Confluent Cloud managed JDBC sink connectors, use the dedicated `PostgresSink` or `MySqlSink` connector classes instead, which have explicit platform support.
:::

#### Supported Transforms

DataHub uses an **advanced transform pipeline strategy** that automatically handles complex transform chains by applying the complete pipeline to all topics and checking if results exist. This provides robust support for any combination of transforms.

##### Topic Routing Transforms

- **RegexRouter**: `org.apache.kafka.connect.transforms.RegexRouter`
- **Cloud RegexRouter**: `io.confluent.connect.cloud.transforms.TopicRegexRouter`
- **Debezium EventRouter**: `io.debezium.transforms.outbox.EventRouter` (Outbox pattern)

##### Non-Topic Routing Transforms

DataHub recognizes but passes through these transforms (they don't affect lineage):

- InsertField, ReplaceField, MaskField, ValueToKey, HoistField, ExtractField
- SetSchemaMetadata, Flatten, Cast, HeadersFrom, TimestampConverter
- Filter, InsertHeader, DropHeaders, Drop, TombstoneHandler

##### Transform Pipeline Strategy

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

#### How Lineage Inference Works with Transform Pipelines

Kafka Connect connectors can apply transforms (like RegexRouter) that modify topic names before data reaches Kafka. DataHub's lineage inference analyzes these transform configurations to determine how topics are produced:

1. **Configuration Analysis** - Extracts source tables from connector configuration (`table.include.list`, `database.include.list`)
2. **Transform Application** - Applies configured transforms (RegexRouter, EventRouter, etc.) to predict final topic names
3. **Topic Validation** - Validates predicted topics against actual cluster topics using Kafka REST API v3
4. **Lineage Construction** - Maps source tables to validated topics, preserving schema information

This approach works for both self-hosted and Confluent Cloud environments:

- **Self-hosted**: Uses runtime `/connectors/{name}/topics` API for actual topics produced by each connector
- **Confluent Cloud**: Uses Kafka REST API v3 to get all cluster topics, then applies transform pipeline to match with connector config

**Key Benefits**:

- **90-95% accuracy** for Cloud connectors with transforms (significant improvement over previous config-only approach)
- **Full RegexRouter support** with Java regex compatibility
- **Complex transform chains** handled correctly
- **Schema preservation** maintains full table names with schema information

**Configuration Options:**

- **Environment/Cluster IDs (recommended)**: Use `confluent_cloud_environment_id` and `confluent_cloud_cluster_id` for automatic URI construction
- **Auto-derivation**: DataHub finds Kafka REST endpoint automatically from connector configs
- **Manual endpoint**: Specify `kafka_rest_endpoint` if auto-derivation doesn't work
- **Separate credentials (typical)**: Use `connect_api_key`/`connect_api_secret` for Connect API and `kafka_api_key`/`kafka_api_secret` for Kafka REST API
- **Legacy credentials**: Use `username`/`password` for Connect API (falls back for Kafka API if separate credentials not provided)

#### Enhanced Topic Resolution for Source and Sink Connectors

DataHub now provides intelligent topic resolution that works reliably across all environments, including Confluent Cloud where the Kafka Connect topics API is unavailable.

##### How It Works

**Source Connectors** (Debezium, Snowflake CDC, JDBC):

- Always derive expected topics from connector configuration (`table.include.list`, `database.include.list`)
- Apply configured transforms (RegexRouter, EventRouter, etc.) to predict final topic names
- When Kafka API is available: Filter to only topics that exist in Kafka
- When Kafka API is unavailable (Confluent Cloud): Create lineages for all configured tables without filtering

**Sink Connectors** (S3, Snowflake, BigQuery, JDBC):

- Support both explicit topic lists (`topics` field) and regex patterns (`topics.regex` field)
- When `topics.regex` is used:
  - Priority 1: Match against `manifest.topic_names` from Kafka API (if available)
  - Priority 2: Query DataHub for Kafka topics and match pattern (if `use_schema_resolver` enabled)
  - Priority 3: Warn user that pattern cannot be expanded

##### Configuration Examples

**Source Connector with Pattern Expansion:**

```yml
# Debezium PostgreSQL source with wildcard tables
connector.config:
  table.include.list: "public.analytics_.*"
  # When Kafka API unavailable, DataHub will:
  # 1. Query DataHub for all PostgreSQL tables matching pattern
  # 2. Derive expected topic names (server.schema.table format)
  # 3. Apply transforms if configured
  # 4. Create lineages without Kafka validation
```

**Sink Connector with topics.regex (Confluent Cloud):**

```yml
# S3 sink connector consuming from pattern-matched topics
connector.config:
  topics.regex: "analytics\\..*" # Match topics like analytics.users, analytics.orders
  # When Kafka API unavailable, DataHub will:
  # 1. Query DataHub for all Kafka topics (requires use_schema_resolver: true)
  # 2. Match topics against the regex pattern
  # 3. Create lineages for matched topics
```

**Enable DataHub Topic Querying for Sink Connectors:**

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-abc456"
    username: "your-connect-api-key"
    password: "your-connect-api-secret"

    # Enable DataHub schema resolver for topic pattern expansion
    use_schema_resolver: true # Required for topics.regex fallback

    # Configure graph connection for DataHub queries
    datahub_gms_url: "http://localhost:8080" # Your DataHub GMS endpoint
```

##### Key Benefits

1. **Confluent Cloud Support**: Both source and sink connectors work correctly with pattern-based configurations
2. **Config as Source of Truth**: Source connectors always derive topics from configuration, not from querying all tables in DataHub
3. **Smart Fallback**: Sink connectors can query DataHub for Kafka topics when Kafka API is unavailable
4. **Pattern Expansion**: Wildcards in `table.include.list` and `topics.regex` are properly expanded
5. **Transform Support**: All transforms (RegexRouter, EventRouter, etc.) are applied correctly

##### When DataHub Topic Querying is Used

DataHub will query for topics in these scenarios:

**Source Connectors:**

- When expanding wildcard patterns in `table.include.list` (e.g., `ANALYTICS.PUBLIC.*`)
- Queries source platform (PostgreSQL, MySQL, etc.) for tables matching the pattern

**Sink Connectors:**

- When `topics.regex` is used AND Kafka API is unavailable (Confluent Cloud)
- Queries DataHub's Kafka platform for topics matching the regex pattern
- Requires `use_schema_resolver: true` in configuration

**Important Notes:**

- DataHub never queries "all tables" to create lineages - config is always the source of truth
- Source connectors query source platforms (databases) to expand table patterns
- Sink connectors query Kafka platform to expand topic regex patterns
- Both require appropriate DataHub credentials and connectivity

#### Using DataHub Schema Resolver for Pattern Expansion and Column-Level Lineage

The Kafka Connect source can query DataHub for schema information to provide two capabilities:

1. **Pattern Expansion** - Converts wildcard patterns like `database.*` into actual table names by querying DataHub
2. **Column-Level Lineage** - Generates field-level lineage by matching schemas between source tables and Kafka topics

Both features require existing metadata in DataHub from your database and Kafka schema registry ingestion.

##### Auto-Enabled for Confluent Cloud

**Starting with the latest version**, `use_schema_resolver` is **automatically enabled** for Confluent Cloud environments to provide better defaults for enhanced lineage extraction. This gives you column-level lineage and pattern expansion out of the box!

**Confluent Cloud (Auto-Enabled):**

```yml
source:
  type: kafka-connect
  config:
    # Confluent Cloud environment
    confluent_cloud_environment_id: "env-xyz123"
    confluent_cloud_cluster_id: "lkc-abc456"
    username: "your-connect-api-key"
    password: "your-connect-api-secret"

    # Schema resolver automatically enabled! ✓
    # use_schema_resolver: true (auto-enabled)
    # schema_resolver_expand_patterns: true (auto-enabled)
    # schema_resolver_finegrained_lineage: true (auto-enabled)
```

**To disable** (if you don't need these features):

```yml
source:
  type: kafka-connect
  config:
    confluent_cloud_environment_id: "env-xyz123"
    confluent_cloud_cluster_id: "lkc-abc456"
    use_schema_resolver: false # Explicitly disable auto-enable
```

**Self-hosted (Manual Enable Required):**

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"

    # Must explicitly enable for self-hosted
    use_schema_resolver: true

    # DataHub connection
    datahub_api:
      server: "http://localhost:8080"
```

**Important Prerequisites:**

> **⚠️ Source database tables must be ingested into DataHub BEFORE running Kafka Connect ingestion**
>
> The schema resolver queries DataHub for existing table metadata. If your source databases haven't been ingested yet, the feature will have no effect. Run database ingestion first!

**Recommended Ingestion Order:**

1. Ingest source databases (PostgreSQL, MySQL, Snowflake, etc.) → DataHub
2. Ingest Kafka schema registry (optional, for topic schemas) → DataHub
3. Run Kafka Connect ingestion → Enjoy enhanced lineage!

##### Configuration Overview

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"

    # Enable DataHub schema querying (auto-enabled for Confluent Cloud)
    use_schema_resolver: true

    # Control which features to use (both default to true when schema resolver enabled)
    schema_resolver_expand_patterns: true # Expand wildcard patterns
    schema_resolver_finegrained_lineage: true # Generate column-level lineage

    # DataHub connection (required when use_schema_resolver=true)
    datahub_api:
      server: "http://localhost:8080"
      token: "your-datahub-token" # Optional
```

##### Pattern Expansion

Converts wildcard patterns in connector configurations into actual table names by querying DataHub.

**Example: MySQL Source with Wildcards**

```yml
# Connector config contains pattern
connector.config:
  table.include.list: "analytics.user_*" # Pattern: matches user_events, user_profiles, etc.

# DataHub config
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_expand_patterns: true
# Result: DataHub queries for MySQL tables matching "analytics.user_*"
# Finds: user_events, user_profiles, user_sessions
# Creates lineage:
#   mysql.analytics.user_events -> kafka.server.analytics.user_events
#   mysql.analytics.user_profiles -> kafka.server.analytics.user_profiles
#   mysql.analytics.user_sessions -> kafka.server.analytics.user_sessions
```

**When to use:**

- Connector configs have wildcard patterns (`database.*`, `schema.table_*`)
- You want accurate lineage without manually listing every table
- Source metadata exists in DataHub from database ingestion

**When to skip:**

- Connector configs use explicit table lists (no patterns)
- Source metadata not yet in DataHub
- Want faster ingestion without DataHub API calls

**Configuration:**

```yml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_expand_patterns: true # Enable pattern expansion


    # If you only want column-level lineage but NOT pattern expansion:
    # schema_resolver_expand_patterns: false
```

**Behavior without schema resolver:**
Patterns are treated as literal table names, resulting in potentially incorrect lineage.

##### Column-Level Lineage

Generates field-level lineage by matching column names through the Kafka Connect pipeline. It is supported for both directions:

- **Source connectors** (DB table → Kafka topic): field names are taken from the source table schema in DataHub
- **Sink connectors** (Kafka topic → DB table): field names are taken from the Kafka topic schema in DataHub

**Prerequisites:**

Both the source and target schemas must already be ingested into DataHub before running Kafka Connect ingestion:

- For source connectors: ingest the source database (e.g. run the Postgres source)
- For sink connectors: ingest both the Kafka topics (e.g. run the Kafka source) and the destination database

**Example: PostgreSQL to Kafka CDC (source direction)**

```yml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_finegrained_lineage: true
# Source table schema in DataHub:
# postgres.public.users: [user_id, email, created_at, updated_at]

# Kafka topic schema in DataHub:
# kafka.server.public.users: [user_id, email, created_at, updated_at]

# Result: Column-level lineage created:
#   postgres.public.users.user_id -> kafka.server.public.users.user_id
#   postgres.public.users.email -> kafka.server.public.users.email
#   postgres.public.users.created_at -> kafka.server.public.users.created_at
#   postgres.public.users.updated_at -> kafka.server.public.users.updated_at
```

Column matching is case-insensitive, so Kafka fields with lowercase names (e.g. `order_id`) will match Snowflake columns stored in uppercase (`ORDER_ID`).

Topic routing transforms (RegexRouter, EventRouter, etc.) work transparently — the transform pipeline correctly identifies which topic maps to which dataset, and column-level lineage operates on those resolved pairs.

**Field-Level Transform Support:**

The following field-level transforms are applied when building the column mapping:

| Transform                             | Effect on column lineage                                                                              |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `ReplaceField$Value`                  | include/exclude filter and field renames are respected                                                |
| `ExtractField$Value`                  | sub-fields of the extracted struct are promoted as top-level (e.g. Debezium `field=after` unwrapping) |
| `HoistField$Value`                    | all fields are nested under the new struct field (e.g. `id` → `data.id`)                              |
| `Flatten$Value`                       | nested struct paths are joined using the configured delimiter (default: `.`)                          |
| `MaskField`, `Cast`, `Filter`, `Drop` | field names unchanged — lineage is unaffected                                                         |

For example, a `ReplaceField` that excludes internal columns:

```yml
transforms: "removeFields"
transforms.removeFields.type: "org.apache.kafka.connect.transforms.ReplaceField$Value"
transforms.removeFields.exclude: "internal_id,temp_column"
# Source schema: [user_id, email, internal_id, temp_column]
# After transform: [user_id, email]
# Column lineage created only for: user_id, email
```

**Configuration:**

```yml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_finegrained_lineage: true # Enable column-level lineage


    # If you only want pattern expansion but NOT column-level lineage:
    # schema_resolver_finegrained_lineage: false
```

**Behavior without schema resolver:**
Only dataset-level lineage is created (e.g., `postgres.users -> kafka.users`), without field-level detail.

##### Complete Configuration Example

```yml
source:
  type: kafka-connect
  config:
    # Kafka Connect cluster
    connect_uri: "http://localhost:8083"
    cluster_name: "production-connect"

    # Enable schema resolver features
    use_schema_resolver: true
    schema_resolver_expand_patterns: true # Expand wildcard patterns
    schema_resolver_finegrained_lineage: true # Generate column-level lineage

    # DataHub connection
    datahub_api:
      server: "http://datahub.company.com"
      token: "${DATAHUB_TOKEN}"

    # Platform instances (if using multiple)
    platform_instance_map:
      postgres: "prod-postgres"
      kafka: "prod-kafka"
```

##### Performance Impact

**API Calls per Connector:**

- Pattern expansion: 1 GraphQL query per unique wildcard pattern
- Column-level lineage: 2 GraphQL queries (source schema + target schema)
- Results cached for ingestion run duration

**Optimization:**

```yml
# Minimal configuration - no schema resolver
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"
    # use_schema_resolver: false  # Default - no DataHub queries

# Pattern expansion only
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_expand_patterns: true
    schema_resolver_finegrained_lineage: false  # Skip column lineage for faster ingestion

# Column lineage only
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    schema_resolver_expand_patterns: false      # Skip pattern expansion
    schema_resolver_finegrained_lineage: true
```

**Best Practice:**
Run database and Kafka schema ingestion before Kafka Connect ingestion to pre-populate DataHub with schema metadata.

##### Troubleshooting

**"Pattern expansion found no matches for: analytics.\*"**

Causes:

- Source database metadata not in DataHub
- Pattern syntax doesn't match DataHub dataset names
- Platform instance mismatch

Solutions:

1. Run database ingestion first to populate DataHub
2. Verify pattern matches table naming in source system
3. Check `platform_instance_map` matches database ingestion config
4. Use explicit table list to bypass pattern expansion temporarily

**"SchemaResolver not available: DataHub graph connection is not available"**

Causes:

- Missing `datahub_api` configuration
- DataHub GMS not accessible

Solutions:

```yml
source:
  type: kafka-connect
  config:
    use_schema_resolver: true
    datahub_api:
      server: "http://localhost:8080" # Add DataHub GMS URL
      token: "your-token" # Add if authentication enabled
```

**Column-level lineage not appearing**

Check:

1. Source table schema exists: Search for table in DataHub UI
2. Kafka topic schema exists: Search for topic in DataHub UI
3. Column names match (case differences are handled automatically)
4. Check ingestion logs for warnings about missing schemas

**Slow ingestion with schema resolver enabled**

Profile:

- Check logs for "Schema resolver cache hits: X, misses: Y"
- High misses indicate missing metadata in DataHub

Temporarily disable to compare:

```yml
use_schema_resolver: false
```

#### Working with Platform Instances

If you've multiple instances of kafka OR source/sink systems that are referred in your `kafka-connect` setup, you'd need to configure platform instance for these systems in `kafka-connect` recipe to generate correct lineage edges. You must have already set `platform_instance` in recipes of original source/sink systems. Refer the document [Working with Platform Instances](https://docs.datahub.com/docs/platform-instances) to understand more about this.

There are two options available to declare source/sink system's `platform_instance` in `kafka-connect` recipe. If single instance of platform is used across all `kafka-connect` connectors, you can use `platform_instance_map` to specify platform_instance to use for a platform when constructing URNs for lineage.

Example:

```yml
# Map of platform name to platform instance
platform_instance_map:
  snowflake: snowflake_platform_instance
  mysql: mysql_platform_instance
```

If multiple instances of platform are used across `kafka-connect` connectors, you'd need to specify platform_instance to use for platform for every connector.

##### Example - Multiple MySQL Source Connectors each reading from different mysql instance

```yml
# Map of platform name to platform instance per connector
connect_to_platform_map:
  mysql_connector1:
    mysql: mysql_instance1

  mysql_connector2:
    mysql: mysql_instance2
```

Here mysql_connector1 and mysql_connector2 are names of MySQL source connectors as defined in `kafka-connect` connector config.

##### Example - Multiple MySQL Source Connectors each reading from difference mysql instance and writing to different kafka cluster

```yml
connect_to_platform_map:
  mysql_connector1:
    mysql: mysql_instance1
    kafka: kafka_instance1

  mysql_connector2:
    mysql: mysql_instance2
    kafka: kafka_instance2
```

You can also use combination of `platform_instance_map` and `connect_to_platform_map` in your recipe. Note that, the platform_instance specified for the connector in `connect_to_platform_map` will always take higher precedance even if platform_instance for same platform is set in `platform_instance_map`.

If you do not use `platform_instance` in original source/sink recipes, you do not need to specify them in above configurations.

Note that, you do not need to specify platform_instance for BigQuery.

##### Example - Multiple BigQuery Sink Connectors each writing to different kafka cluster

```yml
connect_to_platform_map:
  bigquery_connector1:
    kafka: kafka_instance1

  bigquery_connector2:
    kafka: kafka_instance2
```

#### Provided Configurations from External Sources

Kafka Connect supports pluggable configuration providers which can load configuration data from external sources at runtime. These values are not available to DataHub ingestion source through Kafka Connect APIs. If you are using such provided configurations to specify connection url (database, etc) in Kafka Connect connector configuration then you will need also add these in `provided_configs` section in recipe for DataHub to generate correct lineage.

```yml
# Optional mapping of provider configurations if using
provided_configs:
  - provider: env
    path_key: MYSQL_CONNECTION_URL
    value: jdbc:mysql://test_mysql:3306/librarydb
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Air-gapped or Performance-Optimized Environments

Disable topic discovery entirely for environments where API access is not available or not needed:

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"
    use_connect_topics_api: false # Disables all topic discovery API calls
```

**Note**: When `use_connect_topics_api` is `false`, topic information will not be extracted, which may impact lineage accuracy but improves performance and works in air-gapped environments.

#### Topic Discovery Issues

**Problem**: Missing or incomplete topic information in lineage

**Solutions**:

1. **Verify Environment Detection**:

   ```bash
   # Check logs for environment detection messages
   # Self-hosted: "Detected self-hosted Kafka Connect - using runtime topics API"
   # Confluent Cloud: "Detected Confluent Cloud - using comprehensive Kafka REST API topic retrieval"
   ```

2. **Test API Connectivity**:

   ```bash
   # For self-hosted - test topics API
   curl -X GET "http://localhost:8083/connectors/{connector-name}/topics"

   # For Confluent Cloud - test Kafka REST API v3
   curl -X GET "https://pkc-xxxxx.region.provider.confluent.cloud/kafka/v3/clusters/{cluster-id}/topics"
   ```

3. **Configuration Troubleshooting**:
   ```yml
   # Enable debug logging
   source:
     type: kafka-connect
     config:
       # ... other config ...
       use_connect_topics_api: true # Ensure this is enabled (default)
   ```

#### Environment-Specific Issues

**Self-hosted Issues**:

- **403/401 errors**: Check authentication credentials (`username`, `password`)
- **404 errors**: Verify Kafka Connect cluster is running and REST API is accessible
- **Empty topic lists**: Check if connectors are actually running and processing data

**Confluent Cloud Issues**:

- **Missing topics**: Verify connector configuration has proper source table fields (`table.include.list`, `query`)
- **Transform accuracy**: Check that RegexRouter patterns in connector config are valid Java regex
- **Complex transforms**: Now fully supported via forward transform pipeline with topic validation
- **Schema preservation**: Full schema information (e.g., `public.users`) is maintained through transform pipeline

#### Performance Optimization

If topic discovery is impacting performance:

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"
    use_connect_topics_api: false # Disable for better performance (no topic info)
```
