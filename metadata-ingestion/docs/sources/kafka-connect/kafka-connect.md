## Advanced Configurations

### Environment-Specific Topic Discovery

DataHub's Kafka Connect source automatically detects your environment (self-hosted vs Confluent Cloud) and uses the appropriate topic discovery strategy:

#### Self-hosted Kafka Connect

Uses the runtime `/connectors/{name}/topics` API endpoint for accurate, real-time topic information:

```yml
source:
  type: kafka-connect
  config:
    # Self-hosted Kafka Connect cluster
    connect_uri: "http://localhost:8083"
    # use_connect_topics_api: true  # Default - enables runtime topic discovery
```

#### Confluent Cloud

Uses comprehensive transform pipeline support with Kafka REST API v3 topic validation and config-based fallback:

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

**How Lineage Inference Works with Transform Pipelines:**

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

### Enhanced Topic Resolution for Source and Sink Connectors

DataHub now provides intelligent topic resolution that works reliably across all environments, including Confluent Cloud where the Kafka Connect topics API is unavailable.

#### How It Works

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

#### Configuration Examples

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

#### Key Benefits

1. **Confluent Cloud Support**: Both source and sink connectors work correctly with pattern-based configurations
2. **Config as Source of Truth**: Source connectors always derive topics from configuration, not from querying all tables in DataHub
3. **Smart Fallback**: Sink connectors can query DataHub for Kafka topics when Kafka API is unavailable
4. **Pattern Expansion**: Wildcards in `table.include.list` and `topics.regex` are properly expanded
5. **Transform Support**: All transforms (RegexRouter, EventRouter, etc.) are applied correctly

#### When DataHub Topic Querying is Used

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

### Using DataHub Schema Resolver for Pattern Expansion and Column-Level Lineage

The Kafka Connect source can query DataHub for schema information to provide two capabilities:

1. **Pattern Expansion** - Converts wildcard patterns like `database.*` into actual table names by querying DataHub
2. **Column-Level Lineage** - Generates field-level lineage by matching schemas between source tables and Kafka topics

Both features require existing metadata in DataHub from your database and Kafka schema registry ingestion.

#### Auto-Enabled for Confluent Cloud

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

#### Configuration Overview

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

#### Pattern Expansion

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

#### Column-Level Lineage

Generates field-level lineage by matching column names between source tables and Kafka topics.

**Example: PostgreSQL to Kafka CDC**

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

**Requirements:**

- Source table schema exists in DataHub (from database ingestion)
- Kafka topic schema exists in DataHub (from schema registry or Kafka ingestion)
- Column names match between source and target (case-insensitive matching)

**Benefits:**

- **Impact Analysis**: See which fields are affected by schema changes
- **Data Tracing**: Track specific data elements through pipelines
- **Schema Understanding**: Visualize how data flows at the field level

**ReplaceField Transform Support:**

Column-level lineage respects ReplaceField transforms that filter or rename columns:

```yml
# Connector excludes specific fields
connector.config:
  transforms: "removeFields"
  transforms.removeFields.type: "org.apache.kafka.connect.transforms.ReplaceField$Value"
  transforms.removeFields.exclude: "internal_id,temp_column"
# DataHub behavior:
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

#### Complete Configuration Example

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

#### Performance Impact

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

#### Troubleshooting

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

### Working with Platform Instances

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

#### Example - Multiple MySQL Source Connectors each reading from different mysql instance

```yml
# Map of platform name to platform instance per connector
connect_to_platform_map:
  mysql_connector1:
    mysql: mysql_instance1

  mysql_connector2:
    mysql: mysql_instance2
```

Here mysql_connector1 and mysql_connector2 are names of MySQL source connectors as defined in `kafka-connect` connector config.

#### Example - Multiple MySQL Source Connectors each reading from difference mysql instance and writing to different kafka cluster

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

#### Example - Multiple BigQuery Sink Connectors each writing to different kafka cluster

```yml
connect_to_platform_map:
  bigquery_connector1:
    kafka: kafka_instance1

  bigquery_connector2:
    kafka: kafka_instance2
```

### Provided Configurations from External Sources

Kafka Connect supports pluggable configuration providers which can load configuration data from external sources at runtime. These values are not available to DataHub ingestion source through Kafka Connect APIs. If you are using such provided configurations to specify connection url (database, etc) in Kafka Connect connector configuration then you will need also add these in `provided_configs` section in recipe for DataHub to generate correct lineage.

```yml
# Optional mapping of provider configurations if using
provided_configs:
  - provider: env
    path_key: MYSQL_CONNECTION_URL
    value: jdbc:mysql://test_mysql:3306/librarydb
```

## Troubleshooting

### Topic Discovery Issues

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

### Environment-Specific Issues

**Self-hosted Issues**:

- **403/401 errors**: Check authentication credentials (`username`, `password`)
- **404 errors**: Verify Kafka Connect cluster is running and REST API is accessible
- **Empty topic lists**: Check if connectors are actually running and processing data

**Confluent Cloud Issues**:

- **Missing topics**: Verify connector configuration has proper source table fields (`table.include.list`, `query`)
- **Transform accuracy**: Check that RegexRouter patterns in connector config are valid Java regex
- **Complex transforms**: Now fully supported via forward transform pipeline with topic validation
- **Schema preservation**: Full schema information (e.g., `public.users`) is maintained through transform pipeline

### Performance Optimization

If topic discovery is impacting performance:

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"
    use_connect_topics_api: false # Disable for better performance (no topic info)
```
