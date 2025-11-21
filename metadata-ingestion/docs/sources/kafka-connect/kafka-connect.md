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
