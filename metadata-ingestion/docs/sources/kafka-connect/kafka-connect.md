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
Uses comprehensive Kafka REST API v3 for optimal transform pipeline support with config-based fallback:

```yml
source:
  type: kafka-connect
  config:
    # Confluent Cloud Connect URI - automatically detected
    connect_uri: "https://api.confluent.cloud/connect/v1/environments/env-123/clusters/lkc-abc456"
    username: "your-api-key"      # Confluent Cloud API key (shared for both Connect and Kafka)
    password: "your-api-secret"   # Confluent Cloud API secret (shared for both Connect and Kafka)
    
    # Optional: Dedicated Kafka REST endpoint for comprehensive topic retrieval  
    kafka_rest_endpoint: "https://pkc-xxxxx.region.provider.confluent.cloud"
    # kafka_api_key: "separate-kafka-key"     # Optional: Use different credentials for Kafka API
    # kafka_api_secret: "separate-kafka-secret" # Optional: Use different credentials for Kafka API
    
    # use_connect_topics_api: true  # Default - enables comprehensive topic retrieval with credential reuse
```

**Enhanced Topic Retrieval:**
DataHub automatically enables comprehensive topic retrieval by:
1. **Auto-deriving REST endpoint** from connector configs (looks for `kafka.endpoint` fields)
2. **Getting all topics** from the Kafka cluster via REST API v3
3. **Reusing Connect credentials** for Kafka API authentication (same API key/secret in Confluent Cloud)
4. **Applying the reverse transform pipeline** to discover topic mappings  
5. **Creating accurate lineage** for complex transform scenarios
6. **Gracefully fallback** to config-based derivation if API fails

**Configuration Options:**
- **Auto-derivation (recommended)**: DataHub finds Kafka REST endpoint automatically from connector configs
- **Manual endpoint**: Specify `kafka_rest_endpoint` if auto-derivation doesn't work
- **Shared credentials**: Use `username`/`password` for both Connect and Kafka APIs
- **Separate credentials (rare)**: Use `kafka_api_key`/`kafka_api_secret` for dedicated access

#### Air-gapped or Performance-Optimized Environments
Disable topic discovery entirely for environments where API access is not available or not needed:

```yml
source:
  type: kafka-connect
  config:
    connect_uri: "http://localhost:8083"
    use_connect_topics_api: false  # Disables all topic discovery API calls
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
       use_connect_topics_api: true  # Ensure this is enabled (default)
   ```

### Environment-Specific Issues

**Self-hosted Issues**:
- **403/401 errors**: Check authentication credentials (`username`, `password`)
- **404 errors**: Verify Kafka Connect cluster is running and REST API is accessible
- **Empty topic lists**: Check if connectors are actually running and processing data

**Confluent Cloud Issues**:
- **Missing topics**: Verify connector configuration has proper topic fields (`topics`, `kafka.topic`, `table.include.list`)
- **Dynamic topics**: Topics created at runtime may not appear in static configuration
- **Complex transforms**: Now fully supported via reverse transform pipeline strategy

### Performance Optimization

If topic discovery is impacting performance:

```yml
source:
  type: kafka-connect  
  config:
    connect_uri: "http://localhost:8083"
    use_connect_topics_api: false  # Disable for better performance (no topic info)
```
