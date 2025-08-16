:::note
Stateful Ingestion is available only when a Platform Instance is assigned to this source.
:::

### Connecting to Confluent Cloud

If using Confluent Cloud you can use a recipe like this. In this `consumer_config.sasl.username` and `consumer_config.sasl.password` are the API credentials that you get (in the Confluent UI) from your cluster -> Data Integration -> API Keys. `schema_registry_config.basic.auth.user.info` has API credentials for Confluent schema registry which you get (in Confluent UI) from Schema Registry -> API credentials.

When creating API Key for the cluster ensure that the ACLs associated with the key are set like below. This is required for DataHub to read topic metadata from topics in Confluent Cloud.

```
Topic Name = *
Permission = ALLOW
Operation = DESCRIBE
Pattern Type = LITERAL
```

```yml
source:
  type: "kafka"
  config:
    platform_instance: "YOUR_CLUSTER_ID"
    connection:
      bootstrap: "abc-defg.eu-west-1.aws.confluent.cloud:9092"
      consumer_config:
        security.protocol: "SASL_SSL"
        sasl.mechanism: "PLAIN"
        sasl.username: "${CLUSTER_API_KEY_ID}"
        sasl.password: "${CLUSTER_API_KEY_SECRET}"
      schema_registry_url: "https://abc-defgh.us-east-2.aws.confluent.cloud"
      schema_registry_config:
        basic.auth.user.info: "${REGISTRY_API_KEY_ID}:${REGISTRY_API_KEY_SECRET}"

sink:
  # sink configs
```

If you are trying to add domains to your topics you can use a configuration like below.

```yml
source:
  type: "kafka"
  config:
    # ...connection block
    domain:
      "urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810":
        allow:
          - ".*"
      "urn:li:domain:d6ec9868-6736-4b1f-8aa6-fee4c5948f17":
        deny:
          - ".*"
```

Note that the `domain` in config above can be either an _urn_ or a domain _id_ (i.e. `urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810` or simply `13ae4d85-d955-49fc-8474-9004c663a810`). The Domain should exist in your DataHub instance before ingesting data into the Domain. To create a Domain on DataHub, check out the [Domains User Guide](https://docs.datahub.com/docs/domains/).

If you are using a non-default subject naming strategy in the schema registry, such as [RecordNameStrategy](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work), the mapping for the topic's key and value schemas to the schema registry subject names should be provided via `topic_subject_map` as shown in the configuration below.

```yml
source:
  type: "kafka"
  config:
    # ...connection block
    # Defines the mapping for the key & value schemas associated with a topic & the subject name registered with the
    # kafka schema registry.
    topic_subject_map:
      # Defines both key & value schema for topic 'my_topic_1'
      "my_topic_1-key": "io.acryl.Schema1"
      "my_topic_1-value": "io.acryl.Schema2"
      # Defines only the value schema for topic 'my_topic_2' (the topic doesn't have a key schema).
      "my_topic_2-value": "io.acryl.Schema3"
```

### Custom Schema Registry

The Kafka Source uses the schema registry to figure out the schema associated with both `key` and `value` for the topic.
By default it uses the [Confluent's Kafka Schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
and supports the `AVRO` and `PROTOBUF` schema types.

If you're using a custom schema registry, or you are using schema type other than `AVRO` or `PROTOBUF`, then you can provide your own
custom implementation of the `KafkaSchemaRegistryBase` class, and implement the `get_schema_metadata(topic, platform_urn)` method that
given a topic name would return object of `SchemaMetadata` containing schema for that topic. Please refer
`datahub.ingestion.source.confluent_schema_registry::ConfluentSchemaRegistry` for sample implementation of this class.

```python
class KafkaSchemaRegistryBase(ABC):
    @abstractmethod
    def get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        pass
```

The custom schema registry class can be configured using the `schema_registry_class` config param of the `kafka` source as shown below.

```YAML
source:
  type: "kafka"
  config:
    # Set the custom schema registry implementation class
    schema_registry_class: "datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry"
    # Coordinates
    connection:
      bootstrap: "broker:9092"
      schema_registry_url: http://localhost:8081
```

### OAuth Callback

The OAuth callback function can be set up using `config.connection.consumer_config.oauth_cb`.

You need to specify a Python function reference in the format &lt;python-module&gt;:&lt;function-name&gt;.

For example, in the configuration `oauth:create_token`, `create_token` is a function defined in `oauth.py`, and `oauth.py` must be accessible in the PYTHONPATH.

```YAML
source:
  type: "kafka"
  config:
    # Set the custom schema registry implementation class
    schema_registry_class: "datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry"
    # Coordinates
    connection:
      bootstrap: "broker:9092"
      schema_registry_url: http://localhost:8081
      consumer_config:
        security.protocol: "SASL_PLAINTEXT"
        sasl.mechanism: "OAUTHBEARER"
        oauth_cb: "oauth:create_token"
# sink configs
```

### Limitations of `PROTOBUF` schema types implementation

The current implementation of the support for `PROTOBUF` schema type has the following limitations:

- Recursive types are not supported.
- If the schemas of different topics define a type in the same package, the source would raise an exception.

In addition to this, maps are represented as arrays of messages. The following message,

```
message MessageWithMap {
  map<int, string> map_1 = 1;
}
```

becomes:

```
message Map1Entry {
  int key = 1;
  string value = 2/
}
message MessageWithMap {
  repeated Map1Entry map_1 = 1;
}
```

### Enriching DataHub metadata with automated meta mapping

:::note
Meta mapping is currently only available for Avro schemas, and requires that those Avro schemas are pushed to the schema registry.
:::

Avro schemas are permitted to have additional attributes not defined by the specification as arbitrary metadata. A common pattern is to utilize this for business metadata. The Kafka source has the ability to transform this directly into DataHub Owners, Tags and Terms.

#### Simple tags

If you simply have a list of tags embedded into an Avro schema (either at the top-level or for an individual field), you can use the `schema_tags_field` config.

Example Avro schema:

```json
{
  "name": "sampleRecord",
  "type": "record",
  "tags": ["tag1", "tag2"],
  "fields": [
    {
      "name": "field_1",
      "type": "string",
      "tags": ["tag3", "tag4"]
    }
  ]
}
```

The name of the field containing a list of tags can be configured with the `schema_tags_field` property:

```yaml
config:
  schema_tags_field: tags
```

#### meta mapping

You can also map specific Avro fields into Owners, Tags and Terms using meta
mapping.

Example Avro schema:

```json
{
  "name": "sampleRecord",
  "type": "record",
  "owning_team": "@Data-Science",
  "data_tier": "Bronze",
  "fields": [
    {
      "name": "field_1",
      "type": "string",
      "gdpr": {
        "pii": true
      }
    }
  ]
}
```

This can be mapped to DataHub metadata with `meta_mapping` config:

```yaml
config:
  meta_mapping:
    owning_team:
      match: "^@(.*)"
      operation: "add_owner"
      config:
        owner_type: group
    data_tier:
      match: "Bronze|Silver|Gold"
      operation: "add_term"
      config:
        term: "{{ $match }}"
  field_meta_mapping:
    gdpr.pii:
      match: true
      operation: "add_tag"
      config:
        tag: "pii"
```

The underlying implementation is similar to [dbt meta mapping](https://docs.datahub.com/docs/generated/ingestion/sources/dbt#dbt-meta-automated-mappings), which has more detailed examples that can be used for reference.

### Data Profiling

The Kafka source supports comprehensive data profiling of message content to generate field-level statistics and sample values. Profiling analyzes message samples from Kafka topics to provide insights into data quality and distribution.

#### Schema-less Topic Support

DataHub can automatically fall back to schema-less processing for topics not found in the schema registry:

```yaml
source:
  type: kafka
  config:
    # ... other config ...

    # Configure schema-less fallback
    schemaless_fallback:
      enabled: true # Enable automatic fallback (disabled by default)
      max_workers: 20 # Parallel processing for multiple topics (default: 5 * CPU cores)
      sample_timeout_seconds: 2.0 # Timeout per topic
      sample_strategy: "hybrid" # "earliest", "latest", or "hybrid"
```

When `schemaless_fallback.enabled` is true:

- Topics without schema registry entries will automatically have their schema inferred from message data
- DataHub will sample a few messages from the topic to determine field names and types
- This allows ingestion to succeed even for topics that don't use the schema registry
- The inferred schema will include field descriptions showing sample values

#### Performance Tuning

For better performance when using schema-less fallback, you can tune the sampling behavior:

```yaml
source:
  type: kafka
  config:
    # ... other config ...

    schemaless_fallback:
      enabled: true
      max_workers: 10 # Parallel processing (default: 5 * CPU cores)
      sample_timeout_seconds: 1.0 # Limit time per topic (default: 2.0)
      sample_strategy: "latest" # Sampling strategy (default: "hybrid")
```

**Performance Benefits:**

- **Caching**: Inferred schemas are automatically cached (60 minute TTL) to avoid re-sampling
- **Timeouts**: Configurable limits prevent hanging on slow/empty topics
- **Parallel Processing**: Multiple topics processed simultaneously using ThreadPoolExecutor
- **Auto-scaling**: Worker count automatically scales based on CPU cores and topic count
- **Hybrid Sampling**: Tries recent messages first (fast), falls back to historical data if needed
- **Optimized Consumer**: Fast Kafka consumer settings for quick message sampling

#### Configuration Examples for Different Scenarios

**High-Performance Setup (Many topics, powerful machine)**:

```yaml
schemaless_fallback:
  enabled: true
  max_workers: 20 # Higher parallelism
  sample_timeout_seconds: 1.0
  sample_strategy: "latest" # Fastest - recent messages only
```

**Resource-Constrained Setup (Limited CPU/memory)**:

```yaml
schemaless_fallback:
  enabled: true
  max_workers: 2 # Lower parallelism
  sample_timeout_seconds: 2.0
  sample_strategy: "hybrid"
```

**Conservative Setup (Prioritize accuracy over speed)**:

```yaml
schemaless_fallback:
  enabled: true
  max_workers: 5
  sample_timeout_seconds: 5.0
  sample_strategy: "earliest" # Most comprehensive - scan from beginning
```

#### Sampling Strategies

DataHub supports three sampling strategies for schema inference:

- **`hybrid` (default)**: Tries `latest` first for speed, falls back to `earliest` if no recent messages found. Best of both worlds.
- **`latest`**: Only reads recent messages. Fastest but may fail on quiet topics.
- **`earliest`**: Scans from the beginning of topic history. Most comprehensive but slower on large topics.

**Strategy Performance Comparison**:
| Strategy | Speed | Coverage | Best For |
|----------|-------|----------|----------|
| `latest` | ⚡⚡⚡ | 🔍 | Active topics, speed priority |
| `hybrid` | ⚡⚡ | 🔍🔍🔍 | **Recommended - balanced** |
| `earliest` | ⚡ | 🔍🔍🔍 | Comprehensive analysis |

If you prefer the old behavior (warnings for missing schemas), set `enable_schemaless_fallback: false`.

#### Basic Profiling Configuration

To enable profiling, add the `profiling` section to your configuration:

```yaml
source:
  type: "kafka"
  config:
    # ...connection block
    profiling:
      enabled: true
      sample_size: 1000
      max_sample_time_seconds: 60
```

#### Advanced Profiling Configuration

The Kafka source supports all standard Great Expectations profiling features plus Kafka-specific optimizations:

```yaml
source:
  type: "kafka"
  config:
    # ...connection block
    profiling:
      # Basic settings
      enabled: true
      sample_size: 1000 # Number of messages to sample per topic
      max_sample_time_seconds: 60 # Maximum time to spend sampling each topic

      # Sampling strategy
      sampling_strategy: "latest" # Options: latest, random, stratified, full

      # Performance settings
      max_workers: 4 # Parallel profiling workers
      batch_size: 100 # Messages per batch for efficient reading

      # Field-level profiling controls
      include_field_null_count: true
      include_field_distinct_count: true
      include_field_min_value: true
      include_field_max_value: true
      include_field_mean_value: true
      include_field_median_value: true
      include_field_stddev_value: true
      include_field_quantiles: false # Expensive, disabled by default
      include_field_distinct_value_frequencies: false # Expensive
      include_field_histogram: false # Expensive
      include_field_sample_values: true

      # Performance optimization
      turn_off_expensive_profiling_metrics: false
      field_sample_values_limit: 20
      max_number_of_fields_to_profile: 100

      # Complex data handling
      flatten_max_depth: 5 # Max recursion depth for nested JSON/Avro

      # Scheduling (optional)
      operation_config:
        lower_freq_profile_enabled: false # Enable scheduled profiling
        profile_day_of_week: 1 # Monday=0, Sunday=6
        profile_date_of_month: 15 # Run on 15th of each month
```

#### Sampling Strategies

The Kafka source provides multiple sampling strategies optimized for different use cases:

- **`latest`** (default): Samples the most recent messages from the end of each partition
- **`random`**: Samples messages from random offsets across partitions
- **`stratified`**: Evenly distributes samples across the topic timeline
- **`full`**: Processes the entire topic (respects `sample_size` limit)

#### Performance Considerations

For high-throughput topics, consider these optimizations:

```yaml
profiling:
  enabled: true
  # Reduce sample size for faster processing
  sample_size: 500
  max_sample_time_seconds: 30

  # Use latest strategy for best performance
  sampling_strategy: "latest"

  # Enable expensive metrics only when needed
  turn_off_expensive_profiling_metrics: true
  max_number_of_fields_to_profile: 50

  # Increase parallelization
  max_workers: 8
  batch_size: 200
```

#### Scheduled Profiling

You can configure profiling to run only on specific days or dates to reduce resource usage:

```yaml
profiling:
  enabled: true
  operation_config:
    lower_freq_profile_enabled: true
    profile_day_of_week: 0 # Run only on Mondays
    # OR
    profile_date_of_month: 1 # Run only on 1st of each month
```

#### Handling Complex Data

The Kafka source automatically handles nested JSON and Avro structures by flattening them into individual fields. For deeply nested data, you can control the recursion depth:

```yaml
profiling:
  enabled: true
  flatten_max_depth: 3 # Prevent stack overflow on deep nesting
```

This is particularly useful for topics with complex nested messages or potential circular references.
