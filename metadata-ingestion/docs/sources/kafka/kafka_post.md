### Capabilities

:::note
Stateful Ingestion is available only when a Platform Instance is assigned to this source.
:::

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Connecting to Confluent Cloud

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

#### Custom Schema Registry

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

```yaml
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

#### OAuth Callback

The OAuth callback function can be set up for both Kafka sources (consumers) and sinks (producers):

- For sources: `config.connection.consumer_config.oauth_cb`
- For sinks: `config.connection.producer_config.oauth_cb`

You need to specify a Python function reference in the format &lt;python-module&gt;:&lt;function-name&gt;.

For example, in the configuration `oauth:create_token`, `create_token` is a function defined in `oauth.py`, and `oauth.py` must be accessible in the PYTHONPATH.

##### Deploying Custom OAuth Callbacks

**For Built-in Callbacks (Recommended):**

DataHub includes pre-built OAuth callbacks for common use cases:

- **AWS MSK IAM**: `datahub_actions.utils.kafka_msk_iam:oauth_cb`
- **Azure Event Hubs**: `datahub_actions.utils.kafka_eventhubs_auth:oauth_cb`

**Important:** To use these built-in callbacks, you must install the `acryl-datahub-actions` package:

```bash
pip install acryl-datahub-actions>=1.3.1.2
```

**For Custom OAuth Callbacks:**

If you need to implement a custom OAuth callback, you must ensure your Python module is accessible to the DataHub process, e.g. adding it via `PYTHONPATH=/path/to/your/module:$PYTHONPATH` or `pip install my-oauth-package`.

**Example for Kafka Source:**

```yaml
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

**Example for Kafka Sink (e.g., MSK IAM authentication):**

```yaml
sink:
  type: "datahub-kafka"
  config:
    connection:
      bootstrap: "b-1.msk.us-west-2.amazonaws.com:9098"
      schema_registry_url: "http://datahub-gms:8080/schema-registry/api/"
      producer_config:
        security.protocol: "SASL_SSL"
        sasl.mechanism: "OAUTHBEARER"
        sasl.oauthbearer.method: "default"
        oauth_cb: "datahub_actions.utils.kafka_msk_iam:oauth_cb"
```

#### Enriching DataHub metadata with automated meta mapping

:::note
Meta mapping is currently only available for Avro schemas, and requires that those Avro schemas are pushed to the schema registry.
:::

Avro schemas are permitted to have additional attributes not defined by the specification as arbitrary metadata. A common pattern is to utilize this for business metadata. The Kafka source has the ability to transform this directly into DataHub Owners, Tags and Terms.

##### Simple tags

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

##### meta mapping

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

#### Schema Resolution

DataHub provides multi-stage schema resolution for topics whose schemas are not registered in the schema registry, or that use non-default naming strategies. This feature is **independent** of data profiling.

When `schema_resolution.enabled` is true, DataHub attempts to resolve schemas using the following stages in order:

1. **TopicNameStrategy**: Direct lookup using `<topic>-key/value` pattern (most common)
2. **TopicSubjectMap**: User-defined topic-to-subject mappings via `topic_subject_map` config
3. **RecordNameStrategy**: Extract record names from message data and lookup `<record_name>-key/value`
4. **TopicRecordNameStrategy**: Combine topic + record name: `<topic>-<record_name>-key/value`
5. **Schema Inference**: As final fallback, infer schema from message data analysis

This ensures maximum compatibility with different Confluent Schema Registry [naming strategies](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work).

```yaml
source:
  type: kafka
  config:
    schema_resolution:
      enabled: true # disabled by default
      sample_timeout_seconds: 2.0
      sample_strategy: "hybrid" # "earliest", "latest", or "hybrid"
      max_messages_per_topic: 10

    profiling:
      max_workers: 20 # controls parallelization for both profiling and schema resolution
      nested_field_max_depth: 5
```

**Sampling strategies for schema inference:**

- **`hybrid` (default)**: Tries `latest` first for speed, falls back to `earliest` if no recent messages found.
- **`latest`**: Only reads recent messages. Fastest but may fail on quiet topics.
- **`earliest`**: Scans from the beginning of topic history. Most comprehensive but slower on large topics.

**Performance notes:**

- Inferred schemas are cached for 60 minutes to avoid re-sampling.
- Worker count scales automatically based on CPU cores and topic count.
- Set `schema_resolution.enabled: false` to receive warnings for missing schemas instead of automatic resolution.

#### Data Profiling

The Kafka source supports data profiling of message content to generate field-level statistics and sample values. Profiling is completely independent of schema resolution — either can be enabled without the other.

```yaml
source:
  type: "kafka"
  config:
    profiling:
      enabled: true
      sample_size: 200 # messages to sample per topic
      max_sample_time_seconds: 60
      sampling_strategy: "latest" # latest, random, stratified, or full
      max_workers: 4
      batch_size: 100

      # Field-level statistics
      include_field_null_count: true
      include_field_distinct_count: true
      include_field_min_value: true
      include_field_max_value: true
      include_field_mean_value: true
      include_field_median_value: true
      include_field_stddev_value: true
      include_field_quantiles: false # expensive, disabled by default
      include_field_distinct_value_frequencies: false # expensive
      include_field_histogram: false # expensive
      include_field_sample_values: true

      # Nested field handling
      profile_nested_fields: true
      nested_field_max_depth: 10

      # Scheduled profiling (optional)
      operation_config:
        lower_freq_profile_enabled: false
        profile_day_of_week: 1 # Monday=0, Sunday=6
        profile_date_of_month: 15
```

**Sampling strategies:**

- **`latest`** (default): Samples the most recent messages from the end of each partition.
- **`random`**: Samples messages from random offsets across partitions.
- **`stratified`**: Evenly distributes samples across the topic timeline.
- **`full`**: Processes the entire topic (respects `sample_size` limit).

The `nested_field_max_depth` setting (default: 10) prevents recursion errors on deeply nested or circular JSON structures. Reduce it for topics with complex nested messages.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### `PROTOBUF` Schema Type Limitations

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

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Schema Parsing Errors

DataHub automatically handles schema parsing errors gracefully and continues processing.

##### Avro Binary Encoding Errors

**Error:** `avro.errors.InvalidAvroBinaryEncoding: Read 0 bytes, expected 1 bytes`

Enable schema resolution to automatically infer schemas:

```yaml
source:
  type: kafka
  config:
    schema_resolution:
      enabled: true
      sample_strategy: "hybrid"
```

##### Protobuf Duplicate Symbol Errors

**Error:** `Couldn't build proto file into descriptor pool: duplicate symbol`

DataHub logs warnings and continues processing. Topics with schema conflicts will use inferred schemas if schema resolution is enabled.

##### Schema Registry Connection Issues

If schema registry is unavailable, enable schema resolution as a fallback:

```yaml
source:
  type: kafka
  config:
    connection:
      schema_registry_url: "http://localhost:8081"
    schema_resolution:
      enabled: true
```

#### Performance Optimization

For large Kafka clusters with many topics:

```yaml
source:
  type: kafka
  config:
    topic_patterns:
      allow: ["prod_.*", "analytics_.*"]
      deny: [".*_temp", ".*_test"]

    profiling:
      enabled: true
      max_workers: 20 # controls both profiling and schema resolution parallelization
      sample_size: 100
      nested_field_max_depth: 10

    schema_resolution:
      enabled: true
      sample_timeout_seconds: 1.0
```

#### Memory Issues with Large Topics

For topics with large messages or high volume, reduce the sample size and recursion depth:

```yaml
profiling:
  enabled: true
  sample_size: 50
  nested_field_max_depth: 2
```
