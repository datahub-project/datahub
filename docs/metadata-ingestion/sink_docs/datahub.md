
# DataHub

## DataHub Rest

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

### Setup

To install this plugin, run `pip install 'acryl-datahub[datahub-rest]'`.

### Capabilities

Pushes metadata to DataHub using the GMS REST API. The advantage of the REST-based interface
is that any errors can immediately be reported.

### Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes). This should point to the GMS server.

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

If you are connecting to a hosted DataHub Cloud instance, your sink will look like

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    server: "https://<your-instance>.acryl.io/gms"
    token: <token>
```

If you are running the ingestion in a container in docker and your [GMS is also running in docker](../../docker/README.md) then you should use the internal docker hostname of the GMS pod. Usually it would look something like

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    server: "http://datahub-gms:8080"
```

If GMS is running in a kubernetes pod [deployed through the helm charts](../../docs/deploy/kubernetes.md) and you are trying to connect to it from within the kubernetes cluster then you should use the Kubernetes service name of GMS. Usually it would look something like

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    server: "http://datahub-datahub-gms.datahub.svc.cluster.local:8080"
```

If you are using [UI based ingestion](../../docs/ui-ingestion.md) then where GMS is deployed decides what hostname you should use.

### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                      | Required | Default              | Description                                                                                                                                             |
| -------------------------- | -------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server`                   | ✅       |                      | URL of DataHub GMS endpoint.                                                                                                                            |
| `token`                    |          |                      | Bearer token used for authentication.                                                                                                                   |
| `timeout_sec`              |          | 30                   | Per-HTTP request timeout.                                                                                                                               |
| `retry_max_times`          |          | 1                    | Maximum times to retry if HTTP request fails. The delay between retries is increased exponentially                                                      |
| `retry_status_codes`       |          | [429, 502, 503, 504] | Retry HTTP request also on these status codes                                                                                                           |
| `extra_headers`            |          |                      | Extra headers which will be added to the request.                                                                                                       |
| `max_threads`              |          | `15`                 | Max parallelism for REST API calls                                                                                                                      |
| `mode`                     |          | `ASYNC_BATCH`        | [Advanced] Mode of operation - `SYNC`, `ASYNC`, or `ASYNC_BATCH`                                                                                        |
| `ca_certificate_path`      |          |                      | Path to server's CA certificate for verification of HTTPS communications                                                                                |
| `client_certificate_path`  |          |                      | Path to client's CA certificate for HTTPS communications                                                                                                |
| `disable_ssl_verification` |          | false                | Disable ssl certificate validation                                                                                                                      |
| `respect_mcp_sync_marker`  |          | false                | [Advanced] Upgrade a batch to synchronous when any MCP carries the `emitModeMarker=sync` system-metadata marker. See limitation and requirements below. |

#### Marker-aware sync routing (`respect_mcp_sync_marker`)

When enabled, a batch is upgraded to synchronous (`async=false`) if any of its MCPs carries the `emitModeMarker=sync` marker in its system metadata; otherwise the configured `mode` is honored unchanged. It only ever forces more synchronicity, never less. The marker is read, not produced, by the sink: a producer must populate the `emitModeMarker` system-metadata property (to `sync`) on the writes that must remain synchronous (e.g. via a custom aspect mutator/validator or an upstream processing step).

This feature is supported only in specific configurations of DataHub Cloud and only when adopted under specific direction of DataHub. This feature requires a dedicated pool of logical model propagation workers and that MCP throttling be enabled.

## DataHub Kafka

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

#### Note: Requires direct Kafka access

The `datahub-kafka` sink produces directly to the Kafka cluster, so the process running ingestion must have network access to the broker and schema registry. This holds for OSS and for in-cluster managed ingestion. Executors that run outside the cluster (e.g. remote executors with no direct Kafka access) cannot use it — those should emit via the `datahub-rest` sink instead.

### Setup

To install this plugin, run `pip install 'acryl-datahub[datahub-kafka]'`.

### Capabilities

Pushes metadata to DataHub by publishing messages to Kafka. The advantage of the Kafka-based
interface is that it's asynchronous and can handle higher throughput.

### Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  # source configs

sink:
  type: "datahub-kafka"
  config:
    connection:
      bootstrap: "localhost:9092"
      schema_registry_url: "http://localhost:8081"
```

#### Kafka with OAuth

E.g. for AWS MSK clusters using IAM authentication:

```yml
source:
  # source configs
sink:
  type: "datahub-kafka"
  config:
    topic_routes:
      MetadataChangeEvent: "custom_mce_topic_name" # Optional override
      MetadataChangeProposal: "custom_mcp_topic_name" # Optional override
    connection:
      bootstrap: "b-1.your-msk-cluster.region.amazonaws.com:9098"
      schema_registry_url: "http://datahub-gms:8080/schema-registry/api/"
      producer_config:
        security.protocol: "SASL_SSL"
        sasl.mechanism: "OAUTHBEARER"
        sasl.oauthbearer.method: "default"
        oauth_cb: "datahub_actions.utils.kafka_msk_iam:oauth_cb" # OAuth callback
```

**Note:** MSK IAM authentication requires `pip install 'acryl-datahub-actions>=1.3.1.2'` for the OAuth callback.

### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                        | Required | Default                | Description                                                                                                                                              |
| -------------------------------------------- | -------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection.bootstrap`                       | ✅       |                        | Kafka bootstrap URL.                                                                                                                                     |
| `connection.producer_config.<option>`        |          |                        | Passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.SerializingProducer                  |
| `connection.producer_config.oauth_cb`        |          |                        | OAuth callback function for authentication (e.g., `datahub_actions.utils.kafka_msk_iam:oauth_cb` for MSK IAM)                                            |
| `connection.schema_registry_url`             | ✅       |                        | URL of schema registry being used.                                                                                                                       |
| `connection.schema_registry_config.<option>` |          |                        | Passed to https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.schema_registry.SchemaRegistryClient |
| `topic_routes.MetadataChangeEvent`           |          | MetadataChangeEvent    | Overridden Kafka topic name for the MetadataChangeEvent                                                                                                  |
| `topic_routes.MetadataChangeProposal`        |          | MetadataChangeProposal | Overridden Kafka topic name for the MetadataChangeProposal                                                                                               |

The options in the producer config and schema registry config are passed to the Kafka SerializingProducer and SchemaRegistryClient respectively.

For a full example with a number of security options, see this [example recipe](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/recipes/secured_kafka.dhub.yaml).

### Kafka as the default sink (managed / executor-only)

A recipe that omits `sink:` normally defaults to the REST sink. In an executor
deployment that can reach the Kafka cluster, that default can be flipped to the
`datahub-kafka` sink so ingestion writes go to the MCP topic instead of GMS's
synchronous REST path — moving write load off GMS. This is **off by default**
and configured entirely through environment variables on the executor (the
recipe is unchanged, so an explicit `sink:` always wins and is never affected).

It is controlled by a single variable. Set it only on the runs that should use
Kafka (e.g. the in-cluster managed executor); a CLI `datahub ingest` never sets
it and so stays on REST:

| Environment variable                    | Default        | Description                                                                                                                                         |
| --------------------------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATAHUB_INGESTION_DEFAULT_SINK`        | `datahub-rest` | Set to `datahub-kafka` to default no-sink recipes to the Kafka sink. Unset/any other value keeps REST (byte-identical).                             |
| `KAFKA_BOOTSTRAP_SERVER`                |                | Broker the sink produces to (the existing DataHub-wide convention). If unset, the run falls back to REST.                                           |
| `KAFKA_SCHEMAREGISTRY_URL`              |                | Schema registry for MCP schemas (existing convention; defaults to the GMS-hosted registry via GMS base path).                                       |
| `DATAHUB_KAFKA_SINK_QUEUE_MAX_KBYTES`   | 131072         | Per-producer local send-buffer cap (KiB); bounds memory so backpressure engages before OOM.                                                         |
| `DATAHUB_KAFKA_SINK_QUEUE_MAX_MESSAGES` | 20000          | Per-producer local send-buffer cap (message count).                                                                                                 |
| `DATAHUB_KAFKA_SINK_LINGER_MS`          | 100            | Producer `linger.ms` (send-batching window).                                                                                                        |
| `DATAHUB_KAFKA_SINK_MAX_MESSAGE_BYTES`  | 5242880        | Max serialized message size sent to Kafka. Raises librdkafka's ~1 MiB default to the MCP topic max; must be ≤ the broker/topic `max.message.bytes`. |
| `DATAHUB_KAFKA_SINK_INIT_PROBE_TIMEOUT` | 10             | Per-check timeout (seconds) for the broker + schema-registry reachability probe at pipeline init.                                                   |

#### DELETE / soft-delete over Kafka

GMS's async (Kafka) path only accepts the change types
`UPSERT`/`UPDATE`/`CREATE`/`CREATE_ENTITY` (plus `PATCH`); `DELETE` and
`RESTATE` are REST-only, and timeseries aspects are UPSERT-only. When the Kafka
default sink is active it auto-wires a REST fallback (from the same client
config the REST sink would use) and routes exactly those change types
synchronously over REST.

Note that stateful-ingestion stale-entity removal emits a `Status(removed=true)`
**UPSERT**, so it flows over Kafka normally and is _not_ affected by the
fallback — the fallback only handles literal `DELETE`/`RESTATE` MCPs (e.g. hard
deletes) and non-UPSERT timeseries changes, which typical ingestion sources do
not emit.

Ordering across the two paths is **best-effort**: before a fallback DELETE the
sink flushes the local producer queue, but it does not wait for the MCP consumer
to _apply_ earlier Kafka writes. Under consumer lag a synchronous REST DELETE can
still reach GMS before an earlier Kafka UPSERT for the same entity is replayed.
If a preceding async Kafka write for the run is unconfirmed, the fallback DELETE
is skipped and the record fails rather than deleting an entity whose write never
landed. DELETE-heavy recipes that mix change types should prefer the REST sink.

#### Failure behavior and monitoring

- **Unreachable broker / registry at init** — a bounded probe runs when the
  Kafka default is selected; if either endpoint is unreachable (or
  `KAFKA_BOOTSTRAP_SERVER` is unset), the run **degrades to REST** and logs a
  `WARNING`. Sustained fallback across runs means Kafka is down and GMS is
  taking the full write load again — alert on it.
- **Full producer queue** — the sink blocks and polls to apply backpressure
  rather than crashing, up to `max_queue_full_block_seconds` (300s) before
  failing the run.
- **Oversized aspect** — an aspect larger than `DATAHUB_KAFKA_SINK_MAX_MESSAGE_BYTES`
  (or the broker/topic `max.message.bytes`) fails that record via the delivery
  callback and is reported as a run failure — never silently dropped. Raise both
  the env var and the topic limit together if you need larger aspects.
- **Run report** — the sink report exposes `kafka_backpressure_engagements`,
  `kafka_backpressure_blocked_seconds`, and `kafka_undelivered` so throttling
  and drops are visible in the run summary, not just logs. Pair with
  consumer-lag monitoring on the MCP topic to confirm writes are applied.

## DataHub Lite (experimental)

A sink that provides integration with [DataHub Lite](../../docs/datahub_lite.md) for local metadata exploration and serving.

### Setup

To install this plugin, run `pip install 'acryl-datahub[datahub-lite]'`.

### Capabilities

Pushes metadata to a local DataHub Lite instance.

### Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  # source configs
sink:
  type: "datahub-lite"
```

By default, `datahub-lite` uses a **DuckDB** database and will write to a database file located under **~/.datahub/lite/**.

To configure the location, you can specify it directly in the config:

```yml
source:
  # source configs
sink:
  type: "datahub-lite"
  config:
    type: "duckdb"
    config:
      file: "<path_to_duckdb_file>"
```

:::note

DataHub Lite currently doesn't support stateful ingestion, so you'll have to turn off stateful ingestion in your recipe to use it. This will be fixed shortly.

:::

### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field    | Required | Default                                      | Description                                                                                                                      |
| -------- | -------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `type`   |          | duckdb                                       | Type of DataHub Lite implementation to use                                                                                       |
| `config` |          | `{"file": "~/.datahub/lite/datahub.duckdb"}` | Config dictionary to pass through to the DataHub Lite implementation. See below for fields accepted by the DuckDB implementation |

#### DuckDB Config Details

| Field     | Required | Default                            | Description                                                                                                                                                        |
| --------- | -------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `file`    |          | `"~/.datahub/lite/datahub.duckdb"` | File to use for DuckDB storage                                                                                                                                     |
| `options` |          | `{}`                               | Options dictionary to pass through to DuckDB library. See [the official spec](https://duckdb.org/docs/sql/configuration.html) for the options supported by DuckDB. |
