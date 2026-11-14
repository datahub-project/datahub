
# Python Emitter

In some cases, you might want to construct Metadata events directly and use programmatic ways to emit that metadata to DataHub. Use-cases are typically push-based and include emitting metadata events from CI/CD pipelines, custom orchestrators etc.

The `acryl-datahub` Python package offers REST and Kafka emitter API-s, which can easily be imported and called from your own code.

> **Pro Tip!** Throughout our API guides, we have examples of using Python API SDK.
> Lookout for the `| Python |` tab within our tutorials.

## Installation

Follow the installation guide for the main `acryl-datahub` package [here](./README.md#install-from-pypi). Read on for emitter specific installation instructions.

## REST Emitter

The REST emitter is a thin wrapper on top of the `requests` module and offers a blocking interface for sending metadata events over HTTP. Use this when simplicity and acknowledgement of metadata being persisted to DataHub's metadata store is more important than throughput of metadata emission. Also use this when read-after-write scenarios exist, e.g. writing metadata and then immediately reading it back.

### Installation

```console
pip install -U `acryl-datahub[datahub-rest]`
```

### Example Usage

```python
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass

from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

# For DataHub Cloud, you will want to point to your DataHub Cloud's server's GMS endpoint
# emitter = DatahubRestEmitter(gms_server="https://<your-domain>.acryl.io/gms", token="<your token>", extra_headers={})

# Test the connection
emitter.test_connection()

# Construct a dataset properties object
dataset_properties = DatasetPropertiesClass(description="This table stored the canonical User profile",
    customProperties={
         "governance": "ENABLED"
    })

# Construct a MetadataChangeProposalWrapper object.
metadata_event = MetadataChangeProposalWrapper(
    entityUrn=builder.make_dataset_urn("bigquery", "my-project.my-dataset.user-table"),
    aspect=dataset_properties,
)

# Emit metadata! This is a blocking call
emitter.emit(metadata_event)
```

Other examples:

- [lineage_emitter_mcpw_rest.py](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_mcpw_rest.py) - emits simple bigquery table-to-table (dataset-to-dataset) lineage via REST as MetadataChangeProposalWrapper.

### Emit Modes

`emit()` and `emit_mcp()` accept an optional `emit_mode: EmitMode` argument that controls how long the call waits and what consistency it guarantees. The emitter default is `SYNC_PRIMARY`. You can override it per call, or set `default_emit_mode` once on the emitter.

| `EmitMode`                 | Call returns after…                          | Use when                                                                  |
| -------------------------- | -------------------------------------------- | ------------------------------------------------------------------------- |
| `SYNC_WAIT`                | SQL **and** Elasticsearch are updated        | The write must be immediately searchable; strongest consistency, slowest. |
| `SYNC_PRIMARY` _(default)_ | SQL is updated (Elasticsearch indexed async) | Low-volume writes that need read-after-write on direct entity gets.       |
| `ASYNC`                    | The change is queued (returns immediately)   | High-throughput or bulk ingestion where eventual consistency is fine.     |
| `ASYNC_WAIT`               | The queued change is confirmed persisted     | You want async batching/parallelism but still need persistence confirmed. |

#### Choosing a mode

- **Low volume, and you read back or need failures raised at the call site** → keep `SYNC_PRIMARY` (or `SYNC_WAIT` if the write must be searchable immediately).
- **High-volume or bulk ingestion** → set `ASYNC` explicitly.

The default `SYNC_PRIMARY` is **not** suited to high-throughput or bulk ingestion: a synchronous primary-storage commit per write puts heavy load on GMS and its backing SQL store at volume. Custom scripts and direct SDK usage that emit at scale should opt into `ASYNC`.

#### Async behavior to be aware of

With `ASYNC`, the change is processed after the call returns. Two consequences:

- **`emit()` does not raise on a rejected or invalid write.** The call succeeds once the change is queued; validation or persistence failures surface later in the Failed-MCP topic and consumer logs, not at the call site.
- **No read-after-write guarantee.** Any flow that writes and then immediately reads the same entity must tolerate eventual consistency.

#### Example

```python
from datahub.emitter.rest_emitter import DatahubRestEmitter, EmitMode

# Set the default for every emit on this emitter (recommended for bulk ingestion)
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", default_emit_mode=EmitMode.ASYNC)

# Or override per call
emitter.emit(metadata_event, emit_mode=EmitMode.ASYNC)
```

### Emitter Code

If you're interested in looking at the REST emitter code, it is available [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/emitter/rest_emitter.py)

## Kafka Emitter

The Kafka emitter is a thin wrapper on top of the SerializingProducer class from `confluent-kafka` and offers a non-blocking interface for sending metadata events to DataHub. Use this when you want to decouple your metadata producer from the uptime of your datahub metadata server by utilizing Kafka as a highly available message bus. For example, if your DataHub metadata service is down due to planned or unplanned outages, you can still continue to collect metadata from your mission critical systems by sending it to Kafka. Also use this emitter when throughput of metadata emission is more important than acknowledgement of metadata being persisted to DataHub's backend store.

**_Note_**: The Kafka emitter uses Avro to serialize the Metadata events to Kafka. Changing the serializer will result in unprocessable events as DataHub currently expects the metadata events over Kafka to be serialized in Avro.

### Installation

```console
# For emission over Kafka
pip install -U `acryl-datahub[datahub-kafka]`
```

### Example Usage

```python
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass

from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
# Create an emitter to Kafka
kafka_config = {
    "connection": {
        "bootstrap": "localhost:9092",
        "schema_registry_url": "http://localhost:8081",
        "schema_registry_config": {}, # schema_registry configs passed to underlying schema registry client
        "producer_config": {}, # extra producer configs passed to underlying kafka producer
    }
}

emitter = DatahubKafkaEmitter(
    KafkaEmitterConfig.parse_obj(kafka_config)
)

# Construct a dataset properties object
dataset_properties = DatasetPropertiesClass(description="This table stored the canonical User profile",
    customProperties={
         "governance": "ENABLED"
    })

# Construct a MetadataChangeProposalWrapper object.
metadata_event = MetadataChangeProposalWrapper(
    entityUrn=builder.make_dataset_urn("bigquery", "my-project.my-dataset.user-table"),
    aspect=dataset_properties,
)


# Emit metadata! This is a non-blocking call
emitter.emit(
    metadata_event,
    callback=lambda exc, message: print(f"Message sent to topic:{message.topic()}, partition:{message.partition()}, offset:{message.offset()}") if message else print(f"Failed to send with: {exc}")
)

#Send all pending events
emitter.flush()
```

### Emitter Code

If you're interested in looking at the Kafka emitter code, it is available [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/emitter/kafka_emitter.py)

## Other Languages

Emitter API-s are also supported for:

- [Java](../metadata-integration/java/as-a-library.md)
