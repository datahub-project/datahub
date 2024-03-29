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

# For Acryl, you will want to point to your Acryl server's GMS endpoint
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

- [lineage_emitter_mcpw_rest.py](./examples/library/lineage_emitter_mcpw_rest.py) - emits simple bigquery table-to-table (dataset-to-dataset) lineage via REST as MetadataChangeProposalWrapper.

### Emitter Code

If you're interested in looking at the REST emitter code, it is available [here](./src/datahub/emitter/rest_emitter.py)

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

If you're interested in looking at the Kafka emitter code, it is available [here](./src/datahub/emitter/kafka_emitter.py)

## Other Languages

Emitter API-s are also supported for:

- [Java](../metadata-integration/java/as-a-library.md)
