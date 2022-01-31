import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass

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
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_dataset_urn("bigquery", "my-project.my-dataset.user-table"),
    aspectName="datasetProperties",
    aspect=dataset_properties,
)


# Emit metadata! This is a non-blocking call
emitter.emit(
    metadata_event,
    callback=lambda exc, message: print(f"Message sent to topic:{message.topic()}, partition:{message.partition()}, offset:{message.offset()}") if message else print(f"Failed to send with: {exc}")
)

#Send all pending events
emitter.flush()






