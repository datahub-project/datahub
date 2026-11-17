import datahub.emitter.mce_builder as builder
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

# Construct a lineage object.
lineage_mcp = MetadataChangeProposalWrapper(
    entityUrn=builder.make_dataset_urn("bigquery", "downstream"),
    aspect=UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=builder.make_dataset_urn("bigquery", "upstream1"),
                type=DatasetLineageTypeClass.TRANSFORMED,
            ),
            UpstreamClass(
                dataset=builder.make_dataset_urn("bigquery", "upstream2"),
                type=DatasetLineageTypeClass.TRANSFORMED,
            ),
        ]
    ),
)

# Create an emitter to DataHub's Kafka broker.
emitter = DatahubKafkaEmitter(
    KafkaEmitterConfig.model_validate(
        # This is the same config format as the standard Kafka sink's YAML.
        {
            "connection": {
                "bootstrap": "broker:9092",
                "producer_config": {},
                "schema_registry_url": "http://schema-registry:8081",
            }
        }
    )
)


# Emit metadata!
def callback(err, msg):
    if err:
        # Handle the metadata emission error.
        print("error:", err)


emitter.emit_mcp_async(lineage_mcp, callback)
emitter.flush()
