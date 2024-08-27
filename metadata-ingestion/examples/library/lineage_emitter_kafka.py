import datahub.emitter.mce_builder as builder
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig

# Construct a lineage object.
lineage_mce = builder.make_lineage_mce(
    [
        builder.make_dataset_urn("bigquery", "upstream1"),
        builder.make_dataset_urn("bigquery", "upstream2"),
    ],
    builder.make_dataset_urn("bigquery", "downstream"),
)

# Create an emitter to DataHub's Kafka broker.
emitter = DatahubKafkaEmitter(
    KafkaEmitterConfig.parse_obj(
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


emitter.emit_mce_async(lineage_mce, callback)
emitter.flush()
