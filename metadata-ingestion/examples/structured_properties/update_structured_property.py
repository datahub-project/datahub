import logging
from typing import Union

from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.urns import StructuredPropertyUrn
from datahub.specific.structured_property import StructuredPropertyPatchBuilder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Get an emitter, either REST or Kafka, this example shows you both
def get_emitter() -> Union[DataHubRestEmitter, DatahubKafkaEmitter]:
    USE_REST_EMITTER = True
    if USE_REST_EMITTER:
        gms_endpoint = "http://localhost:8080"
        return DataHubRestEmitter(gms_server=gms_endpoint)
    else:
        kafka_server = "localhost:9092"
        schema_registry_url = "http://localhost:8081"
        return DatahubKafkaEmitter(
            config=KafkaEmitterConfig(
                connection=KafkaProducerConnectionConfig(
                    bootstrap=kafka_server, schema_registry_url=schema_registry_url
                )
            )
        )


# input your unique structured property ID
property_urn = StructuredPropertyUrn("io.acryl.dataManagement.dataSteward")

with get_emitter() as emitter:
    for patch_mcp in (
        StructuredPropertyPatchBuilder(str(property_urn))
        .set_display_name("test display name")
        .set_cardinality("MULTIPLE")
        .add_entity_type("urn:li:entityType:datahub.dataJob")
        .build()
    ):
        emitter.emit(patch_mcp)
