import logging
from typing import Union

from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.specific.dataset import DatasetPatchBuilder

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


dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

property_map_to_set = {
    "cluster_name": "datahubproject.acryl.io",
    "retention_time": "2 years",
}

with get_emitter() as emitter:
    for patch_mcp in (
        DatasetPatchBuilder(dataset_urn)
        .set_custom_properties(property_map_to_set)
        .build()
    ):
        emitter.emit(patch_mcp)


log.info(
    f"Replaced custom properties on dataset {dataset_urn} as {property_map_to_set}"
)
