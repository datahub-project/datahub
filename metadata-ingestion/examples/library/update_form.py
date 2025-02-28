import logging
from typing import Union

from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import (
    FormPromptClass,
    FormPromptTypeClass,
    FormTypeClass,
    OwnerClass,
    OwnershipTypeClass,
    StructuredPropertyParamsClass,
)
from datahub.metadata.urns import FormUrn
from datahub.specific.form import FormPatchBuilder

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


# input your unique form ID
form_urn = FormUrn("metadata_initiative_1")

# example prompts to add, must reference an existing structured property
new_prompt = FormPromptClass(
    id="abcd",
    title="title",
    type=FormPromptTypeClass.STRUCTURED_PROPERTY,
    structuredPropertyParams=StructuredPropertyParamsClass(
        "urn:li:structuredProperty:io.acryl.test"
    ),
    required=True,
)
new_prompt2 = FormPromptClass(
    id="1234",
    title="title",
    type=FormPromptTypeClass.FIELDS_STRUCTURED_PROPERTY,
    structuredPropertyParams=StructuredPropertyParamsClass(
        "urn:li:structuredProperty:io.acryl.test"
    ),
    required=True,
)

with get_emitter() as emitter:
    for patch_mcp in (
        FormPatchBuilder(str(form_urn))
        .add_owner(
            OwnerClass(
                owner="urn:li:corpuser:jdoe", type=OwnershipTypeClass.TECHNICAL_OWNER
            )
        )
        .set_name("New Name")
        .set_description("New description here")
        .set_type(FormTypeClass.VERIFICATION)
        .set_ownership_form(True)
        .add_prompts([new_prompt, new_prompt2])
        .build()
    ):
        emitter.emit(patch_mcp)
