import time

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = builder.make_dataset_urn(
    platform="hive", name="logging.events.clickstream", env="PROD"
)

field_urn = builder.make_schema_field_urn(parent_urn=dataset_urn, field_path="user_id")

current_properties = graph.get_aspect(
    entity_urn=field_urn, aspect_type=models.StructuredPropertiesClass
)

property_urn = "urn:li:structuredProperty:io.acryl.dataQuality.score"
property_value = "0.95"

new_assignment = models.StructuredPropertyValueAssignmentClass(
    propertyUrn=property_urn,
    values=[property_value],
    created=models.AuditStampClass(
        time=int(time.time() * 1000), actor=builder.make_user_urn("datahub")
    ),
)

if current_properties and current_properties.properties:
    property_exists = False
    for i, prop in enumerate(current_properties.properties):
        if prop.propertyUrn == property_urn:
            current_properties.properties[i] = new_assignment
            property_exists = True
            break
    if not property_exists:
        current_properties.properties.append(new_assignment)
else:
    current_properties = models.StructuredPropertiesClass(properties=[new_assignment])

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=field_urn,
        aspect=current_properties,
    )
)
