import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))


# get the structuredProperties aspect for a given asset
asset_urn = "urn:li:glossaryTerm:test123"
structured_properties = graph.get_aspect(
    entity_urn=asset_urn, aspect_type=StructuredPropertiesClass
)

# if it doesn't exist, default to an empty list of properties
if structured_properties is None:
    structured_properties = StructuredPropertiesClass(properties=[])


# remove a property we don't want on the asset anymore
property_to_remove = "urn:li:structuredProperty:removeMe"
updated_properties = [
    prop
    for prop in structured_properties.properties
    if prop.propertyUrn != property_to_remove
]
structured_properties.properties = updated_properties


# edit an existing property's value on an asset
edited_urn = "urn:li:structuredProperty:editMe"
edited_property = [
    prop for prop in structured_properties.properties if prop.propertyUrn == edited_urn
][0]
edited_property.values = ["new value"]

for index, property in enumerate(structured_properties.properties):
    if property.propertyUrn == edited_urn:
        structured_properties.properties[index] = edited_property


# add a new property to an asset
new_property_urn = "urn:li:structuredProperty:addMe"
new_property = StructuredPropertyValueAssignmentClass(
    propertyUrn=new_property_urn, values=["newly added"]
)
structured_properties.properties.append(new_property)


event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=str(asset_urn),
    aspect=structured_properties,
)
rest_emitter.emit(event)
