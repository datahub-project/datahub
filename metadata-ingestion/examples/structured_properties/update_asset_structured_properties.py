import logging

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.specific.structured_properties import StructuredPropertiesAssetPatchBuilder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))


# get the structuredProperties aspect for a given asset
asset_urn = "urn:li:glossaryTerm:test123"


patch_builder = StructuredPropertiesAssetPatchBuilder(asset_urn)

# remove a property we don't want on the asset anymore
property_to_remove = "urn:li:structuredProperty:removeMe"

patch_builder = patch_builder.remove_structured_property(property_to_remove)

# edit an existing property's value on an asset (editMe is a structured property
# of type String and single cardinality)
edited_urn = "urn:li:structuredProperty:editMe"
patch_builder.set_structured_property(edited_urn, "new value")

# add a new property to an asset
new_property_urn = "urn:li:structuredProperty:addMe"
patch_builder.add_structured_property(new_property_urn, "newly added")


for mcp in patch_builder.build():
    rest_emitter.emit(mcp)
