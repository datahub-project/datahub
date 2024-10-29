import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

model_group_urns = [
    builder.make_ml_model_group_urn(
        group_name="my-model-group", platform="science", env="PROD"
    )
]
model_urn = builder.make_ml_model_urn(
    model_name="science-model", platform="science", env="PROD"
)

# This code concatenates the new features with the existing features in the feature table.
# If you want to replace all existing features with only the new ones, you can comment out this line.
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

target_model_properties = graph.get_aspect(
    entity_urn=model_urn, aspect_type=models.MLModelPropertiesClass
)
if target_model_properties:
    current_model_groups = target_model_properties.groups
    print("current_model_groups:", current_model_groups)
    if current_model_groups:
        model_group_urns += current_model_groups

model_properties = models.MLModelPropertiesClass(groups=model_group_urns)
# MCP createion
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlModel",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=model_urn,
    aspect=model_properties,
)

# Emit metadata! This is a blocking call
emitter.emit(metadata_change_proposal)
