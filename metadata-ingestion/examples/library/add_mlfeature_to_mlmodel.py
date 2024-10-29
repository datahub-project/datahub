import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import MLModelPropertiesClass

gms_endpoint = "http://localhost:8080"
# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

model_urn = builder.make_ml_model_urn(
    model_name="my-test-model", platform="science", env="PROD"
)
feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="my-feature3", feature_table_name="my-feature-table"
    ),
]

# This code concatenates the new features with the existing features in the model
# If you want to replace all existing features with only the new ones, you can comment out this line.
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
model_properties = graph.get_aspect(
    entity_urn=model_urn, aspect_type=MLModelPropertiesClass
)
if model_properties:
    current_features = model_properties.mlFeatures
    print("current_features:", current_features)
    if current_features:
        feature_urns += current_features

model_properties = models.MLModelPropertiesClass(mlFeatures=feature_urns)

# MCP creation
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlModel",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=model_urn,
    aspect=model_properties,
)

# Emit metadata!
emitter.emit(metadata_change_proposal)
