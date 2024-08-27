import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import MLFeatureTablePropertiesClass

gms_endpoint = "http://localhost:8080"
# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

feature_table_urn = builder.make_ml_feature_table_urn(
    feature_table_name="my-feature-table", platform="feast"
)
feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="my-feature2", feature_table_name="my-feature-table"
    ),
]

# This code concatenates the new features with the existing features in the feature table.
# If you want to replace all existing features with only the new ones, you can comment out this line.
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
feature_table_properties = graph.get_aspect(
    entity_urn=feature_table_urn, aspect_type=MLFeatureTablePropertiesClass
)
if feature_table_properties:
    current_features = feature_table_properties.mlFeatures
    print("current_features:", current_features)
    if current_features:
        feature_urns += current_features

feature_table_properties = models.MLFeatureTablePropertiesClass(mlFeatures=feature_urns)
# MCP createion
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlFeatureTable",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=feature_table_urn,
    aspect=feature_table_properties,
)

# Emit metadata! This is a blocking call
emitter.emit(metadata_change_proposal)
