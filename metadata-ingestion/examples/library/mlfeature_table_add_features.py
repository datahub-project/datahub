import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})

feature_table_urn = builder.make_ml_feature_table_urn(
    feature_table_name="customer_features", platform="feast"
)

new_feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="customer_lifetime_value",
        feature_table_name="customer_features",
    ),
    builder.make_ml_feature_urn(
        feature_name="days_since_last_purchase",
        feature_table_name="customer_features",
    ),
    builder.make_ml_feature_urn(
        feature_name="total_purchase_count",
        feature_table_name="customer_features",
    ),
]

# Read existing features to avoid overwriting them
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
feature_table_properties = graph.get_aspect(
    entity_urn=feature_table_urn,
    aspect_type=models.MLFeatureTablePropertiesClass,
)

if feature_table_properties and feature_table_properties.mlFeatures:
    existing_features = feature_table_properties.mlFeatures
    all_feature_urns = list(set(existing_features + new_feature_urns))
else:
    all_feature_urns = new_feature_urns

updated_properties = models.MLFeatureTablePropertiesClass(
    mlFeatures=all_feature_urns,
    description="Customer features with newly added purchase metrics",
)

metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=feature_table_urn,
    aspect=updated_properties,
)

emitter.emit(metadata_change_proposal)
