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

primary_key_urns = [
    builder.make_ml_primary_key_urn(
        feature_table_name="customer_features",
        primary_key_name="customer_id",
    )
]

# Read existing properties to preserve other fields
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
feature_table_properties = graph.get_aspect(
    entity_urn=feature_table_urn,
    aspect_type=models.MLFeatureTablePropertiesClass,
)

if feature_table_properties:
    feature_table_properties.mlPrimaryKeys = primary_key_urns
    updated_properties = feature_table_properties
else:
    updated_properties = models.MLFeatureTablePropertiesClass(
        mlPrimaryKeys=primary_key_urns,
    )

metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=feature_table_urn,
    aspect=updated_properties,
)

emitter.emit(metadata_change_proposal)

# Also create the primary key entity with its properties
dataset_urn = builder.make_dataset_urn(
    name="customers", platform="snowflake", env="PROD"
)
primary_key_urn = primary_key_urns[0]

primary_key_properties = models.MLPrimaryKeyPropertiesClass(
    description="Unique identifier for customers in the system",
    dataType="TEXT",
    sources=[dataset_urn],
)

pk_metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=primary_key_urn,
    aspect=primary_key_properties,
)

emitter.emit(pk_metadata_change_proposal)
