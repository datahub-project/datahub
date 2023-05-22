import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

feature_table_urn = builder.make_ml_feature_table_urn(
    feature_table_name="my-feature-table", platform="feast"
)
feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="my-feature", feature_table_name="my-feature-table"
    ),
    builder.make_ml_feature_urn(
        feature_name="my-feature2", feature_table_name="my-feature-table"
    ),
]
feature_table_properties = models.MLFeatureTablePropertiesClass(
    description="Test description", mlFeatures=feature_urns
)

# MCP creation
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlFeatureTable",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=feature_table_urn,
    aspect=feature_table_properties,
)

# Emit metadata!
emitter.emit(metadata_change_proposal)
