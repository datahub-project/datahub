import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

feature_table_urn = builder.make_ml_feature_table_urn(
    feature_table_name="users_feature_table", platform="feast"
)

feature_urns = [
    builder.make_ml_feature_urn(
        feature_name="user_signup_date", feature_table_name="users_feature_table"
    ),
    builder.make_ml_feature_urn(
        feature_name="user_last_active_date", feature_table_name="users_feature_table"
    ),
]

primary_key_urns = [
    builder.make_ml_primary_key_urn(
        feature_table_name="users_feature_table",
        primary_key_name="user_id",
    )
]

feature_table_properties = models.MLFeatureTablePropertiesClass(
    description="Test description",
    # link your features to a feature table
    mlFeatures=feature_urns,
    # link your primary keys to the feature table
    mlPrimaryKeys=primary_key_urns,
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
