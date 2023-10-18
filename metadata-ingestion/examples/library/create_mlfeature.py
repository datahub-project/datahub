import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

dataset_urn = builder.make_dataset_urn(
    name="fct_users_created", platform="hive", env="PROD"
)
feature_urn = builder.make_ml_feature_urn(
    feature_table_name="users_feature_table",
    feature_name="user_signup_date",
)

#  Create feature
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlFeature",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=feature_urn,
    aspectName="mlFeatureProperties",
    aspect=models.MLFeaturePropertiesClass(
        description="Represents the date the user created their account",
        # attaching a source to a feature creates lineage between the feature
        # and the upstream dataset. This is how lineage between your data warehouse
        # and machine learning ecosystem is established.
        sources=[dataset_urn],
        dataType="TIME",
    ),
)

# Emit metadata!
emitter.emit(metadata_change_proposal)
