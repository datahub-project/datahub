import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

dataset_urn = builder.make_dataset_urn(
    name="fct_users_deleted", platform="hive", env="PROD"
)
feature_urn = builder.make_ml_feature_urn(
    feature_table_name="my-feature-table",
    feature_name="my-feature",
)

#  Create feature
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlFeature",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=feature_urn,
    aspectName="mlFeatureProperties",
    aspect=models.MLFeaturePropertiesClass(
        description="my feature", sources=[dataset_urn], dataType="TEXT"
    ),
)

# Emit metadata!
emitter.emit(metadata_change_proposal)
