import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

dataset_urn = builder.make_dataset_urn(
    name="fct_users_created", platform="hive", env="PROD"
)
primary_key_urn = builder.make_ml_primary_key_urn(
    feature_table_name="users_feature_table",
    primary_key_name="user_id",
)

#  Create feature
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlPrimaryKey",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=primary_key_urn,
    aspectName="mlPrimaryKeyProperties",
    aspect=models.MLPrimaryKeyPropertiesClass(
        description="Represents the id of the user the other features relate to.",
        # attaching a source to a ml primary key creates lineage between the feature
        # and the upstream dataset. This is how lineage between your data warehouse
        # and machine learning ecosystem is established.
        sources=[dataset_urn],
        dataType="TEXT",
    ),
)

# Emit metadata!
emitter.emit(metadata_change_proposal)
