import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})
model_group_urn = builder.make_ml_model_group_urn(
    group_name="my-recommendations-model-group", platform="science", env="PROD"
)


metadata_change_proposal = MetadataChangeProposalWrapper(
    entityType="mlModelGroup",
    changeType=models.ChangeTypeClass.UPSERT,
    entityUrn=model_group_urn,
    aspectName="mlModelGroupProperties",
    aspect=models.MLModelGroupPropertiesClass(
        description="Grouping of ml model training runs related to home page recommendations.",
    ),
)


# Emit metadata!
emitter.emit(metadata_change_proposal)
