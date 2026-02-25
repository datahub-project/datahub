import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

deployment_urn = builder.make_ml_model_deployment_urn(
    platform="sagemaker",
    deployment_name="recommendation-endpoint",
    env="PROD",
)

owner_to_add = builder.make_user_urn("mlops_team")

current_ownership = graph.get_aspect(
    entity_urn=deployment_urn, aspect_type=models.OwnershipClass
)

if current_ownership:
    if owner_to_add not in [owner.owner for owner in current_ownership.owners]:
        current_ownership.owners.append(
            models.OwnerClass(
                owner=owner_to_add,
                type=models.OwnershipTypeClass.TECHNICAL_OWNER,
            )
        )
else:
    current_ownership = models.OwnershipClass(
        owners=[
            models.OwnerClass(
                owner=owner_to_add,
                type=models.OwnershipTypeClass.TECHNICAL_OWNER,
            )
        ]
    )

emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=deployment_urn,
        aspect=current_ownership,
    )
)
