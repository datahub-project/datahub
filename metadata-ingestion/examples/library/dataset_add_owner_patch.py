from datahub.emitter.mce_builder import make_dataset_urn, make_group_urn, make_user_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import OwnerClass, OwnershipTypeClass
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="fct_users_created", env="PROD"
)

# Create Dataset Patch to Add + Remove Owners
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.add_owner(
    OwnerClass(make_user_urn("user-to-add-id"), OwnershipTypeClass.TECHNICAL_OWNER)
)
patch_builder.remove_owner(make_group_urn("group-to-remove-id"))
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)
