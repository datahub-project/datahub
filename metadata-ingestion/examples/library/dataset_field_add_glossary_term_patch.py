from datahub.emitter.mce_builder import make_dataset_urn, make_term_urn
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import GlossaryTermAssociationClass
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create Dataset URN
dataset_urn = make_dataset_urn(
    platform="snowflake", name="fct_users_created", env="PROD"
)

# Create Dataset Patch to Add + Remove Term for 'profile_id' column
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.for_field("profile_id").add_term(
    GlossaryTermAssociationClass(make_term_urn("term-to-add-id"))
)
patch_builder.for_field("profile_id").remove_term(
    "urn:li:glossaryTerm:term-to-remove-id"
)
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)
