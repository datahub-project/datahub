from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
rest_emitter = DataHubRestEmitter(gms_server="http://localhost:8080")

# Create Dataset URN
dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

# Create Dataset Patch to Add and Remove Structured Properties
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.add_structured_property(
    "urn:li:structuredProperty:retentionTimeInDays", 12
)
patch_builder.remove_structured_property(
    "urn:li:structuredProperty:customClassification"
)
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    rest_emitter.emit(patch_mcp)
