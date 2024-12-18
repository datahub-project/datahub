from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.specific.dataset import DatasetPatchBuilder

# Create DataHub Client
rest_emitter = DataHubRestEmitter(gms_server="http://localhost:8080")

# Create Dataset URN
dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")

# Create Dataset Patch to Add Custom Properties
patch_builder = DatasetPatchBuilder(dataset_urn)
patch_builder.add_custom_property("cluster_name", "datahubproject.acryl.io")
patch_builder.add_custom_property("retention_time", "2 years")
patch_mcps = patch_builder.build()

# Emit Dataset Patch
for patch_mcp in patch_mcps:
    rest_emitter.emit(patch_mcp)
