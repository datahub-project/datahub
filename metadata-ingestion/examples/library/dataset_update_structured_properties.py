import logging

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.specific.dataset import DatasetPatchBuilder

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create rest emitter
rest_emitter = DataHubRestEmitter(gms_server="http://localhost:8080")

dataset_urn = make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")


for patch_mcp in (
    DatasetPatchBuilder(dataset_urn)
    .set_structured_property("io.acryl.dataManagement.replicationSLA", 120)
    .build()
):
    rest_emitter.emit(patch_mcp)


log.info(f"Added cluster_name, retention_time properties to dataset {dataset_urn}")
