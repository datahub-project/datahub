# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
