# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

dataset_urn = make_dataset_urn(name="fct_users_created", platform="hive")

# Soft-delete the dataset.
graph.delete_entity(urn=dataset_urn, hard=False)

print(f"Deleted dataset {dataset_urn}")
