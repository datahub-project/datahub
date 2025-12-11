# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk.main_client import DataHubClient

client = DataHubClient.from_env()

dataset_urn = DatasetUrn(platform="snowflake", name="upstream_table")
datajob_urn = DataJobUrn(
    job_id="example_datajob",
    flow=DataFlowUrn(orchestrator="airflow", flow_id="example_dag", cluster="PROD"),
)

client.lineage.add_lineage(upstream=datajob_urn, downstream=dataset_urn)
