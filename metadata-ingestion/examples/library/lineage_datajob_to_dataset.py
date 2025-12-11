# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

dataflow_urn = DataFlowUrn(
    orchestrator="airflow", flow_id="data_pipeline", cluster="PROD"
)

client.lineage.add_lineage(
    upstream=DataJobUrn(flow=dataflow_urn, job_id="data_job_1"),
    downstream=DatasetUrn(platform="postgres", name="raw_data"),
)
