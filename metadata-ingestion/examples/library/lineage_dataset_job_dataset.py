# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

datajob_urn = DataJobUrn(
    flow=DataFlowUrn(orchestrator="airflow", flow_id="flow1", cluster="PROD"),
    job_id="job1",
)
input_dataset_urn = DatasetUrn(platform="mysql", name="librarydb.member", env="PROD")
input_datajob_urn = DataJobUrn(
    flow=DataFlowUrn(orchestrator="airflow", flow_id="data_pipeline", cluster="PROD"),
    job_id="job0",
)
output_dataset_urn = DatasetUrn(
    platform="kafka", name="debezium.topics.librarydb.member_checkout", env="PROD"
)


# add datajob -> datajob lineage
client.lineage.add_lineage(
    upstream=input_datajob_urn,
    downstream=datajob_urn,
)

# add dataset -> datajob lineage
client.lineage.add_lineage(
    upstream=input_dataset_urn,
    downstream=datajob_urn,
)

# add datajob -> dataset lineage
client.lineage.add_lineage(
    upstream=datajob_urn,
    downstream=output_dataset_urn,
)
