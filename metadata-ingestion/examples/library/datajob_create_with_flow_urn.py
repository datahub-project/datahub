# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk import DataHubClient, DataJob

client = DataHubClient.from_env()

# datajob will inherit the platform and platform instance from the flow

datajob = DataJob(
    name="example_datajob",
    flow_urn=DataFlowUrn(
        orchestrator="airflow",
        flow_id="example_dag",
        cluster="PROD",
    ),
    platform_instance="PROD",
    inlets=[
        DatasetUrn(platform="hdfs", name="dataset1", env="PROD"),
    ],
    outlets=[
        DatasetUrn(platform="hdfs", name="dataset2", env="PROD"),
    ],
)

client.entities.upsert(datajob)
