# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import DatasetUrn, TagUrn
from datahub.sdk import DataFlow, DataHubClient, DataJob

client = DataHubClient.from_env()

# datajob will inherit the platform and platform instance from the flow

dataflow = DataFlow(
    platform="airflow",
    name="example_dag",
    platform_instance="PROD",
    description="example dataflow",
    tags=[TagUrn(name="tag1"), TagUrn(name="tag2")],
)

datajob = DataJob(
    name="example_datajob",
    flow=dataflow,
    inlets=[
        DatasetUrn(platform="hdfs", name="dataset1", env="PROD"),
    ],
    outlets=[
        DatasetUrn(platform="hdfs", name="dataset2", env="PROD"),
    ],
)

client.entities.upsert(dataflow)
client.entities.upsert(datajob)
