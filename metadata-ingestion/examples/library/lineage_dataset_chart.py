# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import ChartUrn, DatasetUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

# add dataset -> chart lineage
client.lineage.add_lineage(
    upstream=DatasetUrn(platform="hdfs", name="dataset1", env="PROD"),
    downstream=ChartUrn(dashboard_tool="looker", chart_id="chart_1"),
)
