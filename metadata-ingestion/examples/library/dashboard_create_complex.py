# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, Dashboard, DataHubClient, Dataset

client = DataHubClient.from_env()
dashboard1 = Dashboard(
    name="example_dashboard_2",
    platform="looker",
    description="looker dashboard for production",
)
chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
)

input_dataset = Dataset(
    name="example_dataset5",
    platform="snowflake",
    description="snowflake dataset for production",
)


dashboard2 = Dashboard(
    name="example_dashboard",
    platform="looker",
    description="looker dashboard for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
    input_datasets=[input_dataset.urn],
    charts=[chart.urn],
    dashboards=[dashboard1.urn],
)


client.entities.upsert(dashboard1)
client.entities.upsert(chart)
client.entities.upsert(input_dataset)

client.entities.upsert(dashboard2)
