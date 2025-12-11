# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, DataHubClient, Dataset

client = DataHubClient.from_env()

input_datasets = [
    Dataset(
        name="example_dataset",
        platform="snowflake",
        description="looker dataset for production",
        schema=[("id", "string"), ("name", "string")],
    ),
    Dataset(
        name="example_dataset_2",
        platform="snowflake",
        description="looker dataset for production",
        schema=[("id", "string"), ("name", "string")],
    ),
    Dataset(
        name="example_dataset_3",
        platform="snowflake",
        description="looker dataset for production",
        schema=[("id", "string"), ("name", "string")],
    ),
]

# create a chart with two input datasets
chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
    input_datasets=[input_datasets[0], input_datasets[1]],
)

for dataset in input_datasets:
    client.entities.upsert(dataset)

# add a new dataset to the chart
chart.add_input_dataset(input_datasets[2])
client.entities.upsert(chart)
