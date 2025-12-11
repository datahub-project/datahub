# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import Chart, DataHubClient, Dataset

client = DataHubClient.from_env()

# Define the source datasets
upstream_dataset1 = Dataset(platform="bigquery", name="project.dataset.sales_table")
upstream_dataset2 = Dataset(platform="bigquery", name="project.dataset.customer_table")

# Create a chart with lineage to upstream datasets
chart = Chart(
    name="sales_by_customer_chart",
    platform="looker",
    display_name="Sales by Customer",
    description="Bar chart showing total sales aggregated by customer",
    input_datasets=[upstream_dataset1, upstream_dataset2],
)

client.entities.upsert(chart)
