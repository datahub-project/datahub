# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.sdk import Chart, Dashboard, DataHubClient

client = DataHubClient.from_env()

# Create charts that belong to the dashboard
chart1 = Chart(platform="looker", name="sales_by_region_chart")
chart2 = Chart(platform="looker", name="revenue_trend_chart")
chart3 = Chart(platform="looker", name="customer_count_chart")

# Create dashboard with charts
dashboard = Dashboard(
    platform="looker",
    name="sales_dashboard",
    display_name="Sales Overview Dashboard",
    description="Comprehensive sales analytics dashboard",
)

# Add charts to the dashboard
dashboard.add_chart(chart1)
dashboard.add_chart(chart2)
dashboard.add_chart(chart3)

# Upsert the dashboard (this will also create the chart relationships)
client.entities.upsert(dashboard)
