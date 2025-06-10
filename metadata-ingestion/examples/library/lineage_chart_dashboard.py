from datahub.metadata.urns import ChartUrn, DashboardUrn
from datahub.sdk import DataHubClient

client = DataHubClient.from_env()

dashboard = client.entities.get(
    DashboardUrn(dashboard_id="dashboard_1", dashboard_tool="looker")
)
dashboard.add_chart(ChartUrn(chart_id="chart_1", dashboard_tool="looker"))
dashboard.add_chart(ChartUrn(chart_id="chart_2", dashboard_tool="looker"))

client.entities.upsert(dashboard)
