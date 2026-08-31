from datahub.sdk import DashboardUrn, DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use DashboardUrn.from_string(...)
dashboard_urn = DashboardUrn("looker", "example_dashboard_id")

dashboard_entity = client.entities.get(dashboard_urn)
print("Dashboard name:", dashboard_entity.name)
print("Dashboard platform:", dashboard_entity.platform)
print("Dashboard description:", dashboard_entity.description)
