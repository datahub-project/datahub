from datahub.sdk import DashboardUrn, DataHubClient, TagUrn

client = DataHubClient.from_env()

dashboard = client.entities.get(DashboardUrn("looker", "dashboards.999999"))

dashboard.add_tag(TagUrn("Production"))

client.entities.update(dashboard)

print(f"Added tag {TagUrn('Production')} to dashboard {dashboard.urn}")
