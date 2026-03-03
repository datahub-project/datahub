from datahub.sdk import ChartUrn, DataHubClient

client = DataHubClient.from_env()

# Or get this from the UI (share -> copy urn) and use ChartUrn.from_string(...)
chart_urn = ChartUrn("looker", "example_chart_id")

chart_entity = client.entities.get(chart_urn)
print("Chart name:", chart_entity.name)
print("Chart platform:", chart_entity.platform)
print("Chart description:", chart_entity.description)
