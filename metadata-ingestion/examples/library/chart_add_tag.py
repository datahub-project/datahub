from datahub.sdk import ChartUrn, DataHubClient, TagUrn

client = DataHubClient.from_env()

chart = client.entities.get(ChartUrn("looker", "sales_dashboard"))

chart.add_tag(TagUrn("Important"))

client.entities.update(chart)

print(f"Added tag {TagUrn('Important')} to chart {chart.urn}")
