from datahub.sdk import ChartUrn, CorpUserUrn, DataHubClient

client = DataHubClient.from_env()

chart = client.entities.get(ChartUrn("looker", "sales_dashboard"))

chart.add_owner(CorpUserUrn("jdoe"))

client.entities.update(chart)

print(f"Added owner {CorpUserUrn('jdoe')} to chart {chart.urn}")
