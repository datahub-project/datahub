from datahub.metadata.urns import GlossaryTermUrn
from datahub.sdk import ChartUrn, DataHubClient

client = DataHubClient.from_env()

chart = client.entities.get(ChartUrn("looker", "sales_dashboard"))

chart.add_term(GlossaryTermUrn("Revenue"))

client.entities.update(chart)

print(f"Added term {GlossaryTermUrn('Revenue')} to chart {chart.urn}")
