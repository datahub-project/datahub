from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, DataHubClient

client = DataHubClient.from_env()

chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
)

client.entities.upsert(chart)

chart_entity = client.entities.get(chart.urn)

print("Chart name:", chart_entity.name)
print("Chart platform:", chart_entity.platform)
print("Chart description:", chart_entity.description)
