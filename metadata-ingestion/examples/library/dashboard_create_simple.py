from datahub.metadata.urns import TagUrn
from datahub.sdk import Dashboard, DataHubClient

client = DataHubClient.from_env()

dashboard = Dashboard(
    name="example_dashboard",
    platform="looker",
    description="looker dashboard for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
)

client.entities.upsert(dashboard)
