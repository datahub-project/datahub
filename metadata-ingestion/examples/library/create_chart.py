from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, DataHubClient, Dataset

client = DataHubClient.from_env()

dataset = Dataset(
    name="example_dataset",
    platform="looker",
    description="looker dataset for production",
)

chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
    input_datasets=[dataset.urn],
)

client.entities.upsert(dataset)
client.entities.upsert(chart)
