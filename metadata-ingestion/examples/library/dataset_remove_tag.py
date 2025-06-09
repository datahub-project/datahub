from datahub.metadata.urns import TagUrn
from datahub.sdk import DataHubClient
from datahub.sdk.dataset import Dataset

client = DataHubClient.from_env()

dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
    tags=[TagUrn(name="deprecated")],
)

dataset.remove_tag("deprecated")

client.entities.upsert(dataset)
