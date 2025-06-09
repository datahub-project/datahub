from datahub.sdk import DataHubClient
from datahub.sdk.dataset import Dataset
from datahub.metadata.urns import DomainUrn

client = DataHubClient.from_env()

dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
)

dataset.set_domain(DomainUrn(name="marketing"))

client.entities.upsert(dataset)