from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient
from datahub.sdk.dataset import Dataset

client = DataHubClient.from_env()

dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
    owners=[CorpUserUrn(username="jdoe")],
)

dataset.remove_owner(CorpUserUrn(username="jdoe"))

client.entities.upsert(dataset)
