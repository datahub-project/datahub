from datahub.metadata.urns import GlossaryTermUrn
from datahub.sdk import DataHubClient
from datahub.sdk.dataset import Dataset

client = DataHubClient.from_env()

dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
    terms=[GlossaryTermUrn(name="rateofreturn")],
)

dataset.remove_term("rateofreturn")

client.entities.upsert(dataset)
