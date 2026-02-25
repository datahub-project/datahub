from datahub.sdk import DataHubClient
from datahub.sdk.dataset import Dataset

client = DataHubClient.from_env()

# NOTE: structured porperties should be created before adding them to the dataset
# client.entities.get() also works to retrieve the dataset
dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
    structured_properties={
        "urn:li:structuredProperty:sp1": ["PROD"],
        "urn:li:structuredProperty:sp2": [100.0],
    },
)

dataset.remove_structured_property("urn:li:structuredProperty:sp1")

client.entities.upsert(dataset)
