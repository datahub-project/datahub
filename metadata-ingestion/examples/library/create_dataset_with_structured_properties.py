from datahub.sdk import DataHubClient, Dataset

client = DataHubClient.from_env()

properties_urn1 = "urn:li:structuredProperty:sp1"
properties_urn2 = "urn:li:structuredProperty:sp2"


dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
    structured_properties={properties_urn1: ["PROD"], properties_urn2: [100.0]},
)

# update the structured properties
dataset.set_structured_property(properties_urn1, ["PROD", "DEV"])

client.entities.upsert(dataset)
