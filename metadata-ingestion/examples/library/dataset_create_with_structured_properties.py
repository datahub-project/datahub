from datahub.sdk import DataHubClient, Dataset

client = DataHubClient.from_env()

# Use the structured property created by structured_property_create_basic.py
retention_property_urn = "urn:li:structuredProperty:io.acryl.privacy.retentionTime"

dataset = Dataset(
    name="example_dataset",
    platform="snowflake",
    description="airflow pipeline for production",
    structured_properties={retention_property_urn: [90.0]},
)

# update the structured property
dataset.set_structured_property(retention_property_urn, [365.0])

client.entities.upsert(dataset)
print(f"Created dataset: {dataset.urn}")
