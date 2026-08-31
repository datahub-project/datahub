# metadata-ingestion/examples/library/tag_apply_to_dataset.py
from datahub.sdk import DataHubClient, Dataset, Tag

client = DataHubClient.from_env()

# Create Dataset entity
dataset = Dataset(platform="snowflake", name="db.schema.customers", env="PROD")

# Create Tag entity
tag = Tag(name="pii")

# Apply tag to dataset
dataset.add_tag(tag.urn)

# Update the dataset with the new tag
client.entities.upsert(dataset)

print(f"Applied tag {tag.urn} to dataset {dataset.urn}")
