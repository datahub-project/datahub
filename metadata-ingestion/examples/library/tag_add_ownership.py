# metadata-ingestion/examples/library/tag_add_ownership.py
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient, Tag

client = DataHubClient.from_env()

# Create a tag with ownership
tag = Tag(
    name="data_quality",
    owners=[
        CorpUserUrn("data_steward"),
    ],
)

# Upsert the tag
client.entities.upsert(tag)

print(f"Created tag with ownership: {tag.urn}")
