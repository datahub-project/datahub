from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient, Tag

client = DataHubClient.from_env()

# Create a basic tag
basic_tag = Tag(name="deprecated")

# Create a more detailed tag with all properties
detailed_tag = Tag(
    name="data-quality",
    display_name="Data Quality",
    description="Tag used to mark datasets with quality issues or requirements",
    color="#FF5733",
    owners=[
        CorpUserUrn("data-team@company.com"),
    ],
)

# Upsert the tags
client.entities.upsert(basic_tag)
client.entities.upsert(detailed_tag)

print(f"Created basic tag: {basic_tag.urn}")
print(f"Created detailed tag: {detailed_tag.urn}")
