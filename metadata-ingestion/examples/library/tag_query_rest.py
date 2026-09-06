# metadata-ingestion/examples/library/tag_query_rest.py

from datahub.ingestion.graph.client import RelationshipDirection, get_default_graph
from datahub.metadata.schema_classes import OwnershipClass, TagPropertiesClass
from datahub.metadata.urns import TagUrn

graph = get_default_graph()

tag_urn = TagUrn("pii")

properties = graph.get_aspect(entity_urn=str(tag_urn), aspect_type=TagPropertiesClass)
if properties is None:
    raise SystemExit(f"Tag not found: {tag_urn}")

print(f"Tag name: {properties.name}")
print(f"Description: {properties.description}")
print(f"Color: {properties.colorHex}")

ownership = graph.get_aspect(entity_urn=str(tag_urn), aspect_type=OwnershipClass)
if ownership is not None:
    print(f"Number of owners: {len(ownership.owners)}")
    for owner in ownership.owners:
        print(f"  - Owner: {owner.owner}, Type: {owner.type}")

# Find all entities tagged with this tag by walking the TaggedWith relationship.
tagged = list(
    graph.get_related_entities(
        entity_urn=str(tag_urn),
        relationship_types=["TaggedWith"],
        direction=RelationshipDirection.INCOMING,
    )
)
print(f"Found {len(tagged)} entities tagged with this tag")
for related in tagged:
    print(f"  - {related.urn} (type: {related.relationship_type})")
