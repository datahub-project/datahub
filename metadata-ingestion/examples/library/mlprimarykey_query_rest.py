from datahub.ingestion.graph.client import RelationshipDirection, get_default_graph
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    GlossaryTermsClass,
    MLPrimaryKeyPropertiesClass,
    OwnershipClass,
)
from datahub.metadata.urns import MlPrimaryKeyUrn

graph = get_default_graph()

primary_key_urn = MlPrimaryKeyUrn("users_feature_table", "user_id")

print("MLPrimaryKey Entity:", primary_key_urn)

properties = graph.get_aspect(
    entity_urn=str(primary_key_urn), aspect_type=MLPrimaryKeyPropertiesClass
)
if properties is None:
    raise SystemExit(f"MLPrimaryKey not found: {primary_key_urn}")

print("\nPrimary Key Properties:")
print(f"  Description: {properties.description}")
print(f"  Data Type: {properties.dataType}")
print(f"  Sources: {properties.sources}")

ownership = graph.get_aspect(
    entity_urn=str(primary_key_urn), aspect_type=OwnershipClass
)
if ownership is not None:
    print("\nOwnership:")
    for owner in ownership.owners:
        print(f"  - {owner.owner} ({owner.type})")

tags = graph.get_aspect(entity_urn=str(primary_key_urn), aspect_type=GlobalTagsClass)
if tags is not None:
    print("\nTags:")
    for tag in tags.tags:
        print(f"  - {tag.tag}")

terms = graph.get_aspect(
    entity_urn=str(primary_key_urn), aspect_type=GlossaryTermsClass
)
if terms is not None:
    print("\nGlossary Terms:")
    for term in terms.terms:
        print(f"  - {term.urn}")

# Feature tables that use this primary key.
print("\n\nFeature Tables using this Primary Key:")
for related in graph.get_related_entities(
    entity_urn=str(primary_key_urn),
    relationship_types=["KeyedBy"],
    direction=RelationshipDirection.INCOMING,
):
    print(f"  - {related.urn}")

# Upstream datasets this primary key is derived from.
print("\nUpstream Datasets (Sources):")
for related in graph.get_related_entities(
    entity_urn=str(primary_key_urn),
    relationship_types=["DerivedFrom"],
    direction=RelationshipDirection.OUTGOING,
):
    print(f"  - {related.urn}")
