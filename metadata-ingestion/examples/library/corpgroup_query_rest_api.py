# metadata-ingestion/examples/library/corpgroup_query_rest_api.py
from datahub.ingestion.graph.client import RelationshipDirection, get_default_graph
from datahub.metadata.schema_classes import (
    CorpGroupEditableInfoClass,
    CorpGroupInfoClass,
)
from datahub.metadata.urns import CorpGroupUrn

graph = get_default_graph()

group_urn = CorpGroupUrn("data-engineering")

group_info = graph.get_aspect(entity_urn=str(group_urn), aspect_type=CorpGroupInfoClass)
if group_info is None:
    raise SystemExit(f"Group not found: {group_urn}")

print("Group Entity:")
print(f"Display Name: {group_info.displayName}")
print(f"Description: {group_info.description}")
print(f"Email: {group_info.email}")

editable_info = graph.get_aspect(
    entity_urn=str(group_urn), aspect_type=CorpGroupEditableInfoClass
)
if editable_info is not None:
    print(f"\nEditable Description: {editable_info.description}")
    print(f"Picture Link: {editable_info.pictureLink}")

members = list(
    graph.get_related_entities(
        entity_urn=str(group_urn),
        relationship_types=["IsMemberOfGroup"],
        direction=RelationshipDirection.INCOMING,
    )
)
print(f"\nGroup Members ({len(members)} total):")
for member in members:
    print(f"  - {member.urn}")
