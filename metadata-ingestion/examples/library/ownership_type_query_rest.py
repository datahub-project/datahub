from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import OwnershipTypeInfoClass, StatusClass
from datahub.metadata.urns import OwnershipTypeUrn

graph = get_default_graph()

# A built-in ownership type, so this example runs standalone with no prerequisites.
ownership_type_urn = OwnershipTypeUrn("__system__technical_owner")

info = graph.get_aspect(
    entity_urn=str(ownership_type_urn), aspect_type=OwnershipTypeInfoClass
)
if info is None:
    raise SystemExit(f"Ownership type not found: {ownership_type_urn}")

print(f"Successfully retrieved ownership type: {ownership_type_urn}")
print("-" * 80)
print("Ownership Type Details:")
print(f"  Name: {info.name}")
print(f"  Description: {info.description}")
if info.created is not None:
    print(f"  Created: {info.created.time} by {info.created.actor}")
if info.lastModified is not None:
    print(f"  Last Modified: {info.lastModified.time} by {info.lastModified.actor}")

status = graph.get_aspect(entity_urn=str(ownership_type_urn), aspect_type=StatusClass)
if status is not None:
    print(f"\nStatus: {'Removed' if status.removed else 'Active'}")

# Example: Query multiple ownership types
print("\n" + "=" * 80)
print("Querying multiple ownership types:")
print("=" * 80)

ownership_type_urns = [
    OwnershipTypeUrn("__system__business_owner"),
    OwnershipTypeUrn("__system__data_steward"),
]

for urn in ownership_type_urns:
    info = graph.get_aspect(entity_urn=str(urn), aspect_type=OwnershipTypeInfoClass)
    if info is not None:
        print(f"\n{info.name}: {info.description}")
