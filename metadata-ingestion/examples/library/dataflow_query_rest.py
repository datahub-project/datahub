# metadata-ingestion/examples/library/dataflow_query_rest.py
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    EditableDataFlowPropertiesClass,
    GlobalTagsClass,
    OwnershipClass,
)
from datahub.metadata.urns import DataFlowUrn

graph = get_default_graph()

flow_urn = DataFlowUrn(
    orchestrator="airflow", flow_id="example_dataflow", cluster="PROD"
)

print("DataFlow Entity:", flow_urn)

info = graph.get_aspect(entity_urn=str(flow_urn), aspect_type=DataFlowInfoClass)
if info is None:
    raise SystemExit(f"DataFlow not found: {flow_urn}")

# Description lives in two places: dataFlowInfo is what ingestion writes, and the
# editable overlay is what UI and SDK edits write. Show both rather than reimplement
# the precedence -- DataFlow.description already resolves it (see dataflow_read.py).
editable = graph.get_aspect(
    entity_urn=str(flow_urn), aspect_type=EditableDataFlowPropertiesClass
)

print(f"\nFlow Name: {info.name}")
print(f"Description (dataFlowInfo): {info.description}")
if editable is not None:
    print(f"Description (editable overlay): {editable.description}")
print(f"Project: {info.project}")

ownership = graph.get_aspect(entity_urn=str(flow_urn), aspect_type=OwnershipClass)
if ownership is not None:
    print(f"\nOwners: {len(ownership.owners)}")
    for owner in ownership.owners:
        print(f"  - {owner.owner} ({owner.type})")

tags = graph.get_aspect(entity_urn=str(flow_urn), aspect_type=GlobalTagsClass)
if tags is not None:
    print(f"\nTags: {len(tags.tags)}")
    for tag in tags.tags:
        print(f"  - {tag.tag}")
