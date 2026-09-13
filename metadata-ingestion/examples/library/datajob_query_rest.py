# metadata-ingestion/examples/library/datajob_query_rest.py
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    GlobalTagsClass,
    OwnershipClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn

graph = get_default_graph()

datajob_urn = DataJobUrn(
    flow=DataFlowUrn(
        orchestrator="airflow", flow_id="daily_etl_pipeline", cluster="prod"
    ),
    job_id="transform_customer_data",
)

job_info = graph.get_aspect(entity_urn=str(datajob_urn), aspect_type=DataJobInfoClass)
if job_info is None:
    raise SystemExit(f"DataJob not found: {datajob_urn}")

print(f"Job Name: {job_info.name}")
print(f"Description: {job_info.description}")

lineage = graph.get_aspect(
    entity_urn=str(datajob_urn), aspect_type=DataJobInputOutputClass
)
if lineage is not None:
    print(f"\nInput Datasets: {len(lineage.inputDatasetEdges or [])}")
    print(f"Output Datasets: {len(lineage.outputDatasetEdges or [])}")

ownership = graph.get_aspect(entity_urn=str(datajob_urn), aspect_type=OwnershipClass)
if ownership is not None:
    print(f"\nOwners: {len(ownership.owners)}")
    for owner in ownership.owners:
        print(f"  - {owner.owner} ({owner.type})")

tags = graph.get_aspect(entity_urn=str(datajob_urn), aspect_type=GlobalTagsClass)
if tags is not None:
    print("\nTags:")
    for tag in tags.tags:
        print(f"  - {tag.tag}")
