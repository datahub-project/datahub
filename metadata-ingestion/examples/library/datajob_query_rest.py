# metadata-ingestion/examples/library/datajob_query_rest.py
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    EditableDataJobPropertiesClass,
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

# The SDK writes description to the editable overlay, while ingestion sources write it
# to dataJobInfo. Prefer the overlay and fall back, which is what DataJob.description does.
editable = graph.get_aspect(
    entity_urn=str(datajob_urn), aspect_type=EditableDataJobPropertiesClass
)
description = (editable.description if editable else None) or job_info.description

print(f"Job Name: {job_info.name}")
print(f"Description: {description}")

lineage = graph.get_aspect(
    entity_urn=str(datajob_urn), aspect_type=DataJobInputOutputClass
)
if lineage is not None:
    # Lineage comes back either as plain urn lists or as edges carrying audit stamps,
    # depending on which writer produced it -- the SDK writes the former. Read both.
    inputs = [*(lineage.inputDatasets or [])] + [
        edge.destinationUrn for edge in (lineage.inputDatasetEdges or [])
    ]
    outputs = [*(lineage.outputDatasets or [])] + [
        edge.destinationUrn for edge in (lineage.outputDatasetEdges or [])
    ]

    print(f"\nInput Datasets: {len(inputs)}")
    for dataset_urn in inputs:
        print(f"  - {dataset_urn}")
    print(f"Output Datasets: {len(outputs)}")
    for dataset_urn in outputs:
        print(f"  - {dataset_urn}")

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
