"""
Example: Complete migration from dataProcess to dataFlow/dataJob with metadata preservation.

This example demonstrates a full migration path that:
1. Reads an existing deprecated dataProcess entity
2. Extracts all its metadata (inputs, outputs, ownership)
3. Creates equivalent dataFlow and dataJob entities
4. Preserves all metadata relationships

Use this as a template for migrating multiple dataProcess entities in bulk.
"""

from typing import List, Union

from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import DataProcessInfoClass, OwnershipClass
from datahub.metadata.urns import DataProcessUrn, DatasetUrn
from datahub.sdk import DataFlow, DataHubClient, DataJob

graph = get_default_graph()
client = DataHubClient(graph=graph)

# Step 1: Define the dataProcess to migrate
old_dataprocess_urn = DataProcessUrn(
    name="sales_pipeline", orchestrator="airflow", env="PROD"
)

print(f"Migrating: {old_dataprocess_urn}")

# Bail out before writing anything if the source entity isn't there. Emitting from
# defaults would create a dataFlow full of placeholder metadata.
if not graph.exists(str(old_dataprocess_urn)):
    raise SystemExit(
        f"dataProcess not found: {old_dataprocess_urn}\n"
        "Nothing to migrate (already migrated, or never created)."
    )

# Step 2: Read the metadata off the existing dataProcess. Identity comes from the
# urn itself; dataProcess has no globalTags aspect, so there are no tags to migrate.
info = graph.get_aspect(
    entity_urn=str(old_dataprocess_urn), aspect_type=DataProcessInfoClass
)
ownership = graph.get_aspect(
    entity_urn=str(old_dataprocess_urn), aspect_type=OwnershipClass
)

input_datasets: List[Union[str, DatasetUrn]] = (
    [DatasetUrn.from_string(urn) for urn in (info.inputs or [])] if info else []
)
output_datasets: List[Union[str, DatasetUrn]] = (
    [DatasetUrn.from_string(urn) for urn in (info.outputs or [])] if info else []
)
owners = ownership.owners if ownership else []

print("\n=== Extracted Metadata ===")
print(f"Name: {old_dataprocess_urn.name}")
print(f"Orchestrator: {old_dataprocess_urn.orchestrator}")
print(f"Environment: {old_dataprocess_urn.env}")
print(f"Inputs: {len(input_datasets)} datasets")
print(f"Outputs: {len(output_datasets)} datasets")
print(f"Owners: {len(owners)}")

# Step 3: Create the new DataFlow, carrying ownership straight over.
dataflow = DataFlow(
    platform=old_dataprocess_urn.orchestrator,
    name=old_dataprocess_urn.name,
    platform_instance=old_dataprocess_urn.env.lower(),
    description=f"Migrated from dataProcess {old_dataprocess_urn.name}",
    owners=owners,
)

# Step 4: Create the DataJob(s)
# For simplicity, creating one job. In practice, you might split into multiple jobs.
datajob = DataJob(
    name=f"{old_dataprocess_urn.name}_main",
    flow=dataflow,
    description=f"Main task for {old_dataprocess_urn.name}",
    inlets=input_datasets,
    outlets=output_datasets,
)

# Step 5: Upsert the entities
client.entities.upsert(dataflow)
client.entities.upsert(datajob)

print("\n=== Created New Entities ===")
print(f"DataFlow: {dataflow.urn}")
print(f"DataJob: {datajob.urn}")
print(f"Migrated {len(owners)} owner(s) to DataFlow")

print("\n=== Migration Complete ===")
print("Next steps:")
print("1. Verify the new entities in DataHub UI")
print("2. Update any downstream systems to reference the new URNs")
print("3. Consider soft-deleting the old dataProcess entity")
