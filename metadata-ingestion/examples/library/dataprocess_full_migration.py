"""
Example: Complete migration from dataProcess to dataFlow/dataJob with metadata preservation.

This example demonstrates a full migration path that:
1. Reads an existing deprecated dataProcess entity
2. Extracts all its metadata (inputs, outputs, ownership, tags)
3. Creates equivalent dataFlow and dataJob entities
4. Preserves all metadata relationships

Use this as a template for migrating multiple dataProcess entities in bulk.
"""

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from datahub.sdk import DataFlow, DataHubClient, DataJob

# Initialize clients
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
client = DataHubClient.from_env()

# Step 1: Define the dataProcess to migrate
old_dataprocess_urn = "urn:li:dataProcess:(sales_pipeline,airflow,PROD)"

print(f"Migrating: {old_dataprocess_urn}")

try:
    # Step 2: Fetch the existing dataProcess entity
    entity = rest_emitter._session.get(
        f"{rest_emitter._gms_server}/entities/{old_dataprocess_urn}"
    ).json()

    aspects = entity.get("aspects", {})

    # Extract identity information
    key = aspects.get("dataProcessKey", {})
    name = key.get("name", "unknown_process")
    orchestrator = key.get("orchestrator", "unknown")
    origin = key.get("origin", "PROD")

    # Extract process info
    process_info = aspects.get("dataProcessInfo", {})
    input_datasets = process_info.get("inputs", [])
    output_datasets = process_info.get("outputs", [])

    # Extract ownership
    ownership_aspect = aspects.get("ownership", {})
    owners = ownership_aspect.get("owners", [])

    # Extract tags
    tags_aspect = aspects.get("globalTags", {})
    tags = tags_aspect.get("tags", [])

    print("\n=== Extracted Metadata ===")
    print(f"Name: {name}")
    print(f"Orchestrator: {orchestrator}")
    print(f"Environment: {origin}")
    print(f"Inputs: {len(input_datasets)} datasets")
    print(f"Outputs: {len(output_datasets)} datasets")
    print(f"Owners: {len(owners)}")
    print(f"Tags: {len(tags)}")

    # Step 3: Create the new DataFlow
    dataflow = DataFlow(
        platform=orchestrator,
        name=name,
        platform_instance=origin.lower(),
        description=f"Migrated from dataProcess {name}",
    )

    # Step 4: Create the DataJob(s)
    # For simplicity, creating one job. In practice, you might split into multiple jobs.
    datajob = DataJob(
        name=f"{name}_main",
        flow=dataflow,
        description=f"Main task for {name}",
        inlets=[inp for inp in input_datasets],  # These should be dataset URNs
        outlets=[out for out in output_datasets],  # These should be dataset URNs
    )

    # Step 5: Upsert the entities
    client.entities.upsert(dataflow)
    client.entities.upsert(datajob)

    print("\n=== Created New Entities ===")
    print(f"DataFlow: {dataflow.urn}")
    print(f"DataJob: {datajob.urn}")

    # Step 6: Migrate ownership to DataFlow
    if owners:
        ownership_to_add = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=owner.get("owner"),
                    type=getattr(OwnershipTypeClass, owner.get("type", "DATAOWNER")),
                )
                for owner in owners
            ]
        )
        rest_emitter.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=str(dataflow.urn),
                aspect=ownership_to_add,
            )
        )
        print(f"Migrated {len(owners)} owner(s) to DataFlow")

    # Step 7: Migrate tags to DataFlow
    if tags:
        tags_to_add = GlobalTagsClass(
            tags=[TagAssociationClass(tag=tag.get("tag")) for tag in tags]
        )
        rest_emitter.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=str(dataflow.urn),
                aspect=tags_to_add,
            )
        )
        print(f"Migrated {len(tags)} tag(s) to DataFlow")

    print("\n=== Migration Complete ===")
    print("Next steps:")
    print("1. Verify the new entities in DataHub UI")
    print("2. Update any downstream systems to reference the new URNs")
    print("3. Consider soft-deleting the old dataProcess entity")

except Exception as e:
    print(f"Error during migration: {e}")
    print("\nCommon issues:")
    print("- DataProcess entity doesn't exist (already migrated or never created)")
    print("- Network connectivity to DataHub GMS")
    print("- Permission issues writing to DataHub")
