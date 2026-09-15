"""
Example: Query an existing (deprecated) dataProcess entity for migration purposes.

This example shows how to read a deprecated dataProcess entity from DataHub
to understand its structure before migrating it to dataFlow and dataJob entities.

Note: This is only for reading existing data. Do NOT create new dataProcess entities.
Use dataFlow and dataJob instead for all new implementations.
"""

from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    DataProcessInfoClass,
    OwnershipClass,
    StatusClass,
)
from datahub.metadata.urns import DataProcessUrn

graph = get_default_graph()

dataprocess_urn = DataProcessUrn(
    name="customer_etl_job", orchestrator="airflow", env="PROD"
)

if not graph.exists(str(dataprocess_urn)):
    raise SystemExit(
        f"dataProcess not found: {dataprocess_urn}\n"
        "DataProcess is deprecated - use dataFlow and dataJob instead."
    )

print(f"Found dataProcess: {dataprocess_urn}")
print("\n=== Entity Aspects ===")

# The identity fields come from the urn itself -- no need to fetch the key aspect.
print("\nIdentity:")
print(f"  Name: {dataprocess_urn.name}")
print(f"  Orchestrator: {dataprocess_urn.orchestrator}")
print(f"  Origin (Fabric): {dataprocess_urn.env}")

info = graph.get_aspect(
    entity_urn=str(dataprocess_urn), aspect_type=DataProcessInfoClass
)
if info is not None:
    print("\nProcess Info:")
    if info.inputs is not None:
        print(f"  Input Datasets: {len(info.inputs)}")
        for inp in info.inputs:
            print(f"    - {inp}")
    if info.outputs is not None:
        print(f"  Output Datasets: {len(info.outputs)}")
        for out in info.outputs:
            print(f"    - {out}")

ownership = graph.get_aspect(
    entity_urn=str(dataprocess_urn), aspect_type=OwnershipClass
)
if ownership is not None:
    print("\nOwnership:")
    for owner in ownership.owners:
        print(f"  - {owner.owner} (type: {owner.type})")

status = graph.get_aspect(entity_urn=str(dataprocess_urn), aspect_type=StatusClass)
if status is not None:
    print(f"\nStatus: {status.removed}")

flow_urn = (
    f"urn:li:dataFlow:({dataprocess_urn.orchestrator},"
    f"{dataprocess_urn.name},{dataprocess_urn.env.lower()})"
)
print("\n=== Migration Recommendation ===")
print("Replace this dataProcess with:")
print(f"  DataFlow URN: {flow_urn}")
print(f"  DataJob URN: urn:li:dataJob:({flow_urn},main_task)")
print("\nSee dataprocess_migrate_to_flow_job.py for migration code examples.")
