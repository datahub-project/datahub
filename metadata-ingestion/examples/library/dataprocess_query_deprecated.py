"""
Example: Query an existing (deprecated) dataProcess entity for migration purposes.

This example shows how to read a deprecated dataProcess entity from DataHub
to understand its structure before migrating it to dataFlow and dataJob entities.

Note: This is only for reading existing data. Do NOT create new dataProcess entities.
Use dataFlow and dataJob instead for all new implementations.
"""

from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create emitter to read from DataHub
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# URN of the deprecated dataProcess entity to query
dataprocess_urn = "urn:li:dataProcess:(customer_etl_job,airflow,PROD)"

# Fetch the entity using the REST API
try:
    entity = emitter._session.get(
        f"{emitter._gms_server}/entities/{dataprocess_urn}"
    ).json()

    print(f"Found dataProcess: {dataprocess_urn}")
    print("\n=== Entity Aspects ===")

    # Extract key information for migration
    if "aspects" in entity:
        aspects = entity["aspects"]

        # Key aspect (identity)
        if "dataProcessKey" in aspects:
            key = aspects["dataProcessKey"]
            print("\nIdentity:")
            print(f"  Name: {key.get('name')}")
            print(f"  Orchestrator: {key.get('orchestrator')}")
            print(f"  Origin (Fabric): {key.get('origin')}")

        # Core process information
        if "dataProcessInfo" in aspects:
            info = aspects["dataProcessInfo"]
            print("\nProcess Info:")
            if "inputs" in info:
                print(f"  Input Datasets: {len(info['inputs'])}")
                for inp in info["inputs"]:
                    print(f"    - {inp}")
            if "outputs" in info:
                print(f"  Output Datasets: {len(info['outputs'])}")
                for out in info["outputs"]:
                    print(f"    - {out}")

        # Ownership information
        if "ownership" in aspects:
            ownership = aspects["ownership"]
            print("\nOwnership:")
            for owner in ownership.get("owners", []):
                print(f"  - {owner['owner']} (type: {owner.get('type', 'UNKNOWN')})")

        # Tags
        if "globalTags" in aspects:
            tags = aspects["globalTags"]
            print("\nTags:")
            for tag in tags.get("tags", []):
                print(f"  - {tag['tag']}")

        # Status
        if "status" in aspects:
            status = aspects["status"]
            print(f"\nStatus: {status.get('removed', False)}")

    print("\n=== Migration Recommendation ===")
    print("Replace this dataProcess with:")
    print(
        f"  DataFlow URN: urn:li:dataFlow:({key.get('orchestrator')},{key.get('name')},{key.get('origin', 'PROD').lower()})"
    )
    print(
        f"  DataJob URN: urn:li:dataJob:(urn:li:dataFlow:({key.get('orchestrator')},{key.get('name')},{key.get('origin', 'PROD').lower()}),main_task)"
    )
    print("\nSee dataprocess_migrate_to_flow_job.py for migration code examples.")

except Exception as e:
    print(f"Error querying dataProcess: {e}")
    print("\nThis is expected if the entity doesn't exist.")
    print("DataProcess is deprecated - use dataFlow and dataJob instead.")
