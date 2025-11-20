# Inlined from /metadata-ingestion/examples/library/ownership_type_query_rest.py

import json
from urllib.parse import quote

import requests

# Configuration
DATAHUB_SERVER = "http://localhost:8080"
OWNERSHIP_TYPE_URN = "urn:li:ownershipType:__system__technical_owner"

# URL encode the URN
encoded_urn = quote(OWNERSHIP_TYPE_URN, safe="")

# Construct the REST API endpoint
url = f"{DATAHUB_SERVER}/entities/{encoded_urn}"

# Make the GET request
response = requests.get(url)

if response.status_code == 200:
    entity = response.json()

    print(f"Successfully retrieved ownership type: {OWNERSHIP_TYPE_URN}")
    print("-" * 80)
    print(json.dumps(entity, indent=2))
    print("-" * 80)

    # Extract specific information
    if "aspects" in entity:
        aspects = entity["aspects"]

        # Get ownership type info
        if "ownershipTypeInfo" in aspects:
            info = aspects["ownershipTypeInfo"]["value"]
            print("\nOwnership Type Details:")
            print(f"  Name: {info.get('name')}")
            print(f"  Description: {info.get('description')}")

            if "created" in info:
                created = info["created"]
                print(f"  Created: {created.get('time')} by {created.get('actor')}")

            if "lastModified" in info:
                modified = info["lastModified"]
                print(
                    f"  Last Modified: {modified.get('time')} by {modified.get('actor')}"
                )

        # Get status
        if "status" in aspects:
            status = aspects["status"]["value"]
            is_removed = status.get("removed", False)
            print(f"\nStatus: {'Removed' if is_removed else 'Active'}")

elif response.status_code == 404:
    print(f"Ownership type not found: {OWNERSHIP_TYPE_URN}")
else:
    print(f"Error: {response.status_code}")
    print(response.text)

# Example: Query multiple ownership types
print("\n" + "=" * 80)
print("Querying multiple ownership types:")
print("=" * 80)

ownership_type_urns = [
    "urn:li:ownershipType:__system__technical_owner",
    "urn:li:ownershipType:__system__business_owner",
    "urn:li:ownershipType:__system__data_steward",
]

for urn in ownership_type_urns:
    encoded_urn = quote(urn, safe="")
    url = f"{DATAHUB_SERVER}/entities/{encoded_urn}"
    response = requests.get(url)

    if response.status_code == 200:
        entity = response.json()
        if "aspects" in entity and "ownershipTypeInfo" in entity["aspects"]:
            info = entity["aspects"]["ownershipTypeInfo"]["value"]
            print(f"\n{info.get('name')}: {info.get('description')}")
