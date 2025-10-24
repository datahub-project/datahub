import json
import urllib.parse

import requests

# Configuration
gms_server = "http://localhost:8080"
primary_key_urn = "urn:li:mlPrimaryKey:(users_feature_table,user_id)"

# Encode the URN for use in URL
encoded_urn = urllib.parse.quote(primary_key_urn, safe="")

# Fetch the MLPrimaryKey entity
response = requests.get(f"{gms_server}/entities/{encoded_urn}")

if response.status_code == 200:
    entity_data = response.json()
    print("MLPrimaryKey Entity:")
    print(json.dumps(entity_data, indent=2))

    # Extract specific aspects
    if "aspects" in entity_data:
        aspects = entity_data["aspects"]

        # Get mlPrimaryKeyProperties
        if "mlPrimaryKeyProperties" in aspects:
            properties = aspects["mlPrimaryKeyProperties"]["value"]
            print("\nPrimary Key Properties:")
            print(f"  Description: {properties.get('description', 'N/A')}")
            print(f"  Data Type: {properties.get('dataType', 'N/A')}")
            if "sources" in properties:
                print(f"  Sources: {properties['sources']}")

        # Get ownership
        if "ownership" in aspects:
            ownership = aspects["ownership"]["value"]
            print("\nOwnership:")
            for owner in ownership.get("owners", []):
                print(f"  - {owner['owner']} ({owner['type']})")

        # Get tags
        if "globalTags" in aspects:
            tags = aspects["globalTags"]["value"]
            print("\nTags:")
            for tag in tags.get("tags", []):
                print(f"  - {tag['tag']}")

        # Get glossary terms
        if "glossaryTerms" in aspects:
            terms = aspects["glossaryTerms"]["value"]
            print("\nGlossary Terms:")
            for term in terms.get("terms", []):
                print(f"  - {term['urn']}")
else:
    print(f"Failed to fetch entity. Status code: {response.status_code}")
    print(f"Response: {response.text}")

# Find feature tables that use this primary key
# Query for entities with a KeyedBy relationship to this primary key
relationships_response = requests.get(
    f"{gms_server}/relationships",
    params={
        "direction": "INCOMING",
        "urn": primary_key_urn,
        "types": "KeyedBy",
    },
)

if relationships_response.status_code == 200:
    relationships_data = relationships_response.json()
    print("\n\nFeature Tables using this Primary Key:")
    for relationship in relationships_data.get("relationships", []):
        print(f"  - {relationship['entity']}")
else:
    print(
        f"\nFailed to fetch relationships. Status code: {relationships_response.status_code}"
    )

# Find upstream datasets that this primary key is derived from
upstream_response = requests.get(
    f"{gms_server}/relationships",
    params={
        "direction": "OUTGOING",
        "urn": primary_key_urn,
        "types": "DerivedFrom",
    },
)

if upstream_response.status_code == 200:
    upstream_data = upstream_response.json()
    print("\nUpstream Datasets (Sources):")
    for relationship in upstream_data.get("relationships", []):
        print(f"  - {relationship['entity']}")
else:
    print(
        f"\nFailed to fetch upstream lineage. Status code: {upstream_response.status_code}"
    )
