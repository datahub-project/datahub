# metadata-ingestion/examples/library/dataflow_query_rest.py
import json
import urllib.parse

import requests

# DataHub REST API endpoint
DATAHUB_GMS_URL = "http://localhost:8080"

# Create the URN for the DataFlow
flow_urn = "urn:li:dataFlow:(airflow,daily_sales_pipeline,prod)"

# URL encode the URN

encoded_urn = urllib.parse.quote(flow_urn, safe="")

# Fetch the entity
response = requests.get(f"{DATAHUB_GMS_URL}/entities/{encoded_urn}")

if response.status_code == 200:
    entity_data = response.json()
    print("DataFlow Entity:")
    print(json.dumps(entity_data, indent=2))

    # Extract specific aspects
    aspects = entity_data.get("aspects", {})

    if "dataFlowInfo" in aspects:
        info = aspects["dataFlowInfo"]
        print(f"\nFlow Name: {info.get('name')}")
        print(f"Description: {info.get('description')}")
        print(f"Project: {info.get('project')}")

    if "ownership" in aspects:
        ownership = aspects["ownership"]
        print(f"\nOwners: {len(ownership.get('owners', []))}")
        for owner in ownership.get("owners", []):
            print(f"  - {owner.get('owner')} ({owner.get('type')})")

    if "globalTags" in aspects:
        tags = aspects["globalTags"]
        print(f"\nTags: {len(tags.get('tags', []))}")
        for tag in tags.get("tags", []):
            print(f"  - {tag.get('tag')}")
else:
    print(f"Failed to fetch entity: {response.status_code}")
    print(response.text)
