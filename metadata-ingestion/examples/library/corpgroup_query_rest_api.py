# metadata-ingestion/examples/library/corpgroup_query_rest_api.py
import json
import os
from urllib.parse import quote

import requests

GMS_SERVER = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
group_name = "data-engineering"
group_urn = f"urn:li:corpGroup:{group_name}"

headers = {}
if token:
    headers["Authorization"] = f"Bearer {token}"

encoded_urn = quote(group_urn, safe="")
response = requests.get(f"{GMS_SERVER}/entities/{encoded_urn}", headers=headers)

if response.status_code == 200:
    entity_data = response.json()
    print("Group Entity:")
    print(json.dumps(entity_data, indent=2))

    if "aspects" in entity_data:
        aspects = entity_data["aspects"]

        if "corpGroupInfo" in aspects:
            group_info = aspects["corpGroupInfo"]["value"]
            print(f"\nDisplay Name: {group_info.get('displayName')}")
            print(f"Description: {group_info.get('description')}")
            print(f"Email: {group_info.get('email')}")

        if "corpGroupEditableInfo" in aspects:
            editable_info = aspects["corpGroupEditableInfo"]["value"]
            print(f"\nEditable Description: {editable_info.get('description')}")
            print(f"Picture Link: {editable_info.get('pictureLink')}")
else:
    print(f"Failed to fetch group: {response.status_code}")

encoded_urn = quote(group_urn, safe="")
response = requests.get(
    f"{GMS_SERVER}/relationships",
    params={
        "direction": "INCOMING",
        "urn": group_urn,
        "types": "IsMemberOfGroup",
    },
    headers=headers,
)

if response.status_code == 200:
    relationships = response.json()
    print(f"\nGroup Members ({relationships.get('total', 0)} total):")

    for relationship in relationships.get("relationships", []):
        member_urn = relationship.get("entity")
        print(f"  - {member_urn}")
else:
    print(f"Failed to fetch group members: {response.status_code}")
