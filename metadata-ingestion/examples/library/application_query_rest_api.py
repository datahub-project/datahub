# metadata-ingestion/examples/library/application_query_rest_api.py
import json
import os
from urllib.parse import quote

import requests


def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
application_urn = make_application_urn("customer-analytics-service")

encoded_urn = quote(application_urn, safe="")

headers = {}
if token:
    headers["Authorization"] = f"Bearer {token}"

response = requests.get(f"{gms_server}/entities/{encoded_urn}", headers=headers)

if response.status_code == 200:
    entity_data = response.json()
    print(f"Application: {application_urn}")
    print(json.dumps(entity_data, indent=2))

    if "aspects" in entity_data and "applicationProperties" in entity_data["aspects"]:
        props = entity_data["aspects"]["applicationProperties"]["value"]
        print(f"\nApplication Name: {props.get('name')}")
        print(f"Description: {props.get('description')}")
        if "customProperties" in props:
            print(
                f"Custom Properties: {json.dumps(props['customProperties'], indent=2)}"
            )
else:
    print(f"Failed to fetch application: {response.status_code} - {response.text}")
