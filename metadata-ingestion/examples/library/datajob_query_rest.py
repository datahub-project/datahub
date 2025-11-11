# metadata-ingestion/examples/library/datajob_query_rest.py
import json
from urllib.parse import quote

import requests

datajob_urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,daily_etl_pipeline,prod),transform_customer_data)"

gms_server = "http://localhost:8080"
url = f"{gms_server}/entities/{quote(datajob_urn, safe='')}"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=2))

    if "aspects" in data:
        aspects = data["aspects"]

        if "dataJobInfo" in aspects:
            job_info = aspects["dataJobInfo"]["value"]
            print(f"\nJob Name: {job_info.get('name')}")
            print(f"Description: {job_info.get('description')}")
            print(f"Type: {job_info.get('type')}")

        if "dataJobInputOutput" in aspects:
            lineage = aspects["dataJobInputOutput"]["value"]
            print(f"\nInput Datasets: {len(lineage.get('inputDatasetEdges', []))}")
            print(f"Output Datasets: {len(lineage.get('outputDatasetEdges', []))}")

        if "ownership" in aspects:
            ownership = aspects["ownership"]["value"]
            print(f"\nOwners: {len(ownership.get('owners', []))}")
            for owner in ownership.get("owners", []):
                print(f"  - {owner.get('owner')} ({owner.get('type')})")

        if "globalTags" in aspects:
            tags = aspects["globalTags"]["value"]
            print("\nTags:")
            for tag in tags.get("tags", []):
                print(f"  - {tag.get('tag')}")
else:
    print(f"Failed to retrieve data job: {response.status_code}")
    print(response.text)
