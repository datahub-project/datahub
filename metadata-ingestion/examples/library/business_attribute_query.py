import logging
import os
from urllib.parse import quote

import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
business_attribute_urn = "urn:li:businessAttribute:customer_id"

url = f"{gms_server}/entities/{quote(business_attribute_urn, safe='')}"

headers = {}
if token:
    headers["Authorization"] = f"Bearer {token}"

response = requests.get(url, headers=headers)

if response.status_code == 200:
    entity = response.json()
    log.info(f"Business Attribute: {business_attribute_urn}")
    log.info(f"Response: {entity}")

    aspects = entity.get("aspects", {})

    if "businessAttributeInfo" in aspects:
        info = aspects["businessAttributeInfo"]["value"]
        log.info(f"Name: {info.get('name')}")
        log.info(f"Description: {info.get('description')}")
        log.info(f"Type: {info.get('type')}")

    if "ownership" in aspects:
        ownership = aspects["ownership"]["value"]
        owners = ownership.get("owners", [])
        log.info(f"Owners: {[owner['owner'] for owner in owners]}")
else:
    log.error(
        f"Failed to fetch business attribute: {response.status_code} - {response.text}"
    )
