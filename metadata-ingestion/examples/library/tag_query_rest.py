# metadata-ingestion/examples/library/tag_query_rest.py
import logging
from urllib.parse import quote

import requests

from datahub.emitter.mce_builder import make_tag_urn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
gms_server = "http://localhost:8080"
tag_urn = make_tag_urn("pii")

# Fetch tag entity
response = requests.get(f"{gms_server}/entities/{quote(tag_urn, safe='')}")

if response.status_code == 200:
    tag_data = response.json()
    log.info(f"Successfully retrieved tag: {tag_urn}")

    # Extract tag properties
    if "aspects" in tag_data and "tagProperties" in tag_data["aspects"]:
        properties = tag_data["aspects"]["tagProperties"]["value"]
        log.info(f"Tag name: {properties.get('name')}")
        log.info(f"Description: {properties.get('description')}")
        log.info(f"Color: {properties.get('colorHex')}")

    # Extract ownership if present
    if "aspects" in tag_data and "ownership" in tag_data["aspects"]:
        ownership = tag_data["aspects"]["ownership"]["value"]
        log.info(f"Number of owners: {len(ownership.get('owners', []))}")
        for owner in ownership.get("owners", []):
            log.info(f"  - Owner: {owner['owner']}, Type: {owner['type']}")
else:
    log.error(f"Failed to retrieve tag: {response.status_code} - {response.text}")

# Query relationships to find all entities tagged with this tag
relationships_url = (
    f"{gms_server}/relationships"
    f"?direction=INCOMING"
    f"&urn={quote(tag_urn, safe='')}"
    f"&types=TaggedWith"
)

response = requests.get(relationships_url)

if response.status_code == 200:
    relationships = response.json()
    total = relationships.get("total", 0)
    log.info(f"Found {total} entities tagged with this tag")

    for rel in relationships.get("relationships", []):
        log.info(f"  - {rel['entity']} (type: {rel['type']})")
else:
    log.error(
        f"Failed to retrieve relationships: {response.status_code} - {response.text}"
    )
