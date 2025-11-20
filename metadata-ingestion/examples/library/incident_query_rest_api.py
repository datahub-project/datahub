# metadata-ingestion/examples/library/incident_query_rest_api.py
import logging
import os

import requests

import datahub.metadata.schema_classes as models
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata._urns.urn_defs import IncidentUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
gms_endpoint = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))

# Specify the incident to query (use the incident ID from incident_create.py)
incident_id = "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d"
incident_urn = IncidentUrn(incident_id)

# Query the incident info aspect
incident_info = graph.get_aspect(
    entity_urn=str(incident_urn),
    aspect_type=models.IncidentInfoClass,
)

if incident_info:
    log.info(f"Incident: {incident_urn}")
    log.info(f"  Type: {incident_info.type}")
    log.info(f"  Title: {incident_info.title}")
    log.info(f"  Description: {incident_info.description}")
    log.info(f"  Priority: {incident_info.priority}")
    log.info(f"  Status State: {incident_info.status.state}")
    log.info(f"  Status Stage: {incident_info.status.stage}")
    log.info(f"  Status Message: {incident_info.status.message}")
    log.info(f"  Affected Entities: {len(incident_info.entities)}")
    for entity_urn in incident_info.entities:
        log.info(f"    - {entity_urn}")

    if incident_info.assignees:
        log.info(f"  Assignees: {len(incident_info.assignees)}")
        for assignee in incident_info.assignees:
            log.info(f"    - {assignee.actor}")

    if incident_info.source:
        log.info(f"  Source Type: {incident_info.source.type}")
        if incident_info.source.sourceUrn:
            log.info(f"  Source URN: {incident_info.source.sourceUrn}")

    log.info(
        f"  Created: {incident_info.created.time} by {incident_info.created.actor}"
    )
    log.info(
        f"  Last Updated: {incident_info.status.lastUpdated.time} by {incident_info.status.lastUpdated.actor}"
    )
else:
    log.warning(f"Incident {incident_urn} not found")

# Query the tags aspect
tags = graph.get_aspect(
    entity_urn=str(incident_urn),
    aspect_type=models.GlobalTagsClass,
)

if tags:
    log.info(f"  Tags: {len(tags.tags)}")
    for tag_association in tags.tags:
        log.info(f"    - {tag_association.tag}")

# Alternative: Use the REST API directly with requests
# This approach is useful for integration with external systems

# Query incident entity using the REST API
headers = {"Content-Type": "application/json"}
if token:
    headers["Authorization"] = f"Bearer {token}"

response = requests.get(
    f"{gms_endpoint}/entities/{incident_urn}",
    headers=headers,
)

if response.status_code == 200:
    entity_data = response.json()
    log.info("\nREST API Response:")
    log.info(f"  Entity URN: {entity_data.get('urn')}")
    log.info(f"  Aspects: {list(entity_data.get('aspects', {}).keys())}")
else:
    log.error(f"Failed to query incident via REST API: {response.status_code}")
