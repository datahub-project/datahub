# metadata-ingestion/examples/library/incident_query_rest_api.py
import os

import datahub.metadata.schema_classes as models
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import IncidentUrn

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

if incident_info is None:
    raise SystemExit(f"Incident not found: {incident_urn}")

print(f"Incident: {incident_urn}")
print(f"  Type: {incident_info.type}")
print(f"  Title: {incident_info.title}")
print(f"  Description: {incident_info.description}")
print(f"  Priority: {incident_info.priority}")
print(f"  Status State: {incident_info.status.state}")
print(f"  Status Stage: {incident_info.status.stage}")
print(f"  Status Message: {incident_info.status.message}")
print(f"  Affected Entities: {len(incident_info.entities)}")
for entity_urn in incident_info.entities:
    print(f"    - {entity_urn}")

if incident_info.assignees:
    print(f"  Assignees: {len(incident_info.assignees)}")
    for assignee in incident_info.assignees:
        print(f"    - {assignee.actor}")

if incident_info.source:
    print(f"  Source Type: {incident_info.source.type}")
    if incident_info.source.sourceUrn:
        print(f"  Source URN: {incident_info.source.sourceUrn}")

print(f"  Created: {incident_info.created.time} by {incident_info.created.actor}")
print(
    f"  Last Updated: {incident_info.status.lastUpdated.time} by {incident_info.status.lastUpdated.actor}"
)

# Query the tags aspect
tags = graph.get_aspect(
    entity_urn=str(incident_urn),
    aspect_type=models.GlobalTagsClass,
)

if tags:
    print(f"  Tags: {len(tags.tags)}")
    for tag_association in tags.tags:
        print(f"    - {tag_association.tag}")
