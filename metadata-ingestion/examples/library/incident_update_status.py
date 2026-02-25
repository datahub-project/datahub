# metadata-ingestion/examples/library/incident_update_status.py
import logging

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.urns import IncidentUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
gms_endpoint = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=gms_endpoint, extra_headers={})
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

# Specify the incident to update (use the incident ID from incident_create.py)
incident_id = "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d"
incident_urn = IncidentUrn(incident_id)

# Retrieve the current incident info to preserve other fields
current_incident_info = graph.get_aspect(
    entity_urn=str(incident_urn),
    aspect_type=models.IncidentInfoClass,
)

if not current_incident_info:
    raise ValueError(f"Incident {incident_urn} not found")

# Get the current actor URN for audit stamps
actor_urn = builder.make_user_urn("jdoe")
audit_stamp = models.AuditStampClass(
    time=int(builder.get_sys_time() * 1000),
    actor=actor_urn,
)

# Update the status to reflect progress in resolving the incident
current_incident_info.status = models.IncidentStatusClass(
    state=models.IncidentStateClass.ACTIVE,
    stage=models.IncidentStageClass.WORK_IN_PROGRESS,
    message="Pipeline has been restarted. Monitoring for successful completion.",
    lastUpdated=audit_stamp,
)

# Optionally update priority if severity assessment changed
current_incident_info.priority = (
    1  # HIGH priority (0=CRITICAL, 1=HIGH, 2=MEDIUM, 3=LOW)
)

# Optionally assign team members to work on the incident
assignee1 = models.IncidentAssigneeClass(
    actor=builder.make_user_urn("jdoe"),
    assignedAt=audit_stamp,
)
assignee2 = models.IncidentAssigneeClass(
    actor=builder.make_user_urn("asmith"),
    assignedAt=audit_stamp,
)
current_incident_info.assignees = [assignee1, assignee2]

# Create and emit the metadata change proposal
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=str(incident_urn),
    aspect=current_incident_info,
)

emitter.emit(metadata_change_proposal)
log.info(
    f"Updated incident {incident_urn} status to {current_incident_info.status.state}"
)
log.info(
    f"Status details: stage={current_incident_info.status.stage}, message={current_incident_info.status.message}"
)
log.info(f"Priority updated to {current_incident_info.priority}")
log.info(f"Assigned to {len(current_incident_info.assignees)} team members")
