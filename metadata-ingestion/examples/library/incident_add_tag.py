# metadata-ingestion/examples/library/incident_add_tag.py
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

# Specify the incident to tag (use the incident ID from incident_create.py)
incident_id = "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d"
incident_urn = IncidentUrn(incident_id)

# Create the tag URN
tag_urn = builder.make_tag_urn("data-quality")

# Get the current actor URN for audit stamps
actor_urn = builder.make_user_urn("datahub")
audit_stamp = models.AuditStampClass(
    time=int(builder.get_sys_time() * 1000),
    actor=actor_urn,
)

# Read current tags to preserve existing ones
current_tags = graph.get_aspect(
    entity_urn=str(incident_urn),
    aspect_type=models.GlobalTagsClass,
)

# Create tag association
tag_association = models.TagAssociationClass(
    tag=tag_urn,
    context="incident_categorization",
)

if current_tags:
    # Check if tag already exists
    tag_exists = any(existing_tag.tag == tag_urn for existing_tag in current_tags.tags)
    if not tag_exists:
        current_tags.tags.append(tag_association)
        updated_tags = current_tags
    else:
        log.info(f"Tag {tag_urn} already exists on incident {incident_urn}")
        updated_tags = current_tags
else:
    # No existing tags, create new GlobalTags aspect
    updated_tags = models.GlobalTagsClass(tags=[tag_association])

# Create and emit the metadata change proposal
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=str(incident_urn),
    aspect=updated_tags,
)

emitter.emit(metadata_change_proposal)
log.info(f"Added tag {tag_urn} to incident {incident_urn}")
log.info(f"Incident now has {len(updated_tags.tags)} tag(s)")
