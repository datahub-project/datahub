# metadata-ingestion/examples/library/incident_create.py
import logging
import os
import uuid

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata._urns.urn_defs import IncidentUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
gms_endpoint = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=gms_endpoint, token=token)

# Generate a unique incident ID
incident_id = str(uuid.uuid4())
incident_urn = IncidentUrn(incident_id)

# Create the dataset URN that this incident affects
dataset_urn = builder.make_dataset_urn(
    platform="snowflake", name="analytics.sales_fact", env="PROD"
)

# Get the current actor URN for audit stamps
actor_urn = builder.make_user_urn("datahub")
audit_stamp = models.AuditStampClass(
    time=int(builder.get_sys_time() * 1000),
    actor=actor_urn,
)

# Create the incident info aspect
incident_info = models.IncidentInfoClass(
    type=models.IncidentTypeClass.FRESHNESS,
    title="Sales data not updated in 48 hours",
    description="The sales_fact table has not been refreshed since 2023-10-15. Expected daily updates are missing, which may impact downstream reporting and dashboards.",
    entities=[dataset_urn],
    status=models.IncidentStatusClass(
        state=models.IncidentStateClass.ACTIVE,
        stage=models.IncidentStageClass.TRIAGE,
        message="Investigating potential pipeline failure",
        lastUpdated=audit_stamp,
    ),
    priority=0,  # CRITICAL priority (0=CRITICAL, 1=HIGH, 2=MEDIUM, 3=LOW)
    source=models.IncidentSourceClass(type=models.IncidentSourceTypeClass.MANUAL),
    created=audit_stamp,
)

# Create and emit the metadata change proposal
metadata_change_proposal = MetadataChangeProposalWrapper(
    entityUrn=str(incident_urn),
    aspect=incident_info,
)

emitter.emit(metadata_change_proposal)
log.info(f"Created incident {incident_urn} for dataset {dataset_urn}")
log.info(
    f"Incident details: type={incident_info.type}, priority={incident_info.priority}, status={incident_info.status.state}"
)
