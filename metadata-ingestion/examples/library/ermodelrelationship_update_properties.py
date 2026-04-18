# metadata-ingestion/examples/library/ermodelrelationship_update_properties.py
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableERModelRelationshipPropertiesClass,
)

GMS_ENDPOINT = "http://localhost:8080"
relationship_urn = "urn:li:erModelRelationship:employee_to_company"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})

# Create or update editable properties
audit_stamp = AuditStampClass(
    time=int(time.time() * 1000), actor="urn:li:corpuser:datahub"
)

editable_properties = EditableERModelRelationshipPropertiesClass(
    name="Employee-Company Foreign Key",
    description=(
        "This relationship establishes referential integrity between the Employee "
        "and Company tables. Each employee record must reference a valid company. "
        "The relationship enforces CASCADE on both UPDATE and DELETE operations, "
        "meaning changes to company IDs will propagate to employee records, and "
        "deleting a company will delete all associated employees."
    ),
    created=audit_stamp,
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=relationship_urn,
        aspect=editable_properties,
    )
)

print(f"Updated editable properties for ER Model Relationship {relationship_urn}")
print(f"Name: {editable_properties.name}")
print(f"Description: {editable_properties.description}")
