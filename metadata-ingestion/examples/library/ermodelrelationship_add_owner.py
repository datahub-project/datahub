# metadata-ingestion/examples/library/ermodelrelationship_add_owner.py
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

GMS_ENDPOINT = "http://localhost:8080"
relationship_urn = "urn:li:erModelRelationship:employee_to_company"
owner_urn = "urn:li:corpuser:jdoe"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})

# Read current ownership
# FIXME: emitter.get not available
# gms_response = emitter.get(relationship_urn, aspects=["ownership"])
current_ownership: dict[
    str, object
] = {}  # gms_response.get("ownership", {}) if gms_response else {}

# Build new owners list
existing_owners = []
if isinstance(current_ownership, dict) and "owners" in current_ownership:
    owners_list = current_ownership["owners"]
    if isinstance(owners_list, list):
        existing_owners = [owner["owner"] for owner in owners_list]

# Add new owner if not already present
if owner_urn not in existing_owners:
    owner_list = [
        OwnerClass(owner=existing_owner, type=OwnershipTypeClass.DATAOWNER)
        for existing_owner in existing_owners
    ]
    owner_list.append(
        OwnerClass(
            owner=owner_urn,
            type=OwnershipTypeClass.DATAOWNER,
        )
    )

    ownership = OwnershipClass(
        owners=owner_list,
        lastModified=AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=relationship_urn,
            aspect=ownership,
        )
    )

    print(f"Added owner {owner_urn} to ER Model Relationship {relationship_urn}")
else:
    print(f"Owner {owner_urn} already exists on {relationship_urn}")
