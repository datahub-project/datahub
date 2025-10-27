# metadata-ingestion/examples/library/application_add_owner.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def make_user_urn(username: str) -> str:
    """Create a DataHub user URN."""
    return f"urn:li:corpuser:{username}"


emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

application_urn = make_application_urn("customer-analytics-service")

owner_to_add = make_user_urn("jdoe")

ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner=owner_to_add,
            type=OwnershipTypeClass.TECHNICAL_OWNER,
        )
    ]
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=application_urn,
    aspect=ownership,
)
emitter.emit(metadata_event)

print(f"Added owner {owner_to_add} to application {application_urn}")
