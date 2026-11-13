# metadata-ingestion/examples/library/notebook_add_owner.py

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"

owner_to_add = make_user_urn("data_scientist")
ownership_type = OwnershipTypeClass.TECHNICAL_OWNER

owners_to_add = [
    OwnerClass(owner=owner_to_add, type=ownership_type),
]

ownership = OwnershipClass(owners=owners_to_add)

event = MetadataChangeProposalWrapper(
    entityUrn=notebook_urn,
    aspect=ownership,
)

emitter.emit(event)
print(f"Added owner {owner_to_add} to notebook {notebook_urn}")
