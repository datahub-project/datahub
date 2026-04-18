import logging

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

business_attribute_urn = "urn:li:businessAttribute:customer_id"

owner_to_add = make_user_urn("data_steward")
ownership_type = OwnershipTypeClass.DATA_STEWARD

owners_to_add = [
    OwnerClass(owner=owner_to_add, type=ownership_type),
]

ownership = OwnershipClass(owners=owners_to_add)

event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=business_attribute_urn,
    aspect=ownership,
)

rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Added owner {owner_to_add} to business attribute {business_attribute_urn}")
