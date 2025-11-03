# metadata-ingestion/examples/library/tag_add_ownership.py
import logging

from datahub.emitter.mce_builder import make_tag_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a tag URN
tag_urn = make_tag_urn("data_quality")

# Define ownership
ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner=make_user_urn("data_steward"),
            type=OwnershipTypeClass.DATAOWNER,
        )
    ]
)

# Create the metadata change proposal
event = MetadataChangeProposalWrapper(
    entityUrn=tag_urn,
    aspect=ownership,
)

# Emit to DataHub
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Added ownership to tag {tag_urn}")
