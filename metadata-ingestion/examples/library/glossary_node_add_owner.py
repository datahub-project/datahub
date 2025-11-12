import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata._urns.urn_defs import CorpUserUrn, GlossaryNodeUrn
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a glossary node URN
node_urn = GlossaryNodeUrn("Finance")

# Define the owner
owner_urn = CorpUserUrn("jdoe")

# Create ownership aspect
# This makes jdoe a TECHNICAL_OWNER of the Finance glossary node
ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner=str(owner_urn),
            type=OwnershipTypeClass.TECHNICAL_OWNER,
        )
    ]
)

# Create the metadata change proposal
event = MetadataChangeProposalWrapper(
    entityUrn=str(node_urn),
    aspect=ownership,
)

# Emit to DataHub
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)

log.info(f"Added owner {owner_urn} to glossary node {node_urn}")
