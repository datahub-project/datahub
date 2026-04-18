from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import CorpUserUrn, DomainUrn

graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

domain_urn = DomainUrn(id="marketing")

# Get existing ownership
existing_ownership = graph.get_aspect(str(domain_urn), OwnershipClass)
owner_list = (
    list(existing_ownership.owners)
    if existing_ownership and existing_ownership.owners
    else []
)

# Add new owner with the TECHNICAL_OWNER type
owner_list.append(
    OwnerClass(owner=str(CorpUserUrn("jdoe")), type=OwnershipTypeClass.TECHNICAL_OWNER)
)

# Emit ownership
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=str(domain_urn), aspect=OwnershipClass(owners=owner_list)
    )
)
