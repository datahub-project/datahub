import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DomainPropertiesClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
)
from datahub.metadata.urns import CorpUserUrn, DomainUrn

graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

domain_urn = DomainUrn(id="marketing")

# Get existing properties
existing_properties = graph.get_aspect(str(domain_urn), DomainPropertiesClass)

# Update description
if existing_properties:
    existing_properties.description = (
        "The Marketing domain contains all data assets related to marketing operations, "
        "campaigns, customer analytics, and brand management."
    )
    properties = existing_properties
else:
    properties = DomainPropertiesClass(
        name="Marketing",
        description=(
            "The Marketing domain contains all data assets related to marketing operations, "
            "campaigns, customer analytics, and brand management."
        ),
    )

# Emit properties
emitter.emit_mcp(
    MetadataChangeProposalWrapper(entityUrn=str(domain_urn), aspect=properties)
)

# Get existing institutional memory
existing_memory = graph.get_aspect(str(domain_urn), InstitutionalMemoryClass)
links_list = (
    list(existing_memory.elements)
    if existing_memory and existing_memory.elements
    else []
)

# Add new links
audit_stamp = AuditStampClass(
    time=int(time.time() * 1000), actor=str(CorpUserUrn("datahub"))
)

links_list.append(
    InstitutionalMemoryMetadataClass(
        url="https://wiki.company.com/domains/marketing",
        description="Marketing Domain Wiki - Overview and Guidelines",
        createStamp=audit_stamp,
    )
)

links_list.append(
    InstitutionalMemoryMetadataClass(
        url="https://confluence.company.com/marketing-data-governance",
        description="Marketing Data Governance Policies",
        createStamp=audit_stamp,
    )
)

# Emit institutional memory
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=str(domain_urn), aspect=InstitutionalMemoryClass(elements=links_list)
    )
)
