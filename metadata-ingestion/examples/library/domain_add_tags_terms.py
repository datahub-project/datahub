import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    TagAssociationClass,
)
from datahub.metadata.urns import DomainUrn, GlossaryTermUrn, TagUrn

graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

domain_urn = DomainUrn(id="marketing")

# Get existing tags
existing_tags = graph.get_aspect(str(domain_urn), GlobalTagsClass)
tag_list = list(existing_tags.tags) if existing_tags and existing_tags.tags else []

# Add new tags
tag_list.append(TagAssociationClass(tag=str(TagUrn("pii"))))
tag_list.append(TagAssociationClass(tag=str(TagUrn("customer-data"))))

# Emit tags
emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=str(domain_urn), aspect=GlobalTagsClass(tags=tag_list)
    )
)

# Get existing terms
existing_terms = graph.get_aspect(str(domain_urn), GlossaryTermsClass)
term_list = (
    list(existing_terms.terms) if existing_terms and existing_terms.terms else []
)

# Add new terms
term_list.append(
    GlossaryTermAssociationClass(urn=str(GlossaryTermUrn("marketing.customer")))
)
term_list.append(
    GlossaryTermAssociationClass(urn=str(GlossaryTermUrn("marketing.campaign")))
)

# Emit terms
audit_stamp = AuditStampClass(
    time=int(time.time() * 1000), actor="urn:li:corpuser:datahub"
)

emitter.emit_mcp(
    MetadataChangeProposalWrapper(
        entityUrn=str(domain_urn),
        aspect=GlossaryTermsClass(terms=term_list, auditStamp=audit_stamp),
    )
)
