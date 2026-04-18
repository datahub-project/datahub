# metadata-ingestion/examples/library/query_add_term.py
import logging

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)
from datahub.metadata.urns import QueryUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

query_urn = QueryUrn("my-unique-query-id")

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

existing_terms = graph.get_aspect(
    entity_urn=query_urn.urn(),
    aspect_type=GlossaryTermsClass,
)

terms_to_add = existing_terms.terms if existing_terms else []

terms_to_add.append(
    GlossaryTermAssociationClass(
        urn=make_term_urn("CustomerData"), context="Query subject area"
    )
)

glossary_terms_aspect = GlossaryTermsClass(
    terms=terms_to_add,
    auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
)

event = MetadataChangeProposalWrapper(
    entityUrn=query_urn.urn(),
    aspect=glossary_terms_aspect,
)

emitter.emit(event)
log.info(f"Added glossary term to query {query_urn}")
