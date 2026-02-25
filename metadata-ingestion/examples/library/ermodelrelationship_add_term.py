# metadata-ingestion/examples/library/ermodelrelationship_add_term.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)

GMS_ENDPOINT = "http://localhost:8080"
relationship_urn = "urn:li:erModelRelationship:employee_to_company"
term_urn = "urn:li:glossaryTerm:ReferentialIntegrity"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})

# Read current glossary terms
# FIXME: emitter.get not available
# gms_response = emitter.get(relationship_urn, aspects=["glossaryTerms"])
current_terms: dict[
    str, object
] = {}  # gms_response.get("glossaryTerms", {}) if gms_response else {}

# Build new terms list
existing_terms = []
if isinstance(current_terms, dict) and "terms" in current_terms:
    terms_list = current_terms["terms"]
    if isinstance(terms_list, list):
        existing_terms = [term["urn"] for term in terms_list]

# Add new term if not already present
if term_urn not in existing_terms:
    term_associations = [
        GlossaryTermAssociationClass(urn=existing_term)
        for existing_term in existing_terms
    ]
    term_associations.append(GlossaryTermAssociationClass(urn=term_urn))

    glossary_terms = GlossaryTermsClass(
        terms=term_associations,
        auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
    )

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=relationship_urn,
            aspect=glossary_terms,
        )
    )

    print(f"Added glossary term {term_urn} to ER Model Relationship {relationship_urn}")
else:
    print(f"Glossary term {term_urn} already exists on {relationship_urn}")
