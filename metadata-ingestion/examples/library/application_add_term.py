# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/application_add_term.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)


def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def make_term_urn(term_name: str) -> str:
    """Create a DataHub glossary term URN."""
    return f"urn:li:glossaryTerm:{term_name}"


emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

application_urn = make_application_urn("customer-analytics-service")

term_to_add = make_term_urn("CustomerData")

terms = GlossaryTermsClass(
    terms=[
        GlossaryTermAssociationClass(urn=term_to_add),
    ],
    auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=application_urn,
    aspect=terms,
)
emitter.emit(metadata_event)

print(f"Added term {term_to_add} to application {application_urn}")
