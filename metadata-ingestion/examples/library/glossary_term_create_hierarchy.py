import logging
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata._urns.urn_defs import GlossaryNodeUrn, GlossaryTermUrn
from datahub.metadata.schema_classes import (
    GlossaryNodeInfoClass,
    GlossaryTermInfoClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create a multi-level glossary hierarchy:
# DataGovernance
#   ├── Classification
#   │   ├── Public (term)
#   │   └── Confidential (term)
#   └── PersonalInformation
#       ├── Email (term)
#       └── SSN (term)

rest_emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

# Level 1: Root node
root_urn = GlossaryNodeUrn("DataGovernance")
root_info = GlossaryNodeInfoClass(
    definition="Top-level governance structure for data classification and management",
    name="Data Governance",
)
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=str(root_urn),
        aspect=root_info,
    )
)
log.info(f"Created root node: {root_urn}")

# Level 2: Child nodes
classification_urn = GlossaryNodeUrn("Classification")
classification_info = GlossaryNodeInfoClass(
    definition="Data classification categories",
    name="Classification",
    parentNode=str(root_urn),
)
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=str(classification_urn),
        aspect=classification_info,
    )
)
log.info(f"Created child node: {classification_urn}")

pii_urn = GlossaryNodeUrn("PersonalInformation")
pii_info = GlossaryNodeInfoClass(
    definition="Personal and sensitive data categories",
    name="Personal Information",
    parentNode=str(root_urn),
)
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=str(pii_urn),
        aspect=pii_info,
    )
)
log.info(f"Created child node: {pii_urn}")

# Level 3: Terms under Classification
public_term_urn = GlossaryTermUrn("Public")
public_term_info = GlossaryTermInfoClass(
    definition="Publicly available data with no restrictions",
    termSource="INTERNAL",
    name="Public",
    parentNode=str(classification_urn),
)
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=str(public_term_urn),
        aspect=public_term_info,
    )
)
log.info(f"Created term: {public_term_urn}")

confidential_term_urn = GlossaryTermUrn("Confidential")
confidential_term_info = GlossaryTermInfoClass(
    definition="Restricted access data for internal use only",
    termSource="INTERNAL",
    name="Confidential",
    parentNode=str(classification_urn),
)
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=str(confidential_term_urn),
        aspect=confidential_term_info,
    )
)
log.info(f"Created term: {confidential_term_urn}")

# Level 3: Terms under PersonalInformation
email_term_urn = GlossaryTermUrn("Email")
email_term_info = GlossaryTermInfoClass(
    definition="Email addresses that can identify individuals",
    termSource="INTERNAL",
    name="Email Address",
    parentNode=str(pii_urn),
)
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=str(email_term_urn),
        aspect=email_term_info,
    )
)
log.info(f"Created term: {email_term_urn}")

ssn_term_urn = GlossaryTermUrn("SSN")
ssn_term_info = GlossaryTermInfoClass(
    definition="Social Security Numbers - highly sensitive personal identifiers",
    termSource="INTERNAL",
    name="Social Security Number",
    parentNode=str(pii_urn),
)
rest_emitter.emit(
    MetadataChangeProposalWrapper(
        entityUrn=str(ssn_term_urn),
        aspect=ssn_term_info,
    )
)
log.info(f"Created term: {ssn_term_urn}")

log.info("Successfully created glossary hierarchy with nodes and terms")
