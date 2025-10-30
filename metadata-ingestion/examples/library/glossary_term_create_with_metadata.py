import os

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlossaryTermInfoClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import GlossaryNodeUrn

# Create the term URN
term_urn = make_term_urn("Classification.PII")

# Create GlossaryTermInfo with full metadata
term_info = GlossaryTermInfoClass(
    name="Personally Identifiable Information",
    definition="Information that can be used to identify, contact, or locate a single person, or to identify an individual in context. Examples include name, email address, phone number, and social security number.",
    termSource="INTERNAL",
    # Link to a parent term group (glossary node)
    parentNode=str(GlossaryNodeUrn("Classification")),
    # Custom properties for additional metadata
    customProperties={
        "sensitivity_level": "HIGH",
        "data_retention_period": "7_years",
        "regulatory_framework": "GDPR,CCPA",
    },
)

# Add ownership information
ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner="urn:li:corpuser:datahub",
            type=OwnershipTypeClass.DATAOWNER,
            source=OwnershipSourceClass(type=OwnershipSourceTypeClass.MANUAL),
        ),
        OwnerClass(
            owner="urn:li:corpGroup:privacy-team",
            type=OwnershipTypeClass.DATAOWNER,
            source=OwnershipSourceClass(type=OwnershipSourceTypeClass.MANUAL),
        ),
    ]
)

# Add links to related documentation
institutional_memory = InstitutionalMemoryClass(
    elements=[
        InstitutionalMemoryMetadataClass(
            url="https://wiki.company.com/privacy/pii-guidelines",
            description="Internal PII Handling Guidelines",
            createStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
        ),
        InstitutionalMemoryMetadataClass(
            url="https://gdpr.eu/",
            description="GDPR Official Documentation",
            createStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
        ),
    ]
)

# Emit all aspects for the glossary term
# Get DataHub connection details from environment
gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")

rest_emitter = DatahubRestEmitter(gms_server=gms_server, token=token)

# Emit term info
rest_emitter.emit(MetadataChangeProposalWrapper(entityUrn=term_urn, aspect=term_info))

# Emit ownership
rest_emitter.emit(MetadataChangeProposalWrapper(entityUrn=term_urn, aspect=ownership))

# Emit institutional memory (documentation links)
rest_emitter.emit(
    MetadataChangeProposalWrapper(entityUrn=term_urn, aspect=institutional_memory)
)

print(f"Created glossary term with full metadata: {term_urn}")
