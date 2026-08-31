# metadata-ingestion/examples/library/version_set_link_entity.py
"""
Link an existing entity to an existing version set.

This example shows how to add a new version of an entity to an existing
version set, with proper version tracking and metadata.
"""

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    MetadataAttributionClass,
    VersioningSchemeClass,
    VersionPropertiesClass,
    VersionSetPropertiesClass,
    VersionTagClass,
)

server = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=server)

# Define the version set and the new model version to link
version_set_urn = "urn:li:versionSet:(abc123def456,mlModel)"
new_model_urn = "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my-model-v2,PROD)"

# Update the version set to mark this as the latest
version_set_properties = VersionSetPropertiesClass(
    latest=new_model_urn,
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
)

version_set_mcp = MetadataChangeProposalWrapper(
    entityUrn=version_set_urn,
    aspect=version_set_properties,
)
emitter.emit(version_set_mcp)

# Create version properties for the new model
version_properties = VersionPropertiesClass(
    versionSet=version_set_urn,
    version=VersionTagClass(
        versionTag="2.0.0",
        metadataAttribution=MetadataAttributionClass(
            time=1675209600000,
            actor="urn:li:corpuser:ml-engineer",
        ),
    ),
    sortId="2.0.0",
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    comment="Major update with improved accuracy",
    aliases=[
        VersionTagClass(versionTag="v2"),
        VersionTagClass(versionTag="latest"),
    ],
    sourceCreatedTimestamp=AuditStampClass(
        time=1675209600000,
        actor="urn:li:corpuser:ml-engineer",
    ),
    metadataCreatedTimestamp=AuditStampClass(
        time=1675209600000,
        actor="urn:li:corpuser:datahub",
    ),
)

# Emit the version properties for the new model
model_version_mcp = MetadataChangeProposalWrapper(
    entityUrn=new_model_urn,
    aspect=version_properties,
)
emitter.emit(model_version_mcp)

print(f"Linked {new_model_urn} to version set {version_set_urn}")
print("Version: 2.0.0")
