# metadata-ingestion/examples/library/version_set_link_multiple_versions.py
"""
Link multiple versions of an entity to a version set.

This example demonstrates creating a complete version history for an ML model,
showing how to manage multiple versions with semantic versioning.
"""

from typing import TypedDict

from datahub.emitter.mce_builder import datahub_guid
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


class VersionInfo(TypedDict):
    urn: str
    version: str
    sortId: str
    comment: str
    timestamp: int
    aliases: list[str]
    actor: str


# Generate version set URN
guid_dict = {"platform": "pytorch", "name": "sentiment-analyzer"}
version_set_id = datahub_guid(guid_dict)
version_set_urn = f"urn:li:versionSet:({version_set_id},mlModel)"

# Define the model versions we want to link
versions: list[VersionInfo] = [
    {
        "urn": "urn:li:mlModel:(urn:li:dataPlatform:pytorch,sentiment-analyzer-v1,PROD)",
        "version": "1.0.0",
        "sortId": "1.0.0",
        "comment": "Initial release with basic sentiment analysis",
        "timestamp": 1672531200000,
        "aliases": ["v1"],
        "actor": "urn:li:corpuser:data-scientist",
    },
    {
        "urn": "urn:li:mlModel:(urn:li:dataPlatform:pytorch,sentiment-analyzer-v1.1,PROD)",
        "version": "1.1.0",
        "sortId": "1.1.0",
        "comment": "Minor improvements to accuracy",
        "timestamp": 1675209600000,
        "aliases": ["v1.1"],
        "actor": "urn:li:corpuser:data-scientist",
    },
    {
        "urn": "urn:li:mlModel:(urn:li:dataPlatform:pytorch,sentiment-analyzer-v2,PROD)",
        "version": "2.0.0",
        "sortId": "2.0.0",
        "comment": "Major update with multi-language support",
        "timestamp": 1677628800000,
        "aliases": ["v2", "latest", "production"],
        "actor": "urn:li:corpuser:ml-engineer",
    },
]

# Link each version to the version set
for i, version_info in enumerate(versions):
    is_latest = i == len(versions) - 1

    # Create version properties for each model
    version_properties = VersionPropertiesClass(
        versionSet=version_set_urn,
        version=VersionTagClass(
            versionTag=version_info["version"],
            metadataAttribution=MetadataAttributionClass(
                time=version_info["timestamp"],
                actor=version_info["actor"],
            ),
        ),
        sortId=version_info["sortId"],
        versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
        comment=version_info["comment"],
        aliases=[
            VersionTagClass(versionTag=alias) for alias in version_info["aliases"]
        ],
        sourceCreatedTimestamp=AuditStampClass(
            time=version_info["timestamp"],
            actor=version_info["actor"],
        ),
        metadataCreatedTimestamp=AuditStampClass(
            time=version_info["timestamp"],
            actor="urn:li:corpuser:datahub",
        ),
    )

    # Emit version properties
    model_version_mcp = MetadataChangeProposalWrapper(
        entityUrn=version_info["urn"],
        aspect=version_properties,
    )
    emitter.emit(model_version_mcp)

    print(f"Linked version {version_info['version']}: {version_info['urn']}")

# Update version set to point to the latest version
version_set_properties = VersionSetPropertiesClass(
    latest=versions[-1]["urn"],
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    customProperties={
        "model_type": "sentiment_analysis",
        "language_support": "multi-language",
        "framework": "pytorch",
    },
)

version_set_mcp = MetadataChangeProposalWrapper(
    entityUrn=version_set_urn,
    aspect=version_set_properties,
)
emitter.emit(version_set_mcp)

print(f"\nVersion set created: {version_set_urn}")
print(f"Total versions: {len(versions)}")
print(f"Latest version: {versions[-1]['version']}")
