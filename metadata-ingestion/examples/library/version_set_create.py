# metadata-ingestion/examples/library/version_set_create.py
"""
Create a new version set by linking the first versioned entity.

This example demonstrates creating a version set for an ML model,
establishing the first version in the set.
"""

import os

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

server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
token = os.getenv("DATAHUB_GMS_TOKEN")
emitter = DatahubRestEmitter(gms_server=server, token=token)

# Define the ML model URN that we want to version
model_urn = "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my-model-v1,PROD)"

# Generate a unique ID for the version set based on the model name
guid_dict = {"platform": "sagemaker", "name": "my-model"}
version_set_id = datahub_guid(guid_dict)

# Create the version set URN
version_set_urn = f"urn:li:versionSet:({version_set_id},mlModel)"

# Create version set properties aspect
version_set_properties = VersionSetPropertiesClass(
    latest=model_urn,
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    customProperties={
        "model_family": "recommendation",
        "framework": "tensorflow",
    },
)

# Emit the version set properties
version_set_mcp = MetadataChangeProposalWrapper(
    entityUrn=version_set_urn,
    aspect=version_set_properties,
)
emitter.emit(version_set_mcp)

# Create version properties for the ML model
version_properties = VersionPropertiesClass(
    versionSet=version_set_urn,
    version=VersionTagClass(
        versionTag="1.0.0",
        metadataAttribution=MetadataAttributionClass(
            time=1672531200000,
            actor="urn:li:corpuser:datahub",
        ),
    ),
    sortId="1.0.0",
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    comment="Initial production release",
    aliases=[
        VersionTagClass(versionTag="v1"),
        VersionTagClass(versionTag="stable"),
    ],
    metadataCreatedTimestamp=AuditStampClass(
        time=1672531200000,
        actor="urn:li:corpuser:datahub",
    ),
)

# Emit the version properties for the model
model_version_mcp = MetadataChangeProposalWrapper(
    entityUrn=model_urn,
    aspect=version_properties,
)
emitter.emit(model_version_mcp)

print(f"Created version set: {version_set_urn}")
print(f"Linked first version: {model_urn} with version 1.0.0")
