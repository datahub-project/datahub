# metadata-ingestion/examples/library/version_set_add_properties.py
"""
Create a version set with custom properties and metadata.

This example demonstrates how to create a version set with rich metadata
including custom properties to track additional information.
"""

from datahub.emitter.mce_builder import datahub_guid
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    VersioningSchemeClass,
    VersionSetPropertiesClass,
)

server = "http://localhost:8080"
emitter = DatahubRestEmitter(gms_server=server)

# Generate a unique ID for the version set
guid_dict = {"platform": "tensorflow", "name": "image-classifier"}
version_set_id = datahub_guid(guid_dict)

# Create the version set URN for an ML model
version_set_urn = f"urn:li:versionSet:({version_set_id},mlModel)"

# Define the latest version (this should be an existing ML model)
latest_model_urn = (
    "urn:li:mlModel:(urn:li:dataPlatform:tensorflow,image-classifier-v3,PROD)"
)

# Create version set properties with custom metadata
version_set_properties = VersionSetPropertiesClass(
    latest=latest_model_urn,
    versioningScheme=VersioningSchemeClass.LEXICOGRAPHIC_STRING,
    customProperties={
        "model_type": "image_classification",
        "framework": "tensorflow",
        "architecture": "resnet50",
        "dataset": "imagenet",
        "owner_team": "ml-platform",
        "cost_center": "ML-001",
    },
)

# Emit the version set with properties
version_set_mcp = MetadataChangeProposalWrapper(
    entityUrn=version_set_urn,
    aspect=version_set_properties,
)
emitter.emit(version_set_mcp)

print(f"Created version set: {version_set_urn}")
print(f"Latest version: {latest_model_urn}")
print("Custom properties:")
for key, value in version_set_properties.customProperties.items():
    print(f"  {key}: {value}")
