# metadata-ingestion/examples/library/platform_instance_add_metadata.py
import time

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)

# Create the platform instance URN
platform_instance_urn = builder.make_dataplatform_instance_urn(
    platform="snowflake", instance="acme-prod-account"
)

# Add ownership
owners = [
    OwnerClass(
        owner=builder.make_user_urn("data-platform-team"),
        type=OwnershipTypeClass.TECHNICAL_OWNER,
    ),
    OwnerClass(
        owner=builder.make_user_urn("john.doe"),
        type=OwnershipTypeClass.DATAOWNER,
    ),
]

ownership_mcp = MetadataChangeProposalWrapper(
    entityUrn=platform_instance_urn,
    aspect=OwnershipClass(owners=owners),
)

# Add tags
tags = GlobalTagsClass(
    tags=[
        TagAssociationClass(tag=builder.make_tag_urn("production")),
        TagAssociationClass(tag=builder.make_tag_urn("pci-compliant")),
        TagAssociationClass(tag=builder.make_tag_urn("tier-1")),
    ]
)

tags_mcp = MetadataChangeProposalWrapper(
    entityUrn=platform_instance_urn,
    aspect=tags,
)

# Add institutional memory (links)
links = InstitutionalMemoryClass(
    elements=[
        InstitutionalMemoryMetadataClass(
            url="https://wiki.company.com/snowflake-prod-runbook",
            description="Production Snowflake Runbook",
            createStamp=AuditStampClass(
                time=int(time.time() * 1000), actor=builder.make_user_urn("datahub")
            ),
        ),
        InstitutionalMemoryMetadataClass(
            url="https://wiki.company.com/snowflake-access-guide",
            description="How to request access to production Snowflake",
            createStamp=AuditStampClass(
                time=int(time.time() * 1000), actor=builder.make_user_urn("datahub")
            ),
        ),
    ]
)

links_mcp = MetadataChangeProposalWrapper(
    entityUrn=platform_instance_urn,
    aspect=links,
)

# Emit all metadata changes
emitter = DatahubRestEmitter("http://localhost:8080")
emitter.emit_mcp(ownership_mcp)
emitter.emit_mcp(tags_mcp)
emitter.emit_mcp(links_mcp)

print(f"Added ownership, tags, and links to: {platform_instance_urn}")
