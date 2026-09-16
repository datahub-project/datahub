# metadata-ingestion/examples/library/ermodelrelationship_add_tag.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)

GMS_ENDPOINT = "http://localhost:8080"
relationship_urn = "urn:li:erModelRelationship:employee_to_company"
tag_urn = "urn:li:tag:ForeignKey"

emitter = DatahubRestEmitter(gms_server=GMS_ENDPOINT, extra_headers={})

# Read current tags
# FIXME: emitter.get not available
# gms_response = emitter.get(relationship_urn, aspects=["globalTags"])
current_tags: dict[
    str, object
] = {}  # gms_response.get("globalTags", {}) if gms_response else {}

# Build new tags list
existing_tags = []
if isinstance(current_tags, dict) and "tags" in current_tags:
    tags_list = current_tags["tags"]
    if isinstance(tags_list, list):
        existing_tags = [tag["tag"] for tag in tags_list]

# Add new tag if not already present
if tag_urn not in existing_tags:
    tag_associations = [
        TagAssociationClass(tag=existing_tag) for existing_tag in existing_tags
    ]
    tag_associations.append(TagAssociationClass(tag=tag_urn))

    global_tags = GlobalTagsClass(tags=tag_associations)

    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=relationship_urn,
            aspect=global_tags,
        )
    )

    print(f"Added tag {tag_urn} to ER Model Relationship {relationship_urn}")
else:
    print(f"Tag {tag_urn} already exists on {relationship_urn}")
