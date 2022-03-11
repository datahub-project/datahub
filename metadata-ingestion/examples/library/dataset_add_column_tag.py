import logging
import time

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    TagAssociationClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_simple_field_path_from_v2_field_path(field_path: str) -> str:
    """A helper function to extract simple . path notation from the v2 field path"""
    if field_path.startswith("[version=2.0]"):
        # this is a v2 field path
        tokens = [
            t
            for t in field_path.split(".")
            if not (t.startswith("[") or t.endswith("]"))
        ]
        path = ".".join(tokens)
        return path
    else:
        # not a v2, we assume this is a simple path
        return field_path


# Inputs -> the column, dataset and the tag to set
column = "address.zipcode"
dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")
tag_to_add = make_tag_urn("location")


# First we get the current editable schema metadata
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))


current_editable_schema_metadata = graph.get_aspect_v2(
    entity_urn=dataset_urn,
    aspect="editableSchemaMetadata",
    aspect_type=EditableSchemaMetadataClass,
)


# Some pre-built objects to help all the conditional pathways
tag_association_to_add = TagAssociationClass(tag=tag_to_add)
tags_aspect_to_set = GlobalTagsClass(tags=[tag_association_to_add])
field_info_to_set = EditableSchemaFieldInfoClass(
    fieldPath=column, globalTags=tags_aspect_to_set
)


need_write = False
field_match = False
if current_editable_schema_metadata:
    for fieldInfo in current_editable_schema_metadata.editableSchemaFieldInfo:
        if get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath) == column:
            # we have some editable schema metadata for this field
            field_match = True
            if fieldInfo.globalTags:
                if tag_to_add not in [x.tag for x in fieldInfo.globalTags.tags]:
                    # this tag is not present
                    fieldInfo.globalTags.tags.append(tag_association_to_add)
                    need_write = True
            else:
                fieldInfo.globalTags = tags_aspect_to_set
                need_write = True

    if not field_match:
        # this field isn't present in the editable schema metadata aspect, add it
        field_info = field_info_to_set
        current_editable_schema_metadata.editableSchemaFieldInfo.append(field_info)
        need_write = True

else:
    # create a brand new editable schema metadata aspect
    now = int(time.time() * 1000)  # milliseconds since epoch
    current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
    current_editable_schema_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[field_info_to_set],
        created=current_timestamp,
    )
    need_write = True

if need_write:
    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataset_urn,
        aspectName="editableSchemaMetadata",
        aspect=current_editable_schema_metadata,
    )
    graph.emit(event)
    log.info(f"Tag {tag_to_add} added to column {column} of dataset {dataset_urn}")

else:
    log.info(f"Tag {tag_to_add} already attached to column {column}, omitting write")
