import logging
import time

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    InstitutionalMemoryClass,
)
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Inputs -> owner, ownership_type, dataset
documentation_to_add = (
    "Name of the user who was deleted. This description is updated via PythonSDK."
)
dataset_urn = make_dataset_urn(platform="hive", name="fct_users_deleted", env="PROD")
column = "user_name"
field_info_to_set = EditableSchemaFieldInfoClass(
    fieldPath=column, description=documentation_to_add
)


# Some helpful variables to fill out objects later
now = int(time.time() * 1000)  # milliseconds since epoch
current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")


# First we get the current owners
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(config=DatahubClientConfig(server=gms_endpoint))

current_editable_schema_metadata = graph.get_aspect(
    entity_urn=dataset_urn,
    aspect_type=EditableSchemaMetadataClass,
)


need_write = False

if current_editable_schema_metadata:
    for fieldInfo in current_editable_schema_metadata.editableSchemaFieldInfo:
        if get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath) == column:
            # we have some editable schema metadata for this field
            field_match = True
            if documentation_to_add != fieldInfo.description:
                fieldInfo.description = documentation_to_add
                need_write = True
else:
    # create a brand new editable dataset properties aspect
    current_editable_schema_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[field_info_to_set],
        created=current_timestamp,
    )
    need_write = True

if need_write:
    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=current_editable_schema_metadata,
    )
    graph.emit(event)
    log.info(f"Documentation added to dataset {dataset_urn}")

else:
    log.info("Documentation already exists and is identical, omitting write")


current_institutional_memory = graph.get_aspect(
    entity_urn=dataset_urn, aspect_type=InstitutionalMemoryClass
)

need_write = False
