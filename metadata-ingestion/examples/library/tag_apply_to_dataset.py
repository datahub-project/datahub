# metadata-ingestion/examples/library/tag_apply_to_dataset.py
import logging

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create URNs
dataset_urn = make_dataset_urn(
    platform="snowflake", name="db.schema.customers", env="PROD"
)
tag_urn = make_tag_urn("pii")

# Define global tags
global_tags = GlobalTagsClass(
    tags=[
        TagAssociationClass(tag=tag_urn),
    ]
)

# Create the metadata change proposal
event = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=global_tags,
)

# Emit to DataHub
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Applied tag {tag_urn} to dataset {dataset_urn}")
