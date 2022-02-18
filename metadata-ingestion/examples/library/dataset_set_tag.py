# Imports for urn construction utility methods
import logging

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")
tag_urn = make_tag_urn("purchase")
event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=dataset_urn,
    aspectName="globalTags",
    aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)]),
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
rest_emitter.emit(event)
log.info(f"Set tags to {tag_urn} for dataset {dataset_urn}")
