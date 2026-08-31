# metadata-ingestion/examples/library/notebook_add_tags.py
import logging

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

notebook_urn = "urn:li:notebook:(querybook,customer_analysis_2024)"

tag_to_add = make_tag_urn("production")
tag_association = TagAssociationClass(tag=tag_to_add)

global_tags = GlobalTagsClass(tags=[tag_association])

event = MetadataChangeProposalWrapper(
    entityUrn=notebook_urn,
    aspect=global_tags,
)

emitter.emit(event)
log.info(f"Added tag {tag_to_add} to notebook {notebook_urn}")
