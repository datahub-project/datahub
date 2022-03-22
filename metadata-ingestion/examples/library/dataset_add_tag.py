import logging
from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# read-modify-write requires access to the DataHubGraph (RestEmitter is not enough)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# First we get the current tags
gms_endpoint = "http://localhost:8080"
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))

dataset_urn = make_dataset_urn(platform="hive", name="realestate_db.sales", env="PROD")

current_tags: Optional[GlobalTagsClass] = graph.get_aspect_v2(
    entity_urn=dataset_urn,
    aspect="globalTags",
    aspect_type=GlobalTagsClass,
)

tag_to_add = make_tag_urn("purchase")
tag_association_to_add = TagAssociationClass(tag=tag_to_add)

need_write = False
if current_tags:
    if tag_to_add not in [x.tag for x in current_tags.tags]:
        # tags exist, but this tag is not present in the current tags
        current_tags.tags.append(TagAssociationClass(tag_to_add))
        need_write = True
else:
    # create a brand new tags aspect
    current_tags = GlobalTagsClass(tags=[tag_association_to_add])
    need_write = True

if need_write:
    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=dataset_urn,
        aspectName="globalTags",
        aspect=current_tags,
    )
    graph.emit(event)
    log.info(f"Tag {tag_to_add} added to dataset {dataset_urn}")

else:
    log.info(f"Tag {tag_to_add} already exists, omitting write")
