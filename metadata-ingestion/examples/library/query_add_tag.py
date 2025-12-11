# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/query_add_tag.py
import logging

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass
from datahub.metadata.urns import QueryUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

query_urn = QueryUrn("my-unique-query-id")

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

existing_tags = graph.get_aspect(
    entity_urn=query_urn.urn(),
    aspect_type=GlobalTagsClass,
)

tags_to_add = existing_tags.tags if existing_tags else []

tags_to_add.append(
    TagAssociationClass(tag=make_tag_urn("production"), context="Query categorization")
)

global_tags_aspect = GlobalTagsClass(tags=tags_to_add)

event = MetadataChangeProposalWrapper(
    entityUrn=query_urn.urn(),
    aspect=global_tags_aspect,
)

emitter.emit(event)
log.info(f"Added tag to query {query_urn}")
