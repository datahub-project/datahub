# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/assertion_add_tags.py
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
)

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter("http://localhost:8080")

assertion_urn = "urn:li:assertion:432475190cc846f2894b5b3aa4d55af2"

existing_tags = graph.get_aspect(
    entity_urn=assertion_urn,
    aspect_type=GlobalTagsClass,
)

if existing_tags is None:
    existing_tags = GlobalTagsClass(tags=[])

tag_to_add = builder.make_tag_urn("data-quality")

tag_association = TagAssociationClass(tag=tag_to_add)

if tag_association not in existing_tags.tags:
    existing_tags.tags.append(tag_association)

    tags_mcp = MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=existing_tags,
    )

    emitter.emit_mcp(tags_mcp)
    print(f"Added tag '{tag_to_add}' to assertion {assertion_urn}")
else:
    print(f"Tag '{tag_to_add}' already exists on assertion {assertion_urn}")
