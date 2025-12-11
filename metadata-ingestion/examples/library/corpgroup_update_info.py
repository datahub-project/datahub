# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/corpgroup_update_info.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    CorpGroupEditableInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

group_urn = str(CorpGroupUrn("data-engineering"))

editable_info = CorpGroupEditableInfoClass(
    description="Updated description: The data engineering team builds and maintains data pipelines, infrastructure, and ensures data quality across the organization",
    pictureLink="https://example.com/images/data-engineering-logo.png",
    slack="data-engineering",
    email="data-eng@example.com",
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=group_urn,
    aspect=editable_info,
)
emitter.emit(metadata_event)

print(f"Updated editable info for group: {group_urn}")

admin_urn = str(CorpUserUrn("jdoe"))

ownership = OwnershipClass(
    owners=[
        OwnerClass(
            owner=admin_urn,
            type=OwnershipTypeClass.TECHNICAL_OWNER,
        )
    ]
)

metadata_event = MetadataChangeProposalWrapper(
    entityUrn=group_urn,
    aspect=ownership,
)
emitter.emit(metadata_event)

print(f"Added {admin_urn} as owner of group: {group_urn}")
