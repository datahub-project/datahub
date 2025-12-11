# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/corpgroup_add_members.py
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import GroupMembershipClass
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn

graph = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

group_urn = str(CorpGroupUrn("data-engineering"))

users_to_add = [
    CorpUserUrn("jdoe"),
    CorpUserUrn("asmith"),
    CorpUserUrn("bwilliams"),
]

for user_urn in users_to_add:
    user_urn_str = str(user_urn)

    current_membership = graph.get_aspect(user_urn_str, GroupMembershipClass)

    if current_membership is None:
        current_membership = GroupMembershipClass(groups=[])

    if group_urn not in current_membership.groups:
        current_membership.groups.append(group_urn)

        metadata_event = MetadataChangeProposalWrapper(
            entityUrn=user_urn_str,
            aspect=current_membership,
        )
        emitter.emit(metadata_event)

        print(f"Added {user_urn_str} to group {group_urn}")
    else:
        print(f"{user_urn_str} is already a member of {group_urn}")
