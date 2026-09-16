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
