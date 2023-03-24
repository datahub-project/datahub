from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Callable, Iterable, List, Optional, Union

import pydantic
from pydantic import BaseModel

import datahub.emitter.mce_builder as builder
from datahub.api.entities.corpuser.corpuser import CorpUser, CorpUserGenerationConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    CorpGroupEditableInfoClass,
    CorpGroupInfoClass,
    GroupMembershipClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    _Aspect,
)

if TYPE_CHECKING:
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter

logger = logging.getLogger(__name__)


@dataclass
class CorpGroupGenerationConfig:
    """
    A container for configuration for generation of mcp-s from CorpGroup instances
    """

    override_editable: bool = False
    datahub_graph: Optional[DataHubGraph] = None


class CorpGroup(BaseModel):
    """This is a CorpGroup class which represents a CorpGroup

    Args:
        id (str): The id of the group
        display_name (Optional[str]): The name of the group
        email (Optional[str]): email of this group
        description (Optional[str]): A description of the group
        overrideEditable (bool): If True, group information that is editable in the UI will be overridden
        picture_link (Optional[str]): A URL which points to a picture which user wants to set as the photo for the group
        slack (Optional[str]): Slack channel for the group
        admins (List[Union[str, CorpUser]]): A list of administrator ids (or urns) for the group. You can also provide the user record for the admin inline within this section
        members (List[Union[str, CorpUser]]): A list of member ids (or urns) for the group.
    """

    id: str

    # These are for CorpGroupInfo
    display_name: Optional[str] = None
    email: Optional[str] = None
    description: Optional[str] = None

    # These are for CorpGroupEditableInfo
    overrideEditable: bool = False
    picture_link: Optional[str] = None
    slack: Optional[str] = None
    admins: List[Union[str, CorpUser]] = []
    members: List[Union[str, CorpUser]] = []

    @pydantic.validator("admins", "members", each_item=True)
    def make_urn_if_needed(v):
        if isinstance(v, str):
            return builder.make_user_urn(v)
        return v

    @property
    def urn(self):
        return builder.make_group_urn(self.id)

    def _needs_editable_aspect(self) -> bool:
        return bool(self.picture_link)

    def generate_mcp(
        self, generation_config: CorpGroupGenerationConfig = CorpGroupGenerationConfig()
    ) -> Iterable[MetadataChangeProposalWrapper]:
        urns_created = set()  # dedup member creation on the way out
        members_to_create: List[CorpUser] = (
            [u for u in self.members if isinstance(u, CorpUser)] if self.members else []
        )
        admins_to_create: List[CorpUser] = (
            [u for u in self.admins if isinstance(u, CorpUser)] if self.admins else []
        )

        member_urns: List[str] = (
            [u.urn if isinstance(u, CorpUser) else u for u in self.members]
            if self.members
            else []
        )
        admin_urns: List[str] = (
            [u.urn if isinstance(u, CorpUser) else u for u in self.admins]
            if self.admins
            else []
        )

        for m in members_to_create + admins_to_create:
            if m.urn not in urns_created:
                yield from m.generate_mcp(
                    generation_config=CorpUserGenerationConfig(
                        override_editable=generation_config.override_editable
                    )
                )
                urns_created.add(m.urn)
            else:
                logger.warn(
                    f"Supressing emission of member {m.urn} before we already emitted metadata for it"
                )

        aspects: List[_Aspect] = [StatusClass(removed=False)]
        if generation_config.override_editable:
            aspects.append(
                CorpGroupEditableInfoClass(
                    description=self.description,
                    pictureLink=self.picture_link,
                    slack=self.slack,
                    email=self.email,
                )
            )
        else:
            aspects.append(
                CorpGroupInfoClass(
                    admins=admin_urns,  # deprecated but we fill it out for consistency
                    members=member_urns,  # deprecated but we fill it out for consistency
                    groups=[],  # deprecated
                    displayName=self.display_name,
                    email=self.email,
                    description=self.description,
                    slack=self.slack,
                )
            )
            # picture link is only available in the editable aspect, so we have to use it if it is provided
            if self._needs_editable_aspect():
                aspects.append(
                    CorpGroupEditableInfoClass(
                        description=self.description,
                        pictureLink=self.picture_link,
                        slack=self.slack,
                        email=self.email,
                    )
                )
        for aspect in aspects:
            yield MetadataChangeProposalWrapper(entityUrn=self.urn, aspect=aspect)

        # Unfortunately, admins and members fields in CorpGroupInfo has been deprecated
        # So we need to emit Ownership and GroupMembership oriented to the individual users
        # TODO: Move this to PATCH MCP-s once these aspects are supported via patch.
        if generation_config.datahub_graph is not None:
            datahub_graph = generation_config.datahub_graph
            for urn in admin_urns:
                ownership = datahub_graph.get_aspect(
                    urn, OwnershipClass
                ) or OwnershipClass(owners=[])
                if self.urn not in [owner.owner for owner in ownership.owners]:
                    ownership.owners = ownership.owners + [
                        OwnerClass(owner=urn, type=OwnershipTypeClass.TECHNICAL_OWNER)
                    ]
                    yield MetadataChangeProposalWrapper(
                        entityUrn=self.urn, aspect=ownership
                    )

            for urn in member_urns:
                group_membership = datahub_graph.get_aspect(
                    urn, GroupMembershipClass
                ) or GroupMembershipClass(groups=[])
                if self.urn not in group_membership.groups:
                    group_membership.groups = sorted(
                        set(group_membership.groups + [self.urn])
                    )
                    yield MetadataChangeProposalWrapper(
                        entityUrn=urn, aspect=group_membership
                    )
        else:
            if admin_urns or member_urns:
                raise Exception(
                    "Unable to emit group ownership because admins or members are non-empty, and a DataHubGraph instance was not provided."
                )

        # emit status aspects for all user urns referenced (to ensure they get created)
        for urn in set(admin_urns).union(set(member_urns)):
            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=StatusClass(removed=False)
            )

    @lru_cache(maxsize=32)
    def _datahub_graph_from_datahub_rest_emitter(
        self, rest_emitter: DatahubRestEmitter
    ) -> DataHubGraph:
        """
        Create a datahub graph instance from a REST Emitter.
        A stop-gap implementation which is expected to be removed after PATCH support is implemented
        for membership updates for users <-> groups
        """
        graph = DataHubGraph(
            config=DatahubClientConfig(
                server=rest_emitter._gms_server,
                token=rest_emitter._token,
                timeout_sec=rest_emitter._connect_timeout_sec,
                retry_status_codes=rest_emitter._retry_status_codes,
                extra_headers=rest_emitter._session.headers,
                disable_ssl_verification=rest_emitter._session.verify is False,
            )
        )
        return graph

    def emit(
        self,
        emitter: Union[DatahubRestEmitter, "DatahubKafkaEmitter"],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        A convenience method to emit the CorpGroup entity to DataHub using an emitter.
        See also: generate_mcp to have finer grain control over mcp routing

        :param emitter: Datahub Emitter to emit the proccess event
        :param callback: The callback method for KafkaEmitter if it is used
        """
        datahub_graph = emitter if isinstance(emitter, DataHubGraph) else None
        if not datahub_graph:
            if isinstance(emitter, DatahubRestEmitter):
                # create a datahub graph instance from the emitter
                # this code path exists mainly for backwards compatibility with existing callers
                # who are passing in a DataHubRestEmitter today
                # we won't need this in the future once PATCH support is implemented as all emitters
                # will work
                datahub_graph = self._datahub_graph_from_datahub_rest_emitter(emitter)
        for mcp in self.generate_mcp(
            generation_config=CorpGroupGenerationConfig(
                override_editable=self.overrideEditable, datahub_graph=datahub_graph
            )
        ):
            emitter.emit(mcp, callback)
