from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional, Union, cast

import datahub.emitter.mce_builder as builder
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    CorpGroupEditableInfoClass,
    CorpGroupInfoClass,
)


@dataclass
class CorpGroup:
    """This is a CorpGroup class which represents a CorpGroup

    Args:
        id (str): The id of the group
        display_name (Optional[str]): The name of the group
        email (Optional[str]): email of this group
        description (Optional[str]): A description of the group
        overrideEditable (bool): If True, group information that is editable in the UI will be overridden
        picture_link (Optional[str]): A URL which points to a picture which user wants to set as the photo for the group
        slack (Optional[str]): Slack channel for the group
    """

    id: str
    urn: str = field(init=False)

    # These are for CorpGroupInfo
    display_name: Optional[str] = None
    email: Optional[str] = None
    description: Optional[str] = None

    # These are for CorpGroupEditableInfo
    overrideEditable: bool = False
    picture_link: Optional[str] = None
    slack: Optional[str] = None

    def __post_init__(self):
        self.urn = builder.make_group_urn(self.id)

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        if self.overrideEditable:
            mcp = MetadataChangeProposalWrapper(
                entityType="corpgroup",
                entityUrn=str(self.urn),
                aspectName="corpGroupEditableInfo",
                aspect=CorpGroupEditableInfoClass(
                    description=self.description,
                    pictureLink=self.picture_link,
                    slack=self.slack,
                    email=self.email,
                ),
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

        mcp = MetadataChangeProposalWrapper(
            entityType="corpgroup",
            entityUrn=str(self.urn),
            aspectName="corpGroupInfo",
            aspect=CorpGroupInfoClass(
                admins=[],  # Deprecated, replaced by Ownership aspect
                members=[],  # Deprecated, replaced by GroupMembership aspect
                groups=[],  # Deprecated, this field is unused
                displayName=self.display_name,
                email=self.email,
                description=self.description,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

    def emit(
        self,
        emitter: Union[DatahubRestEmitter, DatahubKafkaEmitter],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        Emit the CorpGroup entity to Datahub

        :param emitter: Datahub Emitter to emit the proccess event
        :param callback: The callback method for KafkaEmitter if it is used
        """
        for mcp in self.generate_mcp():
            if type(emitter).__name__ == "DatahubKafkaEmitter":
                assert callback is not None
                kafka_emitter = cast("DatahubKafkaEmitter", emitter)
                kafka_emitter.emit(mcp, callback)
            else:
                rest_emitter = cast("DatahubRestEmitter", emitter)
                rest_emitter.emit(mcp)
