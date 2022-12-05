from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional, Union

import datahub.emitter.mce_builder as builder
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    CorpUserInfoClass,
    GroupMembershipClass,
)


@dataclass
class CorpUser:
    """This is a CorpUser class which represents a CorpUser

    Args:
        id (str): The id of the user
        display_name (Optional[str]): The name of the user to display in the UI
        email (Optional[str]): email address of this user
        title (Optional[str]): title of this user
        manager_urn (Optional[str]): direct manager of this user
        department_id (Optional[int]): department id this user belongs to
        department_name (Optional[str]): department name this user belongs to
        first_name (Optional[str]): first name of this user
        last_name (Optional[str]): last name of this user
        full_name (Optional[str]): Common name of this user, format is firstName + lastName (split by a whitespace)
        country_code (Optional[str]): two uppercase letters country code. e.g.  US
        groups (List[str]): List of group ids the user belongs to
    """

    id: str
    urn: str = field(init=False)
    display_name: Optional[str] = None
    email: Optional[str] = None
    title: Optional[str] = None
    manager_urn: Optional[str] = None
    department_id: Optional[int] = None
    department_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    country_code: Optional[str] = None
    groups: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.urn = builder.make_user_urn(self.id)

    def generate_group_membership_aspect(self) -> Iterable[GroupMembershipClass]:
        group_membership = GroupMembershipClass(
            groups=[builder.make_group_urn(group) for group in self.groups]
        )
        return [group_membership]

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        mcp = MetadataChangeProposalWrapper(
            entityType="corpuser",
            entityUrn=str(self.urn),
            aspectName="corpUserInfo",
            aspect=CorpUserInfoClass(
                active=True,  # Deprecated, use CorpUserStatus instead.
                displayName=self.display_name,
                email=self.email,
                title=self.title,
                managerUrn=self.manager_urn,
                departmentId=self.department_id,
                departmentName=self.department_name,
                firstName=self.first_name,
                lastName=self.last_name,
                fullName=self.full_name,
                countryCode=self.country_code,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

        for group_membership in self.generate_group_membership_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityType="corpuser",
                entityUrn=str(self.urn),
                aspectName="groupMembership",
                aspect=group_membership,
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

    def emit(
        self,
        emitter: Union[DatahubRestEmitter, DatahubKafkaEmitter],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        Emit the CorpUser entity to Datahub

        :param emitter: Datahub Emitter to emit the proccess event
        :param callback: The callback method for KafkaEmitter if it is used
        """
        for mcp in self.generate_mcp():
            emitter.emit(mcp, callback)
