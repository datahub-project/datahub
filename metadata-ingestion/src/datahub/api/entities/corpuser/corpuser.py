from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

import pydantic

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    CorpUserEditableInfoClass,
    CorpUserInfoClass,
    GroupMembershipClass,
    StatusClass,
)


@dataclass
class CorpUserGenerationConfig:
    """
    A holder for configuration for MCP generation from CorpUser objects
    """

    override_editable: bool = False


class CorpUser(ConfigModel):
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
        description (Optional[str]): A description string for the user
        slack (Optional[str]): Slack handle for the user
        picture_link (Optional[str]): A resolvable url for the user's picture icon
        phone (Optional(str)): A phone number for the user
    """

    id: str
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
    groups: Optional[List[str]] = None
    description: Optional[str] = None
    slack: Optional[str] = None
    picture_link: Optional[str] = None
    phone: Optional[str] = None

    @pydantic.validator("full_name", always=True)
    def full_name_can_be_built_from_first_name_last_name(v, values):
        if not v:
            if "first_name" in values or "last_name" in values:
                first_name = values.get("first_name") or ""
                last_name = values.get("last_name") or ""
                full_name = f"{first_name} {last_name}" if last_name else first_name
                return full_name
        else:
            return v

    @property
    def urn(self):
        return builder.make_user_urn(self.id)

    def _needs_editable_aspect(self) -> bool:
        return (bool)(self.slack or self.description or self.picture_link or self.phone)

    def generate_group_membership_aspect(self) -> Iterable[GroupMembershipClass]:
        if self.groups is not None:
            group_membership = GroupMembershipClass(
                groups=[builder.make_group_urn(group) for group in self.groups]
            )
            return [group_membership]
        else:
            return []

    def generate_mcp(
        self, generation_config: CorpUserGenerationConfig = CorpUserGenerationConfig()
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if generation_config.override_editable or self._needs_editable_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.urn,
                aspect=CorpUserEditableInfoClass(
                    aboutMe=self.description,
                    pictureLink=self.picture_link,
                    displayName=self.full_name,
                    slack=self.slack,
                    email=self.email,
                    phone=self.phone,
                ),
            )
            yield mcp
        else:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
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
            )
            yield mcp

        for group_membership in self.generate_group_membership_aspect():
            mcp = MetadataChangeProposalWrapper(
                entityUrn=str(self.urn),
                aspect=group_membership,
            )
            yield mcp

        # Finally emit status
        yield MetadataChangeProposalWrapper(
            entityUrn=self.urn, aspect=StatusClass(removed=False)
        )

    def emit(
        self,
        emitter: Emitter,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        Emit the CorpUser entity to Datahub

        :param emitter: Datahub Emitter to emit the proccess event
        :param callback: The callback method for KafkaEmitter if it is used
        """
        for mcp in self.generate_mcp():
            emitter.emit(mcp, callback)
