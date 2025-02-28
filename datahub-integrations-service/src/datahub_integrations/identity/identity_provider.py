import logging
from dataclasses import dataclass
from typing import Dict, Optional, Set, Union

from datahub.ingestion.graph.client import AspectBag, DataHubGraph
from datahub.metadata.schema_classes import (
    CorpGroupEditableInfoClass,
    CorpGroupInfoClass,
    CorpUserEditableInfoClass,
    CorpUserInfoClass,
    CorpUserSettingsClass,
    CorpUserStatusClass,
)

logger = logging.getLogger(__name__)


@dataclass
class User:
    urn: str
    displayName: Optional[str] = None
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    slack: Optional[str] = None
    title: Optional[str] = None
    isActive: bool = False

    def get_resolved_display_name(self) -> str:
        if self.displayName:
            return self.displayName
        elif self.firstName and self.lastName:
            return f"{self.firstName} {self.lastName}"
        else:
            return self.urn.split(":")[-1]


@dataclass
class Group:
    urn: str
    displayName: Optional[str] = None
    email: Optional[str] = None
    slack: Optional[str] = None

    def get_resolved_display_name(self) -> str:
        if self.displayName:
            return self.displayName
        else:
            return self.urn.split(":")[-1]


class IdentityProvider:
    def __init__(self, graph: DataHubGraph):
        self.graph = graph

    def batch_get_actors(self, actor_urns: Set[str]) -> Dict[str, Union[User, Group]]:
        actors: Dict[str, Union[User, Group]] = {}
        try:
            for actor_urn in actor_urns:
                aspect_bag = self.graph.get_entity_semityped(actor_urn)
                actor: Optional[Union[User, Group]] = None
                if actor_urn.startswith("urn:li:corpuser:"):
                    actor = self.map_to_user(actor_urn, aspect_bag)
                elif actor_urn.startswith("urn:li:corpGroup:"):
                    actor = self.map_to_group(actor_urn, aspect_bag)
                else:
                    logger.warning(f"Unknown actor urn: {actor_urn}")
                    continue
                if actor:
                    actors[actor_urn] = actor
        except Exception as e:
            logger.error(f"Failed to batch get actors: {actor_urns}", exc_info=e)
        return actors

    def get_user(self, user_urn: str) -> Optional[User]:
        users = self.batch_get_users({user_urn})
        return users.get(user_urn)

    def batch_get_users(self, user_urns: Set[str]) -> Dict[str, User]:
        users: Dict[str, User] = {}
        try:
            for user_urn in user_urns:
                aspect_bag = self.graph.get_entity_semityped(user_urn)
                user = self.map_to_user(user_urn, aspect_bag)
                if user:
                    users[user_urn] = user
        except Exception as e:
            logger.error(f"Failed to batch get users: {user_urns}", exc_info=e)
        return users

    def map_to_user(self, user_urn: str, aspect_bag: AspectBag) -> Optional[User]:
        user = User(user_urn)

        corp_user_info: Optional[CorpUserInfoClass] = aspect_bag.get("corpUserInfo")

        if corp_user_info:
            user.displayName = corp_user_info.get("displayName")
            user.firstName = corp_user_info.get("firstName")
            user.lastName = corp_user_info.get("lastName")
            user.email = corp_user_info.get("email")

        corp_user_editable_info: Optional[CorpUserEditableInfoClass] = aspect_bag.get(
            "corpUserEditableInfo"
        )
        if corp_user_editable_info:
            user.displayName = (
                corp_user_editable_info.get("displayName") or user.displayName
            )
            user.email = corp_user_editable_info.get("email") or user.email
            user.phone = corp_user_editable_info.get("phone") or user.phone
            user.title = corp_user_editable_info.get("title") or user.title
            user.slack = corp_user_editable_info.get("slack") or user.slack

        corp_user_settings: Optional[CorpUserSettingsClass] = aspect_bag.get(
            "corpUserSettings"
        )
        if corp_user_settings:
            notification_settings = corp_user_settings.notificationSettings
            if notification_settings:
                slack_settings = notification_settings.slackSettings
                if slack_settings:
                    user_handle = slack_settings.userHandle
                    if user_handle:
                        user.slack = user_handle

        corp_user_status: Optional[CorpUserStatusClass] = aspect_bag.get(
            "corpUserStatus"
        )
        if corp_user_status:
            user.isActive = corp_user_status.status == "ACTIVE"

        return user

    def get_group(self, group_urn: str) -> Optional[Group]:
        groups = self.batch_get_groups({group_urn})
        return groups.get(group_urn)

    def batch_get_groups(self, group_urns: Set[str]) -> Dict[str, Group]:
        groups: Dict[str, Group] = {}
        try:
            for group_urn in group_urns:
                aspect_bag = self.graph.get_entity_semityped(group_urn)
                group = self.map_to_group(group_urn, aspect_bag)
                if group:
                    groups[group_urn] = group
        except Exception as e:
            logger.error(f"Failed to batch get group: {group_urns}", exc_info=e)
        return groups

    def map_to_group(self, group_urn: str, aspect_bag: AspectBag) -> Optional[Group]:
        group = Group(group_urn)

        corp_group_info: Optional[CorpGroupInfoClass] = aspect_bag.get("corpGroupInfo")
        if corp_group_info:
            group.displayName = corp_group_info.get("displayName")
            group.email = corp_group_info.get("email")
            group.slack = corp_group_info.get("slack")

        corp_group_editable_info: Optional[CorpGroupEditableInfoClass] = aspect_bag.get(
            "corpGroupEditableInfo"
        )
        if corp_group_editable_info:
            group.email = corp_group_editable_info.get("email") or group.email
            group.slack = corp_group_editable_info.get("slack") or group.slack

        return group
