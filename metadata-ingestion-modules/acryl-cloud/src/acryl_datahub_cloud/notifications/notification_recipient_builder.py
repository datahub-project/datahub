import logging
from typing import Any, Dict, List

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    CorpGroupSettingsClass,
    CorpUserSettingsClass,
    NotificationRecipientClass,
    NotificationRecipientTypeClass,
    NotificationSettingsClass,
    NotificationSinkTypeClass,
)

logger = logging.getLogger(__name__)


class NotificationRecipientBuilder:
    """Class to assist in building notification recipients for all notification scenarios"""

    def __init__(self, graph: DataHubGraph):
        self.graph: DataHubGraph = graph
        self.user_settings_map: Dict[str, CorpUserSettingsClass] = {}
        self.group_settings_map: Dict[str, CorpGroupSettingsClass] = {}

    def convert_recipients_to_json_objects(
        self, recipients: List[NotificationRecipientClass]
    ) -> List[Dict[str, Any]]:
        converted_recipients: List[Dict[str, Any]] = []

        for r in recipients:
            recipient: Dict[str, Any] = {}
            recipient["type"] = r.type
            recipient["id"] = r.id
            recipient["actor"] = r.actor
            converted_recipients.append(recipient)

        return converted_recipients

    def build_actor_recipients(
        self,
        actor_urns: List[str],
        notification_scenario_type: str,
        is_default_enabled: bool,
    ) -> List[NotificationRecipientClass]:
        self.populate_user_settings_map(actor_urns)
        self.populate_group_settings_map(actor_urns)

        recipients: List[NotificationRecipientClass] = []

        recipients.extend(
            self.get_slack_recipients(
                actor_urns, notification_scenario_type, is_default_enabled
            )
        )
        recipients.extend(
            self.get_email_recipients(
                actor_urns, notification_scenario_type, is_default_enabled
            )
        )

        return recipients

    def populate_user_settings_map(self, actor_urns: List[str]) -> None:
        new_actors = [
            urn
            for urn in actor_urns
            if urn not in self.user_settings_map and self.is_user(urn)
        ]

        entities = self.graph.get_entities("corpuser", new_actors, ["corpUserSettings"])
        for urn, entity in entities.items():
            user_tuple = entity.get(CorpUserSettingsClass.ASPECT_NAME, (None, None))
            if user_tuple and user_tuple[0]:
                if not isinstance(user_tuple[0], CorpUserSettingsClass):
                    logger.error(
                        f"{user_tuple[0]} is not of type CorpUserSettings for urn: {urn}"
                    )
                else:
                    self.user_settings_map[urn] = user_tuple[0]

    def populate_group_settings_map(self, actor_urns: List[str]) -> None:
        new_actors = [
            urn
            for urn in actor_urns
            if urn not in self.group_settings_map and self.is_group(urn)
        ]

        entities = self.graph.get_entities(
            "corpGroup", new_actors, ["corpGroupSettings"]
        )
        for urn, entity in entities.items():
            group_tuple = entity.get(CorpGroupSettingsClass.ASPECT_NAME, (None, None))
            if group_tuple and group_tuple[0]:
                if not isinstance(group_tuple[0], CorpGroupSettingsClass):
                    logger.error(
                        f"{group_tuple[0]} is not of type CorpGroupSettings for urn: {urn}"
                    )
                else:
                    self.group_settings_map[urn] = group_tuple[0]

    def is_user(self, actor_urn: str) -> bool:
        return actor_urn.startswith("urn:li:corpuser")

    def is_group(self, actor_urn: str) -> bool:
        return actor_urn.startswith("urn:li:corpGroup")

    def get_slack_recipients(
        self,
        actor_urns: List[str],
        notification_scenario_type: str,
        is_default_enabled: bool,
    ) -> List[NotificationRecipientClass]:
        slack_recipients: List[NotificationRecipientClass] = []

        for actor_urn in actor_urns:
            actor_settings = (
                self.user_settings_map.get(actor_urn)
                if self.is_user(actor_urn)
                else self.group_settings_map.get(actor_urn)
            )
            # ensure slack is enabled in the user settings in general
            if (
                actor_settings is not None
                and actor_settings.notificationSettings is not None
                and NotificationSinkTypeClass.SLACK
                in actor_settings.notificationSettings.sinkTypes
            ):
                notification_settings = actor_settings.notificationSettings
                if self.is_notification_enabled(
                    "slack.enabled",
                    notification_settings,
                    notification_scenario_type,
                    is_default_enabled,
                ):
                    recipients = self.build_slack_recipient_objects(
                        actor_urn, notification_scenario_type
                    )
                    if recipients is not None:
                        slack_recipients.extend(recipients)

        return slack_recipients

    def get_email_recipients(
        self,
        actor_urns: List[str],
        notification_scenario_type: str,
        is_default_enabled: bool,
    ) -> List[NotificationRecipientClass]:
        email_recipients: List[NotificationRecipientClass] = []

        for actor_urn in actor_urns:
            actor_settings = (
                self.user_settings_map.get(actor_urn)
                if self.is_user(actor_urn)
                else self.group_settings_map.get(actor_urn)
            )
            # ensure email is enabled in the user settings in general
            if (
                actor_settings is not None
                and actor_settings.notificationSettings is not None
                and NotificationSinkTypeClass.EMAIL
                in actor_settings.notificationSettings.sinkTypes
            ):
                notification_settings = actor_settings.notificationSettings
                if self.is_notification_enabled(
                    "email.enabled",
                    notification_settings,
                    notification_scenario_type,
                    is_default_enabled,
                ):
                    recipient = self.build_email_recipient_object(
                        actor_urn, notification_scenario_type
                    )
                    if recipient is not None:
                        email_recipients.append(recipient)

        return email_recipients

    def is_notification_enabled(
        self,
        sink_key: str,
        notification_settings: NotificationSettingsClass,
        notification_scenario_type: str,
        is_default_enabled: bool,
    ) -> bool:
        """
        Returns if a given scenario type is enabled for a given sink.
        If a notification is default enabled, it will be true if this scenario is not explicitly turned off.
        """
        if notification_settings.settings is not None:
            scenario_settings = notification_settings.settings.get(
                notification_scenario_type
            )
            if (
                scenario_settings is not None
                and scenario_settings.params is not None
                and sink_key in scenario_settings.params
            ):
                is_enabled = scenario_settings.params.get(sink_key)
                if is_enabled == "true" or (is_enabled is None and is_default_enabled):
                    return True
            elif is_default_enabled:
                return True
        elif is_default_enabled:
            return True

        return False

    def build_slack_recipient_objects(
        self, actor_urn: str, notification_scenario_type: str
    ) -> List[NotificationRecipientClass] | None:
        if actor_urn.startswith("urn:li:corpuser"):
            user_recipient = self.build_user_slack_recipient_object(
                actor_urn, notification_scenario_type
            )
            if user_recipient:
                return [user_recipient]
        elif actor_urn.startswith("urn:li:corpGroup"):
            return self.build_group_slack_recipient_objects(
                actor_urn, notification_scenario_type
            )

        return None

    def build_user_slack_recipient_object(
        self, actor_urn: str, notification_scenario_type: str
    ) -> NotificationRecipientClass | None:
        notification_settings = self.get_user_notification_settings(actor_urn)
        if notification_settings is None:
            return None

        slack = self.get_user_slack_for_notification_type(
            notification_settings, notification_scenario_type
        )
        if slack is None:
            return None

        return NotificationRecipientClass(
            type=NotificationRecipientTypeClass.SLACK_DM, id=slack, actor=actor_urn
        )

    def get_user_slack_for_notification_type(
        self,
        notification_settings: NotificationSettingsClass,
        notification_scenario_type: str,
    ) -> str | None:
        custom_slack = self.get_custom_id_for_scenario(
            notification_settings, notification_scenario_type, "slack.channel"
        )
        if custom_slack is not None:
            return custom_slack

        slack_settings = notification_settings.slackSettings
        if slack_settings is not None and slack_settings.userHandle is not None:
            return slack_settings.userHandle

        return None

    def build_group_slack_recipient_objects(
        self, actor_urn: str, notification_scenario_type: str
    ) -> List[NotificationRecipientClass] | None:
        notification_settings = self.get_group_notification_settings(actor_urn)
        if notification_settings is None:
            return None

        slacks = self.get_group_slacks_for_notification_type(
            notification_settings, notification_scenario_type
        )
        if slacks is None:
            return None

        recipients = []
        for slack in slacks:
            recipients.append(
                NotificationRecipientClass(
                    type=NotificationRecipientTypeClass.SLACK_CHANNEL,
                    id=slack,
                    actor=actor_urn,
                )
            )

        return recipients

    def get_group_slacks_for_notification_type(
        self,
        notification_settings: NotificationSettingsClass,
        notification_scenario_type: str,
    ) -> List[str] | None:
        custom_slack = self.get_custom_id_for_scenario(
            notification_settings, notification_scenario_type, "slack.channel"
        )
        if custom_slack is not None:
            return [custom_slack]

        slack_settings = notification_settings.slackSettings
        if slack_settings is not None and slack_settings.channels is not None:
            return slack_settings.channels

        return None

    def build_email_recipient_object(
        self, actor_urn: str, notification_scenario_type: str
    ) -> NotificationRecipientClass | None:
        email = self.get_email_for_notification_type(
            actor_urn, notification_scenario_type
        )
        if email is None:
            return None

        return NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL, id=email, actor=actor_urn
        )

    def get_email_for_notification_type(
        self, actor_urn: str, notification_scenario_type: str
    ) -> str | None:
        notification_settings = (
            self.get_user_notification_settings(actor_urn)
            if self.is_user(actor_urn)
            else self.get_group_notification_settings(actor_urn)
        )
        if notification_settings is None:
            return None

        custom_email = self.get_custom_id_for_scenario(
            notification_settings, notification_scenario_type, "email.address"
        )
        if custom_email is not None:
            return custom_email

        email_settings = notification_settings.emailSettings
        if email_settings is not None and email_settings.email is not None:
            return email_settings.email

        return None

    def get_user_notification_settings(
        self, user_urn: str
    ) -> NotificationSettingsClass | None:
        user_settings = self.user_settings_map.get(user_urn)
        if user_settings is None:
            logger.warning(f"User settings not populdated for user with urn {user_urn}")
            return None

        notification_settings = user_settings.notificationSettings
        if notification_settings is None:
            logger.warning(
                f"User notification settings not populdated for user with urn {user_urn}"
            )
            return None

        return notification_settings

    def get_group_notification_settings(
        self, group_urn: str
    ) -> NotificationSettingsClass | None:
        group_settings = self.group_settings_map.get(group_urn)
        if group_settings is None:
            logger.warning(
                f"Group settings not populdated for group with urn {group_urn}"
            )
            return None

        notification_settings = group_settings.notificationSettings
        if notification_settings is None:
            logger.warning(
                f"Group notification settings not populdated for group with urn {group_urn}"
            )
            return None

        return notification_settings

    def get_custom_id_for_scenario(
        self,
        notification_settings: NotificationSettingsClass,
        notification_scenario_type: str,
        param_key: str,
    ) -> str | None:
        """
        We support storing a custom ID for a given sink type for specific notification scenarios in settings.
        This method takes in the type and finds if there is a custom ID.
        For example, you may want to send notifications about incidents to a different email than your
        default email.
        """
        scenario_settings = notification_settings.settings
        if (
            scenario_settings is not None
            and (setting := scenario_settings.get(notification_scenario_type))
            is not None
        ):
            params = setting.params
            if (
                params is not None
                and (custom_id := params.get(param_key)) is not None
                and custom_id
            ):
                return custom_id

        return None
