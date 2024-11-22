from datetime import datetime, timedelta
from enum import Enum
from typing import Any, List, Optional

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    IncidentNotificationDetailsClass,
    MetadataChangeProposalClass,
    NotificationRecipientClass,
    NotificationRequestClass,
    NotificationSinkTypeClass,
    NotificationTemplateTypeClass,
)
from loguru import logger
from slack_sdk import WebClient

from datahub_integrations.app import graph
from datahub_integrations.identity.identity_provider import IdentityProvider
from datahub_integrations.notifications.constants import (
    DATAHUB_BASE_URL,
    MAX_NOTIFICATION_RETRIES,
    STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED,
)
from datahub_integrations.notifications.sinks.context import NotificationContext
from datahub_integrations.notifications.sinks.sink import NotificationSink
from datahub_integrations.notifications.sinks.slack.send_slack_message import (
    send_notification_to_recipients,
    update_messages,
)
from datahub_integrations.notifications.sinks.slack.template_utils import (
    build_incident_message,
    build_incident_status_change_message,
)
from datahub_integrations.notifications.sinks.slack.types import SlackMessageDetails
from datahub_integrations.notifications.sinks.utils import retry_with_backoff
from datahub_integrations.slack.config import SLACK_PROXY, SlackConnection, slack_config


class RetryMode(Enum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class SlackNotificationSink(NotificationSink):
    """
    A Slack notification sink.
    """

    last_credentials_refresh_attempt: Optional[datetime] = None
    base_url: str
    slack_client: WebClient
    slack_connection_config: Optional[SlackConnection] = None
    identity_provider: IdentityProvider
    graph: DataHubGraph

    def type(self) -> str:
        return NotificationSinkTypeClass.SLACK

    def init(self) -> None:
        # Init Slack client.
        self.base_url = DATAHUB_BASE_URL
        self.graph = graph
        self.identity_provider = IdentityProvider(graph)

    def _check_is_slack_config_valid(self, config: Optional[SlackConnection]) -> bool:
        return config is not None and config.bot_token is not None

    def send(
        self, request: NotificationRequestClass, context: NotificationContext
    ) -> None:
        # Maybe reload slack creds if they are stale.
        self._maybe_reload_web_client()

        if not self._check_is_slack_config_valid(self.slack_connection_config):
            logger.warning(
                "Slack config is invalid. Skipping sending notification request."
            )
            return

        template_type: str = str(request.message.template)

        # Mapping template types to functions
        action_map = {
            NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT: lambda: self._send_new_incident_notification(
                request
            ),
            NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT_UPDATE: lambda: self._update_new_incident_notifications(
                request
            ),
            NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE: lambda: self._send_incident_status_change_notification(
                request
            ),
        }

        # Execute the corresponding function or raise an exception for unsupported types
        if template_type in action_map:
            message_details = action_map[template_type]() or []
            if self._should_save_message_details(template_type):
                # Save message details
                self._save_message_details(request, template_type, message_details)
        else:
            logger.warning(
                f"Unsupported template type {template_type} provided. Not sending notification."
            )

    def _send_new_incident_notification(
        self, request: NotificationRequestClass
    ) -> List[SlackMessageDetails]:
        text, blocks, attachments = build_incident_message(
            request, self.identity_provider, self.slack_client, self.base_url
        )

        return self._send_change_notification(
            request.recipients,
            text,
            blocks,
            attachments,
            RetryMode.ENABLED,
        )

    def _update_new_incident_notifications(
        self, request: NotificationRequestClass
    ) -> None:
        # First, try to find the messages that need to be updated for the recipients.
        message_details = self._get_saved_message_details(request)

        # Build the new message block.
        text, blocks, attachments = build_incident_message(
            request, self.identity_provider, self.slack_client, self.base_url
        )

        self._update_messages(
            message_details,
            text,
            blocks,
            attachments,
            RetryMode.ENABLED,
        )

    def _send_incident_status_change_notification(
        self, request: NotificationRequestClass
    ) -> List[SlackMessageDetails]:
        text, blocks, attachments = build_incident_status_change_message(
            request, self.identity_provider, self.slack_client, self.base_url
        )

        return self._send_change_notification(
            request.recipients,
            text,
            blocks,
            attachments,
            RetryMode.ENABLED,
        )

    def _send_change_notification(
        self,
        recipients: List[NotificationRecipientClass],
        text: str,
        blocks: Optional[Any],
        attachments: Optional[Any],
        retry_mode: RetryMode,
    ) -> List[SlackMessageDetails]:
        max_attempts = (
            MAX_NOTIFICATION_RETRIES if retry_mode == RetryMode.ENABLED else 1
        )
        try:
            return retry_with_backoff(
                send_notification_to_recipients,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                client=self.slack_client,
                recipients=recipients,
                text=text,
                blocks=blocks,
                attachments=attachments,
            )
        except Exception as e:
            logger.exception(
                f"Failed to send notification after {max_attempts} attempts. Error: {e}"
            )
            return []

    def _update_messages(
        self,
        message_details: List[SlackMessageDetails],
        text: str,
        blocks: Optional[Any],
        attachments: Optional[Any],
        retry_mode: RetryMode,
    ) -> None:
        max_attempts = (
            MAX_NOTIFICATION_RETRIES if retry_mode == RetryMode.ENABLED else 1
        )
        try:
            return retry_with_backoff(
                update_messages,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                client=self.slack_client,
                message_details=message_details,
                text=text,
                blocks=blocks,
                attachments=attachments,
            )
        except Exception as e:
            logger.exception(
                f"Failed to update messages after {max_attempts} attempts. Error: {e}"
            )

    def _maybe_reload_web_client(self) -> None:
        """Reloads the WebClient if the last reload was more than 5 minutes ago."""
        current_time = datetime.now()
        if self.last_credentials_refresh_attempt is None or (
            current_time - self.last_credentials_refresh_attempt
        ) > timedelta(minutes=5):
            self._reload_web_client()

    def _reload_web_client(self) -> None:
        try:
            retry_with_backoff(self._try_reload_web_client, max_attempts=3)
        except Exception as e:
            logger.exception(
                "Failed to reload Slack web client! This means slack configs may not be updated.",
                e,
            )

    def _try_reload_web_client(self) -> None:

        # Assuming slack_config has a reload method to refresh its data
        config = slack_config.reload()

        # Set the last credentials refresh time to the current datetime
        self.last_credentials_refresh_attempt = datetime.now()

        if not self._check_is_slack_config_valid(config):
            raise Exception(
                "Failed to retrieve slack connection details! No valid configuration for slack was found."
            )

        # Mount config
        self.slack_connection_config = config

        # Reinitialize the WebClient with possibly updated proxy and token
        self.slack_client = WebClient(proxy=SLACK_PROXY, token=config.bot_token)

    def _should_save_message_details(self, template_type: str) -> bool:
        if (
            STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED
            and template_type == NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT
        ):
            return True
        return False

    def _save_message_details(
        self,
        request: NotificationRequestClass,
        template_type: str,
        message_details: List[SlackMessageDetails],
    ) -> None:
        if template_type == NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT:
            request_parameters = request.message.parameters or {}
            incident_urn = request_parameters.get("incidentUrn")
            if incident_urn is not None:
                self._save_new_incident_message_details(incident_urn, message_details)

    def _save_new_incident_message_details(
        self, incident_urn: str, message_details: List[SlackMessageDetails]
    ) -> None:
        if len(message_details) != 0:
            self.graph.emit_mcp(
                self._build_message_details_patch_proposal(
                    incident_urn, message_details
                )
            )

    def _build_message_details_patch_proposal(
        self, incident_urn: str, message_details: List[SlackMessageDetails]
    ) -> MetadataChangeProposalClass:
        patch = MetadataPatchProposal(
            urn=incident_urn,
        )

        for message_detail in message_details:
            # Save the information about the incident notification.
            patch._add_patch(
                aspect_name="incidentNotificationDetails",
                op="ADD",
                path=f"/slack/messages/{message_detail.message_id}",
                value={
                    "channelId": message_detail.channel_id,
                    "channelName": message_detail.channel_name,
                    "messageId": message_detail.message_id,
                },
            )

        return list(patch.build())[0]

    def _get_saved_message_details(
        self,
        request: NotificationRequestClass,
    ) -> List[SlackMessageDetails]:

        message_details = []

        # Step 1: Extract the incident urn
        request_parameters = request.message.parameters or {}
        incident_urn = request_parameters.get("incidentUrn")

        if incident_urn is not None:
            # Step 2: Find the message ids for the recipients.
            recipient_ids = [recipient.id for recipient in request.recipients]

            # Step 3: Fetch the saved notification details for the recipients
            notification_details = self.graph.get_aspect(
                entity_urn=incident_urn, aspect_type=IncidentNotificationDetailsClass
            )

            # Step 4: Attempt to resolve the ids, to determine which messages should be updated.
            if (
                notification_details is not None
                and notification_details.slack is not None
            ):
                sent_messages = notification_details.slack.messages

                for sent_message in sent_messages:
                    if (
                        sent_message.channelId in recipient_ids
                        or sent_message.channelName in recipient_ids
                    ):
                        message_detail = SlackMessageDetails(
                            channel_id=sent_message.channelId,
                            channel_name=sent_message.channelName or None,
                            message_id=sent_message.messageId,
                        )
                        message_details.append(message_detail)

        return message_details
