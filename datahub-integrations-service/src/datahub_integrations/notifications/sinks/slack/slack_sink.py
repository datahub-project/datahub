from datetime import datetime, timedelta
from enum import Enum
from typing import Any, List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
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
)
from datahub_integrations.notifications.sinks.slack.template_utils import (
    build_incident_status_change_message,
    build_new_incident_message,
)
from datahub_integrations.notifications.sinks.utils import retry_with_backoff
from datahub_integrations.slack.config import SLACK_PROXY, slack_config


class RetryMode(Enum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class SlackNotificationSink(NotificationSink):
    """
    A Slack notification sink.
    """

    last_credentials_refresh: Optional[datetime] = None
    base_url: str
    slack_client: WebClient
    identity_provider: IdentityProvider
    graph: DataHubGraph

    def type(self) -> str:
        return NotificationSinkTypeClass.SLACK

    def init(self) -> None:
        # Init Slack client.
        self.base_url = DATAHUB_BASE_URL
        self.graph = graph
        self.identity_provider = IdentityProvider(graph)

    def send(
        self, request: NotificationRequestClass, context: NotificationContext
    ) -> None:
        # Maybe reload slack creds if they are stale.
        self._maybe_reload_web_client()

        template_type: str = str(request.message.template)

        # Mapping template types to functions
        action_map = {
            NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT: lambda: self._send_new_incident_notification(
                request
            ),
            NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE: lambda: self._send_incident_status_change_notification(
                request
            ),
        }

        # Execute the corresponding function or raise an exception for unsupported types
        if template_type in action_map:
            message_ids = action_map[template_type]()
            if self._should_save_message_ids(template_type):
                # Save template id to GMS.
                self._save_message_ids(template_type, message_ids)
        else:
            raise Exception(f"Unsupported template type {template_type} provided.")

    def _send_new_incident_notification(
        self, request: NotificationRequestClass
    ) -> List[str]:
        text, blocks, attachments = build_new_incident_message(
            request, self.identity_provider, self.slack_client, self.base_url
        )

        return self._send_change_notification(
            request.recipients,
            text,
            blocks,
            attachments,
            RetryMode.ENABLED,
        )

    def _send_incident_status_change_notification(
        self, request: NotificationRequestClass
    ) -> List[str]:
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
    ) -> List[str]:
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
            logger.error(
                f"Failed to send notification after {max_attempts} attempts. Error: {e}"
            )
            return []

    def _maybe_reload_web_client(self) -> None:
        """Reloads the WebClient if the last reload was more than 5 minutes ago."""
        current_time = datetime.now()
        if self.last_credentials_refresh is None or (
            current_time - self.last_credentials_refresh
        ) > timedelta(minutes=5):
            self._reload_web_client()

    def _reload_web_client(self) -> None:
        try:
            retry_with_backoff(self._try_reload_web_client, max_attempts=3)
        except Exception as e:
            logger.error(
                "Failed to reload Slack web client! This means slack configs may not be updated.",
                e,
            )

    def _try_reload_web_client(self) -> None:

        # Assuming slack_config has a reload method to refresh its data
        config = slack_config.reload()

        if config is None or config.bot_token is None:
            raise Exception(
                "Failed to retrieve slack connection details! No valid configuration for slack was found."
            )

        # Reinitialize the WebClient with possibly updated proxy and token
        self.slack_client = WebClient(proxy=SLACK_PROXY, token=config.bot_token)

        # Set the last credentials refresh time to the current datetime
        self.last_credentials_refresh = datetime.now()

    def _should_save_message_ids(self, template_type: str) -> bool:
        if template_type == NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT:
            return True
        return False

    def _save_message_ids(self, template_type: str, message_ids: List[str]) -> bool:
        if template_type == NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT:
            self._save_new_incident_message_ids(message_ids)
        return False

    def _save_new_incident_message_ids(self, message_ids: List[str]) -> None:
        if STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED:
            # todo: save new incident message ids.
            logger.info(f"Saving message ids is not yet supported! {message_ids}")
