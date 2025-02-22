from enum import Enum
from typing import Dict, List

from datahub.metadata.schema_classes import (
    NotificationRecipientClass,
    NotificationRequestClass,
    NotificationSinkTypeClass,
    NotificationTemplateTypeClass,
)
from loguru import logger

from datahub_integrations.notifications.constants import (
    DATAHUB_BASE_URL,
    MAX_NOTIFICATION_RETRIES,
)
from datahub_integrations.notifications.sinks.context import NotificationContext
from datahub_integrations.notifications.sinks.email.send_email import (
    send_change_notification_to_recipients,
    send_custom_email_to_recipients,
    send_ingestion_run_notification_to_recipients,
)
from datahub_integrations.notifications.sinks.email.template_utils import (
    build_assertion_status_change_parameters,
    build_entity_change_parameters,
    build_incident_status_change_parameters,
    build_ingestion_run_change_parameters,
    build_new_incident_parameters,
    build_new_proposal_parameters,
    build_proposal_status_change_parameters,
    build_proposer_proposal_status_change_parameters,
)
from datahub_integrations.notifications.sinks.sink import NotificationSink
from datahub_integrations.notifications.sinks.utils import retry_with_backoff


class RetryMode(Enum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class EmailNotificationSink(NotificationSink):
    """
    An email notification sink.
    """

    base_url: str

    def type(self) -> str:
        return NotificationSinkTypeClass.EMAIL

    def init(self) -> None:
        # Unused for now.
        self.base_url = DATAHUB_BASE_URL

    def send(
        self, request: NotificationRequestClass, context: NotificationContext
    ) -> None:
        template_type: str = str(request.message.template)

        # --- We add a custom branch for BROADCAST_PROPOSAL_STATUS_CHANGE
        if (
            template_type
            == NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE
        ):
            self._send_broadcast_proposal_status_change_notification(request)
            return

        # Mapping template types to functions
        action_map = {
            NotificationTemplateTypeClass.CUSTOM: lambda: self._send_custom_notification(
                request
            ),
            NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT: lambda: self._send_change_notification(
                request.recipients,
                build_new_incident_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE: lambda: self._send_change_notification(
                request.recipients,
                build_incident_status_change_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL: lambda: self._send_change_notification(
                request.recipients,
                build_new_proposal_parameters(request, self.base_url),
                RetryMode.DISABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE: lambda: self._send_change_notification(
                request.recipients,
                build_proposal_status_change_parameters(request, self.base_url),
                RetryMode.DISABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE: lambda: self._send_change_notification(
                request.recipients,
                build_entity_change_parameters(request, self.base_url),
                RetryMode.DISABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_INGESTION_RUN_CHANGE: lambda: self._send_ingestion_run_notification(
                request.recipients,
                build_ingestion_run_change_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE: lambda: self._send_change_notification(
                request.recipients,
                build_assertion_status_change_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
        }

        # Execute the corresponding function or raise an exception for unsupported types
        if template_type in action_map:
            action_map[template_type]()
        else:
            logger.warning(
                f"Unsupported template type {template_type} provided. Not sending notification."
            )

    def _send_broadcast_proposal_status_change_notification(
        self, request: NotificationRequestClass
    ) -> None:
        """
        1) If there's a `creatorUrn`, send a personalized email to the original proposer.
        2) Then send a broadcast email to the rest of the recipients.
        """
        recipients = request.recipients or []
        parameters = request.message.parameters or {}

        creator_urn = parameters.get("creatorUrn")

        # 1) Send the personal notification to the creator (if present in the recipients).
        if creator_urn:
            # Find the matching recipient (with origin=ACTOR_NOTIFICATION if you replicate that Slack logic exactly)
            # but commonly you might just match on the actor URN if it exists among recipients.
            creator_recipient = next(
                (r for r in recipients if r.actor == creator_urn),
                None,
            )
            if creator_recipient:
                # Build a specialized parameter set for the "proposer" message
                proposer_params = build_proposer_proposal_status_change_parameters(
                    request, self.base_url
                )
                # Send the personal email
                self._send_change_notification(
                    [creator_recipient],
                    proposer_params,
                    retry_mode=RetryMode.DISABLED,
                )

        # 2) Send the "normal" proposal status change message to everyone else
        broadcast_recipients = [
            r for r in recipients if not (creator_urn and r.actor == creator_urn)
        ]
        if broadcast_recipients:
            broadcast_params = build_proposal_status_change_parameters(
                request, self.base_url
            )
            self._send_change_notification(
                broadcast_recipients,
                broadcast_params,
                retry_mode=RetryMode.DISABLED,
            )

    def _send_custom_notification(self, request: NotificationRequestClass) -> None:
        if request.message.parameters is None:
            logger.error(
                "Custom notification request does not have parameters. Skipping sending email."
            )
            return

        # Send a custom notification
        subject = request.message.parameters.get("title")
        message = request.message.parameters.get("message")

        if subject is not None and message is not None:
            send_custom_email_to_recipients(request.recipients, subject, message)
        else:
            logger.error(
                "Custom notification request does not have subject or message. Skipping sending email."
            )

    def _send_change_notification(
        self,
        recipients: List[NotificationRecipientClass],
        parameters: Dict[str, str | None],
        retry_mode: RetryMode,
    ) -> None:
        max_attempts = (
            MAX_NOTIFICATION_RETRIES if retry_mode == RetryMode.ENABLED else 1
        )
        try:
            retry_with_backoff(
                send_change_notification_to_recipients,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                recipients=recipients,
                parameters=parameters,
            )
        except Exception as e:
            logger.error(
                f"Failed to send notification after {max_attempts} attempts. Error: {e}"
            )

    def _send_ingestion_run_notification(
        self,
        recipients: List[NotificationRecipientClass],
        parameters: Dict[str, str | None],
        retry_mode: RetryMode,
    ) -> None:
        max_attempts = (
            MAX_NOTIFICATION_RETRIES if retry_mode == RetryMode.ENABLED else 1
        )
        try:
            retry_with_backoff(
                send_ingestion_run_notification_to_recipients,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                recipients=recipients,
                parameters=parameters,
            )
        except Exception as e:
            logger.error(
                f"Failed to send notification after {max_attempts} attempts. Error: {e}"
            )
