from enum import Enum
from typing import Dict, List

from datahub.metadata.schema_classes import (
    NotificationRecipientClass,
    NotificationRecipientTypeClass,
    NotificationRequestClass,
    NotificationSinkTypeClass,
    NotificationTemplateTypeClass,
)
from loguru import logger
from sendgrid import SendGridAPIClient

from datahub_integrations.notifications.constants import (
    DATAHUB_BASE_URL,
    MAX_NOTIFICATION_RETRIES,
    SEND_GRID_API_KEY,
)
from datahub_integrations.notifications.sinks.context import NotificationContext
from datahub_integrations.notifications.sinks.email.send_email import (
    send_change_notification_to_recipients,
    send_compliance_form_notification_to_recipients,
    send_custom_email_to_recipients,
    send_ingestion_run_notification_to_recipients,
    send_support_login_email,
    send_user_invitation_to_recipients,
    send_workflow_request_assignment_notification_to_recipients,
    send_workflow_request_status_change_notification_to_recipients,
)
from datahub_integrations.notifications.sinks.email.template_utils import (
    build_assertion_status_change_parameters,
    build_compliance_form_publish_parameters,
    build_entity_change_parameters,
    build_incident_status_change_parameters,
    build_ingestion_run_change_parameters,
    build_new_incident_parameters,
    build_new_proposal_parameters,
    build_proposal_status_change_parameters,
    build_proposer_proposal_status_change_parameters,
    build_workflow_request_assignment_parameters,
    build_workflow_request_status_change_parameters,
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
    sg_client: SendGridAPIClient

    def type(self) -> str:
        return NotificationSinkTypeClass.EMAIL

    def supported_notification_recipient_types(self) -> List[str]:
        return [NotificationRecipientTypeClass.EMAIL]

    def init(self) -> None:
        self.base_url = DATAHUB_BASE_URL
        self.sg_client = SendGridAPIClient(SEND_GRID_API_KEY)

    def send(
        self, request: NotificationRequestClass, context: NotificationContext
    ) -> None:
        template_type: str = str(request.message.template)

        # --- Special handling for Broadcast Proposal Status Change, because we also broadcast to the original author of proposal.
        if (
            template_type
            == NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE
        ):
            self._send_broadcast_proposal_status_change_notification(request)
            return

        email_recipients = self._get_email_recipients(request)

        # Mapping template types to functions
        action_map = {
            NotificationTemplateTypeClass.CUSTOM: lambda: self._send_custom_notification(
                request
            ),
            NotificationTemplateTypeClass.INVITATION: lambda: self._send_user_invitation_notification(
                email_recipients,
                request.message.parameters or {},
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT: lambda: self._send_change_notification(
                email_recipients,
                build_new_incident_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE: lambda: self._send_change_notification(
                email_recipients,
                build_incident_status_change_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL: lambda: self._send_change_notification(
                email_recipients,
                build_new_proposal_parameters(request, self.base_url),
                RetryMode.DISABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE: lambda: self._send_change_notification(
                email_recipients,
                build_proposal_status_change_parameters(request, self.base_url),
                RetryMode.DISABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE: lambda: self._send_change_notification(
                email_recipients,
                build_entity_change_parameters(request, self.base_url),
                RetryMode.DISABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_INGESTION_RUN_CHANGE: lambda: self._send_ingestion_run_notification(
                email_recipients,
                build_ingestion_run_change_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE: lambda: self._send_change_notification(
                email_recipients,
                build_assertion_status_change_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_COMPLIANCE_FORM_PUBLISH: lambda: self._send_compliance_form_notification(
                email_recipients,
                build_compliance_form_publish_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST: lambda: self._send_workflow_request_assignment_notification(
                email_recipients,
                build_workflow_request_assignment_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE: lambda: self._send_workflow_request_status_change_notification(
                email_recipients,
                build_workflow_request_status_change_parameters(request, self.base_url),
                RetryMode.ENABLED,
            ),
            NotificationTemplateTypeClass.SUPPORT_LOGIN: lambda: self._send_support_login_notification(
                request, RetryMode.ENABLED
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
            send_custom_email_to_recipients(
                request.recipients, subject, message, self.sg_client
            )
        else:
            logger.error(
                "Custom notification request does not have subject or message. Skipping sending email."
            )

    def _send_user_invitation_notification(
        self,
        recipients: List[NotificationRecipientClass],
        parameters: Dict[str, str],
        retry_mode: RetryMode,
    ) -> None:
        """Send user invitation emails using the specialized template."""
        max_attempts = (
            MAX_NOTIFICATION_RETRIES if retry_mode == RetryMode.ENABLED else 1
        )
        try:
            retry_with_backoff(
                send_user_invitation_to_recipients,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                recipients=recipients,
                parameters=parameters,
                sg_client=self.sg_client,
            )
            logger.info(
                f"Successfully sent user invitation emails to {len(recipients)} recipients"
            )
        except Exception as e:
            logger.error(
                f"Failed to send user invitation emails after {max_attempts} attempts: {e}"
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
                sg_client=self.sg_client,
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
                sg_client=self.sg_client,
            )
        except Exception as e:
            logger.error(
                f"Failed to send notification after {max_attempts} attempts. Error: {e}"
            )

    def _send_compliance_form_notification(
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
                send_compliance_form_notification_to_recipients,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                recipients=recipients,
                parameters=parameters,
                sg_client=self.sg_client,
            )
        except Exception as e:
            logger.error(
                f"Failed to send notification after {max_attempts} attempts. Error: {e}"
            )

    def _send_workflow_request_assignment_notification(
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
                send_workflow_request_assignment_notification_to_recipients,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                recipients=recipients,
                parameters=parameters,
                sg_client=self.sg_client,
            )
        except Exception as e:
            logger.error(
                f"Failed to send notification after {max_attempts} attempts. Error: {e}"
            )

    def _send_workflow_request_status_change_notification(
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
                send_workflow_request_status_change_notification_to_recipients,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                recipients=recipients,
                parameters=parameters,
                sg_client=self.sg_client,
            )
        except Exception as e:
            logger.error(
                f"Failed to send notification after {max_attempts} attempts. Error: {e}"
            )

    def _send_support_login_notification(
        self, request: NotificationRequestClass, retry_mode: RetryMode
    ) -> None:
        """Send support login notification email to configured recipients."""
        if request.message.parameters is None:
            logger.error(
                "Support login notification request does not have parameters. Skipping sending email."
            )
            return

        parameters = request.message.parameters
        max_attempts = (
            MAX_NOTIFICATION_RETRIES if retry_mode == RetryMode.ENABLED else 1
        )
        try:
            retry_with_backoff(
                send_support_login_email,
                max_attempts=max_attempts,
                backoff_factor=2,
                initial_backoff=1,
                parameters=parameters,
                sg_client=self.sg_client,
            )
            logger.info("Successfully sent support login notification emails")
        except Exception as e:
            logger.error(
                f"Failed to send support login notification after {max_attempts} attempts. Error: {e}"
            )

    def _get_email_recipients(
        self, request: NotificationRequestClass
    ) -> List[NotificationRecipientClass]:
        return [
            recipient
            for recipient in request.recipients
            if recipient.type in self.supported_notification_recipient_types()
        ]
