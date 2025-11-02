import logging
from typing import List

import anyio
from asyncer import asyncify
from datahub.metadata.schema_classes import NotificationRequestClass

from datahub_integrations.notifications.sinks.context import NotificationContext
from datahub_integrations.notifications.sinks.sink import NotificationSink

logger = logging.getLogger(__name__)


class NotificationManagerMode:
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class NotificationSinkManager:
    """
    Manages notification sinks and dispatches notifications to them based on their capabilities.
    Assumes validation of templates and parameters is handled elsewhere.
    """

    def __init__(
        self,
        sinks: List[NotificationSink],
        mode: str = NotificationManagerMode.ENABLED,
    ) -> None:
        self.mode = mode
        self.sink_registry = list(sinks)
        for sink in self.sink_registry:
            sink.init()

    async def handle(self, request: NotificationRequestClass) -> None:
        logger.info(
            f"Handling notification with sinks: {self.sink_registry}, Request: {request}"
        )

        if self.mode == NotificationManagerMode.DISABLED:
            logger.debug(
                "NotificationSinkManager is disabled. Skipping notification dispatch."
            )
            return

        # Dispatch the notification to eligible sinks.
        await self.dispatch_notifications(request)

    def _get_eligible_sinks(
        self, request: NotificationRequestClass
    ) -> List[NotificationSink]:
        """
        Filter sinks based on the recipient types in the notification request.
        Only return sinks that can handle at least one recipient type in the request.

        Special case: SUPPORT_LOGIN template type doesn't require recipients in the request
        as it uses environment variables (SUPPORT_LOGIN_EMAIL_RECIPIENTS) for recipients.
        """
        # Special handling for template types that use environment variables for recipients
        from datahub.metadata.schema_classes import NotificationTemplateTypeClass

        template_type = str(request.message.template) if request.message else None
        uses_env_recipients = (
            template_type == NotificationTemplateTypeClass.SUPPORT_LOGIN
        )

        if not request.recipients and not uses_env_recipients:
            logger.warning("No recipients in notification request")
            return []

        # Get all recipient types in the request (empty set if no recipients)
        request_recipient_types = (
            {recipient.type for recipient in request.recipients}
            if request.recipients
            else set()
        )

        eligible_sinks = []
        for sink in self.sink_registry:
            # Get the recipient types this sink supports
            sink_supported_types = set(sink.supported_notification_recipient_types())

            # Check if this sink can handle any of the recipient types in the request,
            # or if this is a template type that uses environment variables for recipients
            if request_recipient_types.intersection(sink_supported_types) or (
                uses_env_recipients and sink.type() == "EMAIL"
            ):
                eligible_sinks.append(sink)
                logger.debug(
                    f"Sink {sink.type()} is eligible - supports {sink_supported_types}, "
                    f"request has {request_recipient_types}, uses_env_recipients={uses_env_recipients}"
                )
            else:
                logger.debug(
                    f"Sink {sink.type()} is not eligible - supports {sink_supported_types}, "
                    f"request has {request_recipient_types}, uses_env_recipients={uses_env_recipients}"
                )

        return eligible_sinks

    async def dispatch_notifications(self, request: NotificationRequestClass) -> None:
        # Filter sinks based on recipient types in the request
        eligible_sinks = self._get_eligible_sinks(request)

        if not eligible_sinks:
            template_type = (
                str(request.message.template) if request.message else "unknown"
            )
            recipient_types = (
                [r.type for r in request.recipients] if request.recipients else []
            )
            logger.warning(
                f"No eligible sinks found for notification request with template: {template_type}, "
                f"recipient types: {recipient_types}"
            )
            return

        logger.info(
            f"Dispatching notification to {len(eligible_sinks)} eligible sinks: "
            f"{[sink.type() for sink in eligible_sinks]}"
        )

        async with anyio.create_task_group() as task_group:
            for sink in eligible_sinks:
                task_group.start_soon(self.send_notification, sink, request)

    async def send_notification(
        self, sink: NotificationSink, request: NotificationRequestClass
    ) -> None:
        try:
            await asyncify(sink.send)(request, NotificationContext())
            logger.info(
                f"Successfully sent notification through {sink} for request {request}"
            )
        except Exception as e:
            logger.error(
                f"Failed to send notification through {sink} for request {request}",
                exc_info=e,
            )
