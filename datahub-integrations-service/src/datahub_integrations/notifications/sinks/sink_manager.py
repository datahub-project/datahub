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

    async def dispatch_notifications(self, request: NotificationRequestClass) -> None:
        async with anyio.create_task_group() as task_group:
            for sink in self.sink_registry:
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
