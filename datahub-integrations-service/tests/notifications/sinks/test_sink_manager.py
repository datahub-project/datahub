from unittest.mock import patch

import pytest
from datahub.metadata.schema_classes import (
    NotificationMessageClass,
    NotificationRequestClass,
    NotificationTemplateTypeClass,
)

from datahub_integrations.notifications.sinks.context import NotificationContext
from datahub_integrations.notifications.sinks.sink import NotificationSink
from datahub_integrations.notifications.sinks.sink_manager import (
    NotificationManagerMode,
    NotificationSinkManager,
)


class TestSink(NotificationSink):
    def type(self) -> str:
        return "TEST"

    def init(self) -> None:
        pass

    def send(
        self, request: NotificationRequestClass, context: NotificationContext
    ) -> None:
        pass


@pytest.fixture
def notification_request() -> NotificationRequestClass:
    # Adjust the constructor call with appropriate arguments
    return NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE
        ),
        recipients=[],
    )


@pytest.fixture
def test_sink() -> TestSink:
    return TestSink()


@pytest.fixture
def notification_manager(test_sink: TestSink) -> NotificationSinkManager:
    manager = NotificationSinkManager(sinks=[test_sink])
    return manager


@pytest.mark.anyio
async def test_notification_manager_enabled(
    notification_manager: NotificationSinkManager,
    notification_request: NotificationRequestClass,
    test_sink: TestSink,
) -> None:
    with patch.object(test_sink, "init"), patch.object(test_sink, "send") as mock_send:
        # Test handling in enabled mode
        await notification_manager.handle(notification_request)
        mock_send.assert_called()


@pytest.mark.anyio
async def test_notification_manager_disabled(
    notification_manager: NotificationSinkManager,
    notification_request: NotificationRequestClass,
    test_sink: TestSink,
) -> None:
    with patch.object(test_sink, "init"), patch.object(test_sink, "send") as mock_send:
        # Test handling in disabled mode
        notification_manager.mode = NotificationManagerMode.DISABLED
        await notification_manager.handle(notification_request)
        mock_send.assert_not_called()
