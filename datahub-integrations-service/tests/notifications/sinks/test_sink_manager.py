from typing import List, Optional
from unittest.mock import Mock, patch

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
    def __init__(
        self, sink_type: str = "TEST", supported_types: Optional[List[str]] = None
    ) -> None:
        self.sink_type = sink_type
        self.supported_types = supported_types or ["TEST_RECIPIENT"]

    def type(self) -> str:
        return self.sink_type

    def init(self) -> None:
        pass

    def send(
        self, request: NotificationRequestClass, context: NotificationContext
    ) -> None:
        pass

    def supported_notification_recipient_types(self) -> List[str]:
        return self.supported_types


@pytest.fixture
def notification_request() -> NotificationRequestClass:
    # Adjust the constructor call with appropriate arguments
    return NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE
        ),
        recipients=[Mock(type="TEST_RECIPIENT", id="test-user")],
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
        # Test handling in enabled mode - should send to eligible sinks
        await notification_manager.handle(notification_request)
        # With our new filtering logic, the test sink should be called because
        # the notification_request fixture now has a TEST_RECIPIENT that matches
        # the test sink's supported types
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


# New tests for recipient type filtering
def test_get_eligible_sinks_teams_only() -> None:
    """Test that Teams-only notifications only go to Teams sink."""
    # Create mock sinks with different recipient type support
    teams_sink = TestSink("TEAMS", ["TEAMS_DM", "TEAMS_CHANNEL"])
    slack_sink = TestSink("SLACK", ["SLACK_DM", "SLACK_CHANNEL"])
    email_sink = TestSink("EMAIL", ["EMAIL"])

    manager = NotificationSinkManager([teams_sink, slack_sink, email_sink])

    # Create a Teams-only notification request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE
        ),
        recipients=[Mock(type="TEAMS_DM", id="test-user")],
    )

    eligible_sinks = manager._get_eligible_sinks(request)

    # Verify only Teams sink is eligible
    assert len(eligible_sinks) == 1
    assert eligible_sinks[0].type() == "TEAMS"


def test_get_eligible_sinks_mixed_recipients() -> None:
    """Test that mixed recipient notifications go to multiple sinks."""
    # Create mock sinks with different recipient type support
    teams_sink = TestSink("TEAMS", ["TEAMS_DM", "TEAMS_CHANNEL"])
    slack_sink = TestSink("SLACK", ["SLACK_DM", "SLACK_CHANNEL"])
    email_sink = TestSink("EMAIL", ["EMAIL"])

    manager = NotificationSinkManager([teams_sink, slack_sink, email_sink])

    # Create a mixed notification request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE
        ),
        recipients=[
            Mock(type="TEAMS_DM", id="teams-user"),
            Mock(type="SLACK_DM", id="slack-user"),
        ],
    )

    eligible_sinks = manager._get_eligible_sinks(request)

    # Verify both Teams and Slack sinks are eligible
    assert len(eligible_sinks) == 2
    eligible_types = {s.type() for s in eligible_sinks}
    assert "TEAMS" in eligible_types
    assert "SLACK" in eligible_types
    assert "EMAIL" not in eligible_types


def test_get_eligible_sinks_no_recipients() -> None:
    """Test that notifications with no recipients return no eligible sinks."""
    teams_sink = TestSink("TEAMS", ["TEAMS_DM", "TEAMS_CHANNEL"])
    slack_sink = TestSink("SLACK", ["SLACK_DM", "SLACK_CHANNEL"])

    manager = NotificationSinkManager([teams_sink, slack_sink])

    # Create a notification request with no recipients
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE
        ),
        recipients=[],
    )

    eligible_sinks = manager._get_eligible_sinks(request)

    # Verify no sinks are eligible
    assert len(eligible_sinks) == 0


def test_get_eligible_sinks_slack_only() -> None:
    """Test that Slack-only notifications only go to Slack sink."""
    teams_sink = TestSink("TEAMS", ["TEAMS_DM", "TEAMS_CHANNEL"])
    slack_sink = TestSink("SLACK", ["SLACK_DM", "SLACK_CHANNEL"])
    email_sink = TestSink("EMAIL", ["EMAIL"])

    manager = NotificationSinkManager([teams_sink, slack_sink, email_sink])

    # Create a Slack-only notification request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE
        ),
        recipients=[Mock(type="SLACK_CHANNEL", id="slack-channel")],
    )

    eligible_sinks = manager._get_eligible_sinks(request)

    # Verify only Slack sink is eligible
    assert len(eligible_sinks) == 1
    assert eligible_sinks[0].type() == "SLACK"


@pytest.mark.anyio
async def test_dispatch_notifications_with_filtering() -> None:
    """Test that dispatch_notifications only sends to eligible sinks."""
    # Create mock sinks
    teams_sink = TestSink("TEAMS", ["TEAMS_DM", "TEAMS_CHANNEL"])
    slack_sink = TestSink("SLACK", ["SLACK_DM", "SLACK_CHANNEL"])

    manager = NotificationSinkManager([teams_sink, slack_sink])

    # Create a Teams-only notification request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE
        ),
        recipients=[Mock(type="TEAMS_DM", id="teams-user")],
    )

    # Mock the send_notification method to track calls
    with patch.object(manager, "send_notification") as mock_send:
        await manager.dispatch_notifications(request)

        # Verify only one sink was called (Teams sink)
        assert mock_send.call_count == 1
        # Verify the Teams sink was called
        called_sink = mock_send.call_args[0][0]
        assert called_sink.type() == "TEAMS"


@pytest.mark.anyio
async def test_dispatch_notifications_no_eligible_sinks() -> None:
    """Test that dispatch_notifications handles no eligible sinks gracefully."""
    teams_sink = TestSink("TEAMS", ["TEAMS_DM", "TEAMS_CHANNEL"])
    slack_sink = TestSink("SLACK", ["SLACK_DM", "SLACK_CHANNEL"])

    manager = NotificationSinkManager([teams_sink, slack_sink])

    # Create a notification request with unsupported recipient type
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE
        ),
        recipients=[Mock(type="UNSUPPORTED_TYPE", id="unsupported-user")],
    )

    # Mock the send_notification method
    with patch.object(manager, "send_notification") as mock_send:
        await manager.dispatch_notifications(request)

        # Verify no sinks were called
        assert mock_send.call_count == 0


def test_get_eligible_sinks_support_login_no_recipients() -> None:
    """Test that SUPPORT_LOGIN with empty recipients still routes to email sink."""
    email_sink = TestSink("EMAIL", ["EMAIL"])
    teams_sink = TestSink("TEAMS", ["TEAMS_DM", "TEAMS_CHANNEL"])
    slack_sink = TestSink("SLACK", ["SLACK_DM", "SLACK_CHANNEL"])

    manager = NotificationSinkManager([email_sink, teams_sink, slack_sink])

    # Create a SUPPORT_LOGIN notification request with no recipients
    # (uses environment variable SUPPORT_LOGIN_EMAIL_RECIPIENTS instead)
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.SUPPORT_LOGIN
        ),
        recipients=[],  # Empty - recipients come from environment variable
    )

    eligible_sinks = manager._get_eligible_sinks(request)

    # Verify email sink is eligible even with no recipients (special case)
    assert len(eligible_sinks) == 1
    assert eligible_sinks[0].type() == "EMAIL"


@pytest.mark.anyio
async def test_dispatch_support_login_notification() -> None:
    """Test that SUPPORT_LOGIN notifications are dispatched to email sink even with no recipients."""
    email_sink = TestSink("EMAIL", ["EMAIL"])
    teams_sink = TestSink("TEAMS", ["TEAMS_DM", "TEAMS_CHANNEL"])

    manager = NotificationSinkManager([email_sink, teams_sink])

    # Create a SUPPORT_LOGIN notification request with no recipients
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.SUPPORT_LOGIN
        ),
        recipients=[],
    )

    # Mock the send_notification method to track calls
    with patch.object(manager, "send_notification") as mock_send:
        await manager.dispatch_notifications(request)

        # Verify email sink was called (special case for SUPPORT_LOGIN)
        assert mock_send.call_count == 1
        called_sink = mock_send.call_args[0][0]
        assert called_sink.type() == "EMAIL"
