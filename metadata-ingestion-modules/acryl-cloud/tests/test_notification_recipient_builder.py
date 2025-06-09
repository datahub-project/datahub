from typing import cast
from unittest.mock import MagicMock

import pytest

from acryl_datahub_cloud.notifications.notification_recipient_builder import (
    NotificationRecipientBuilder,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    CorpGroupSettingsClass,
    CorpUserAppearanceSettingsClass,
    CorpUserSettingsClass,
    EmailNotificationSettingsClass,
    NotificationRecipientTypeClass,
    NotificationSettingClass,
    NotificationSettingsClass,
    NotificationSettingValueClass,
    NotificationSinkTypeClass,
    SlackNotificationSettingsClass,
)


@pytest.fixture
def mock_graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def builder(mock_graph: MagicMock) -> NotificationRecipientBuilder:
    return NotificationRecipientBuilder(mock_graph)


def test_init(builder: NotificationRecipientBuilder, mock_graph: MagicMock) -> None:
    """Test basic initialization of the builder"""
    assert builder.graph == mock_graph
    assert builder.user_settings_map == {}
    assert builder.group_settings_map == {}


def test_is_user(builder: NotificationRecipientBuilder) -> None:
    """Test user URN detection"""
    assert builder.is_user("urn:li:corpuser:test-user") is True
    assert builder.is_user("urn:li:corpGroup:test-group") is False
    assert builder.is_user("urn:li:dataset:test-dataset") is False


def test_is_group(builder: NotificationRecipientBuilder) -> None:
    """Test group URN detection"""
    assert builder.is_group("urn:li:corpGroup:test-group") is True
    assert builder.is_group("urn:li:corpuser:test-user") is False
    assert builder.is_group("urn:li:dataset:test-dataset") is False


def test_populate_user_settings_map(builder: NotificationRecipientBuilder) -> None:
    """Test populating user settings map"""
    mock_graph = cast(MagicMock, builder.graph)

    # Mock user settings response
    mock_settings = CorpUserSettingsClass(
        appearance=CorpUserAppearanceSettingsClass(),
        notificationSettings=NotificationSettingsClass(
            sinkTypes=[NotificationSinkTypeClass.SLACK],
            settings={
                "COMPLIANCE_FORM_PUBLISH": NotificationSettingClass(
                    value=NotificationSettingValueClass.ENABLED,
                    params={"slack.enabled": "true"},
                )
            },
        ),
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpuser:test-user": {"corpUserSettings": (mock_settings, None)}
    }

    builder.populate_user_settings_map(["urn:li:corpuser:test-user"])

    assert "urn:li:corpuser:test-user" in builder.user_settings_map
    assert builder.user_settings_map["urn:li:corpuser:test-user"] == mock_settings
    mock_graph.get_entities.assert_called_once_with(
        "corpuser", ["urn:li:corpuser:test-user"], ["corpUserSettings"]
    )


def test_populate_group_settings_map(builder: NotificationRecipientBuilder) -> None:
    """Test populating group settings map"""
    mock_graph = cast(MagicMock, builder.graph)

    # Mock group settings response
    mock_settings = CorpGroupSettingsClass(
        notificationSettings=NotificationSettingsClass(
            sinkTypes=[NotificationSinkTypeClass.SLACK],
            settings={
                "COMPLIANCE_FORM_PUBLISH": NotificationSettingClass(
                    value=NotificationSettingValueClass.ENABLED,
                    params={"slack.enabled": "true"},
                )
            },
        )
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpGroup:test-group": {"corpGroupSettings": (mock_settings, None)}
    }

    builder.populate_group_settings_map(["urn:li:corpGroup:test-group"])

    assert "urn:li:corpGroup:test-group" in builder.group_settings_map
    assert builder.group_settings_map["urn:li:corpGroup:test-group"] == mock_settings
    mock_graph.get_entities.assert_called_once_with(
        "corpGroup", ["urn:li:corpGroup:test-group"], ["corpGroupSettings"]
    )


def test_build_actor_recipients_with_slack(
    builder: NotificationRecipientBuilder,
) -> None:
    """Test building recipients with Slack notifications enabled"""
    mock_graph = cast(MagicMock, builder.graph)

    # Mock user settings with Slack enabled
    mock_settings = CorpUserSettingsClass(
        appearance=CorpUserAppearanceSettingsClass(),
        notificationSettings=NotificationSettingsClass(
            sinkTypes=[NotificationSinkTypeClass.SLACK],
            settings={
                "COMPLIANCE_FORM_PUBLISH": NotificationSettingClass(
                    value=NotificationSettingValueClass.ENABLED,
                    params={"slack.enabled": "true"},
                )
            },
            slackSettings=SlackNotificationSettingsClass(userHandle="test-user"),
        ),
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpuser:test-user": {"corpUserSettings": (mock_settings, None)}
    }

    recipients = builder.build_actor_recipients(
        ["urn:li:corpuser:test-user"], "COMPLIANCE_FORM_PUBLISH", True
    )

    assert len(recipients) == 1
    recipient = recipients[0]
    assert recipient.type == NotificationRecipientTypeClass.SLACK_DM
    assert recipient.id == "test-user"
    assert recipient.actor == "urn:li:corpuser:test-user"


def test_build_actor_recipients_with_email(
    builder: NotificationRecipientBuilder,
) -> None:
    """Test building recipients with email notifications enabled"""
    mock_graph = cast(MagicMock, builder.graph)

    # Mock user settings with email enabled
    mock_settings = CorpUserSettingsClass(
        appearance=CorpUserAppearanceSettingsClass(),
        notificationSettings=NotificationSettingsClass(
            sinkTypes=[NotificationSinkTypeClass.EMAIL],
            settings={
                "COMPLIANCE_FORM_PUBLISH": NotificationSettingClass(
                    value=NotificationSettingValueClass.ENABLED,
                    params={"email.enabled": "true"},
                )
            },
            emailSettings=EmailNotificationSettingsClass(email="test@example.com"),
        ),
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpuser:test-user": {"corpUserSettings": (mock_settings, None)}
    }

    recipients = builder.build_actor_recipients(
        ["urn:li:corpuser:test-user"], "COMPLIANCE_FORM_PUBLISH", True
    )

    assert len(recipients) == 1
    recipient = recipients[0]
    assert recipient.type == NotificationRecipientTypeClass.EMAIL
    assert recipient.id == "test@example.com"
    assert recipient.actor == "urn:li:corpuser:test-user"


def test_build_actor_recipients_with_both_channels(
    builder: NotificationRecipientBuilder,
) -> None:
    """Test building recipients with both Slack and email notifications enabled"""
    mock_graph = cast(MagicMock, builder.graph)

    # Mock user settings with both Slack and email enabled
    mock_settings = CorpUserSettingsClass(
        appearance=CorpUserAppearanceSettingsClass(),
        notificationSettings=NotificationSettingsClass(
            sinkTypes=[
                NotificationSinkTypeClass.SLACK,
                NotificationSinkTypeClass.EMAIL,
            ],
            settings={
                "COMPLIANCE_FORM_PUBLISH": NotificationSettingClass(
                    value=NotificationSettingValueClass.ENABLED,
                    params={"slack.enabled": "true", "email.enabled": "true"},
                )
            },
            slackSettings=SlackNotificationSettingsClass(userHandle="test-user"),
            emailSettings=EmailNotificationSettingsClass(email="test@example.com"),
        ),
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpuser:test-user": {"corpUserSettings": (mock_settings, None)}
    }

    recipients = builder.build_actor_recipients(
        ["urn:li:corpuser:test-user"], "COMPLIANCE_FORM_PUBLISH", True
    )

    assert len(recipients) == 2

    # Verify Slack recipient
    slack_recipient = next(
        r for r in recipients if r.type == NotificationRecipientTypeClass.SLACK_DM
    )
    assert slack_recipient.id == "test-user"
    assert slack_recipient.actor == "urn:li:corpuser:test-user"

    # Verify email recipient
    email_recipient = next(
        r for r in recipients if r.type == NotificationRecipientTypeClass.EMAIL
    )
    assert email_recipient.id == "test@example.com"
    assert email_recipient.actor == "urn:li:corpuser:test-user"


def test_build_actor_recipients_with_default_enabled(
    builder: NotificationRecipientBuilder,
) -> None:
    """Test building recipients when notifications are default enabled"""
    mock_graph = cast(MagicMock, builder.graph)

    # Mock user settings with no explicit settings (should use default)
    mock_settings = CorpUserSettingsClass(
        appearance=CorpUserAppearanceSettingsClass(),
        notificationSettings=NotificationSettingsClass(
            sinkTypes=[NotificationSinkTypeClass.SLACK],
            slackSettings=SlackNotificationSettingsClass(userHandle="test-user"),
        ),
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpuser:test-user": {"corpUserSettings": (mock_settings, None)}
    }

    recipients = builder.build_actor_recipients(
        ["urn:li:corpuser:test-user"],
        "COMPLIANCE_FORM_PUBLISH",
        True,  # Default enabled
    )

    assert len(recipients) == 1
    recipient = recipients[0]
    assert recipient.type == NotificationRecipientTypeClass.SLACK_DM
    assert recipient.id == "test-user"
    assert recipient.actor == "urn:li:corpuser:test-user"


def test_build_actor_recipients_with_default_disabled(
    builder: NotificationRecipientBuilder,
) -> None:
    """Test building recipients when notifications are default disabled"""
    mock_graph = cast(MagicMock, builder.graph)

    # Mock user settings with no explicit settings (should use default)
    mock_settings = CorpUserSettingsClass(
        appearance=CorpUserAppearanceSettingsClass(),
        notificationSettings=NotificationSettingsClass(
            sinkTypes=[NotificationSinkTypeClass.SLACK],
            slackSettings=SlackNotificationSettingsClass(userHandle="test-user"),
        ),
    )

    mock_graph.get_entities.return_value = {
        "urn:li:corpuser:test-user": {"corpUserSettings": (mock_settings, None)}
    }

    recipients = builder.build_actor_recipients(
        ["urn:li:corpuser:test-user"],
        "COMPLIANCE_FORM_PUBLISH",
        False,  # Default disabled
    )

    assert (
        len(recipients) == 0
    )  # No recipients should be returned when default is disabled
