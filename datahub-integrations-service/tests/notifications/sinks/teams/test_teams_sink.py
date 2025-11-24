from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from datahub.metadata.schema_classes import (
    NotificationMessageClass,
    NotificationRecipientClass,
    NotificationRecipientTypeClass,
    NotificationRequestClass,
    NotificationSinkTypeClass,
    NotificationTemplateTypeClass,
)

from datahub_integrations.notifications.sinks.context import NotificationContext
from datahub_integrations.notifications.sinks.teams.teams_sink import (
    TeamsNotificationSink,
)
from datahub_integrations.teams.config import TeamsAppDetails, TeamsConnection
from datahub_integrations.teams.render.render_entity import EntityCardRenderField


@pytest.fixture
def teams_sink() -> TeamsNotificationSink:
    sink = TeamsNotificationSink()
    sink.init()
    return sink


@pytest.fixture
def teams_notification_request() -> NotificationRequestClass:
    """Create a test notification request for Teams."""
    return NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            parameters={
                "title": "Test Incident",
                "description": "Test incident description",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            ),
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_DM,
                id="test-user@example.com",
            ),
        ],
    )


def test_teams_sink_type(teams_sink: TeamsNotificationSink) -> None:
    """Test that Teams sink returns correct type."""
    assert teams_sink.type() == NotificationSinkTypeClass.TEAMS


def test_teams_sink_supported_recipient_types(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test that Teams sink supports correct recipient types."""
    supported_types = teams_sink.supported_notification_recipient_types()
    assert NotificationRecipientTypeClass.TEAMS_CHANNEL in supported_types
    assert NotificationRecipientTypeClass.TEAMS_DM in supported_types
    assert len(supported_types) == 2


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_teams_sink_send_with_valid_config(
    mock_get_config: MagicMock,
    teams_sink: TeamsNotificationSink,
    teams_notification_request: NotificationRequestClass,
) -> None:
    """Test sending notification with valid Teams config."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    # Mock the adaptive card sending methods (incident notifications use adaptive cards)
    with patch.object(
        teams_sink, "_send_teams_adaptive_card_with_details", new_callable=AsyncMock
    ) as mock_send:
        mock_send.return_value = []  # Return empty list of message details
        teams_sink.send(teams_notification_request, NotificationContext())
        # Should be called once for the incident notification
        mock_send.assert_called_once()


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_teams_sink_send_with_invalid_config(
    mock_get_config: MagicMock,
    teams_sink: TeamsNotificationSink,
    teams_notification_request: NotificationRequestClass,
) -> None:
    """Test sending notification with invalid Teams config."""
    # Mock invalid Teams configuration (missing app_password)
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = None
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    # Mock the message sending methods
    with patch.object(teams_sink, "_send_teams_message") as mock_send:
        teams_sink.send(teams_notification_request, NotificationContext())
        # Should not send message with invalid config
        mock_send.assert_not_called()


def test_teams_sink_get_teams_recipients(teams_sink: TeamsNotificationSink) -> None:
    """Test filtering Teams recipients."""
    recipients = [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
            id="teams-channel",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.SLACK_CHANNEL,
            id="slack-channel",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_DM,
            id="teams-user@example.com",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.EMAIL,
            id="user@example.com",
        ),
    ]

    teams_recipients = teams_sink._get_teams_recipients(recipients)

    # Should only return Teams recipients
    assert len(teams_recipients) == 2
    assert "teams-channel" in teams_recipients
    assert "teams-user@example.com" in teams_recipients
    assert "slack-channel" not in teams_recipients
    assert "user@example.com" not in teams_recipients


def test_teams_sink_config_validation(teams_sink: TeamsNotificationSink) -> None:
    """Test Teams configuration validation."""
    # Valid config
    valid_config = MagicMock()
    valid_config.app_details = MagicMock()
    valid_config.app_details.app_id = "test-app-id"
    valid_config.app_details.app_password = "test-app-password"
    valid_config.app_details.tenant_id = "test-tenant-id"
    assert teams_sink._check_is_teams_config_valid(valid_config) is True

    # Invalid config - missing app_details


def test_build_entity_change_message_success(teams_sink: TeamsNotificationSink) -> None:
    """Test successful entity change message building."""
    # Mock notification request with entity change parameters
    params = {
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest.users%2CPROD%29",
        "actorName": "John Doe",
        "operation": "added",
        "modifierType": "tag",
        "modifier0Name": "PII",
        "entityName": "test.users",
        "entityPlatform": "mysql",
    }

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=params,
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    # Mock entity data response
    mock_entity_data = {
        "type": "DATASET",
        "name": "test.users",
        "platform": {"name": "mysql"},
        "properties": {"name": "users"},
        "institutionalMemory": {"elements": []},
        "ownership": {"owners": []},
        "tags": {"tags": []},
        "glossaryTerms": {"terms": []},
        "description": "User table",
    }

    # Mock entity card response
    mock_entity_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "**Dataset: test.users**",
                    "weight": "Bolder",
                    "size": "Medium",
                }
            ],
        },
    }

    with (
        patch.object(teams_sink, "_extract_entity_urn_from_params") as mock_extract,
        patch.object(teams_sink, "_fetch_entity_details") as mock_fetch,
        patch(
            "datahub_integrations.notifications.sinks.teams.teams_sink.render_entity_card"
        ) as mock_render,
        patch.object(
            teams_sink, "_add_change_banner_to_entity_card"
        ) as mock_add_banner,
    ):
        # Setup mocks
        mock_extract.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"
        )
        mock_fetch.return_value = mock_entity_data
        mock_render.return_value = mock_entity_card
        mock_add_banner.return_value = mock_entity_card

        # Execute method
        result = teams_sink._build_entity_change_message(request)

        # Verify calls
        mock_extract.assert_called_once_with(params)
        mock_fetch.assert_called_once_with(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"
        )
        mock_render.assert_called_once_with(mock_entity_data, fields=[])
        mock_add_banner.assert_called_once_with(mock_entity_card, params)

        # Verify result
        assert result == mock_entity_card


def test_build_entity_change_message_no_entity_urn(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test entity change message when entity URN cannot be extracted."""
    params = {
        "actorName": "John Doe",
        "operation": "added",
        "modifierType": "tag",
        "modifier0Name": "PII",
    }

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=params,
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    mock_fallback_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {"type": "AdaptiveCard", "body": []},
    }

    with (
        patch.object(teams_sink, "_extract_entity_urn_from_params") as mock_extract,
        patch.object(teams_sink, "_build_fallback_notification_card") as mock_fallback,
    ):
        # Setup mocks
        mock_extract.return_value = None
        mock_fallback.return_value = mock_fallback_card

        # Execute method
        result = teams_sink._build_entity_change_message(request)

        # Verify calls
        mock_extract.assert_called_once_with(params)
        mock_fallback.assert_called_once_with(params)

        # Verify result
        assert result == mock_fallback_card


def test_build_entity_change_message_fetch_entity_fails(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test entity change message when entity details fetch fails."""
    params = {
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest.users%2CPROD%29",
        "actorName": "John Doe",
        "operation": "added",
        "modifierType": "tag",
        "modifier0Name": "PII",
    }

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=params,
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    mock_fallback_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {"type": "AdaptiveCard", "body": []},
    }

    with (
        patch.object(teams_sink, "_extract_entity_urn_from_params") as mock_extract,
        patch.object(teams_sink, "_fetch_entity_details") as mock_fetch,
        patch.object(teams_sink, "_build_fallback_notification_card") as mock_fallback,
    ):
        # Setup mocks
        mock_extract.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"
        )
        mock_fetch.return_value = None
        mock_fallback.return_value = mock_fallback_card

        # Execute method
        result = teams_sink._build_entity_change_message(request)

        # Verify calls
        mock_extract.assert_called_once_with(params)
        mock_fetch.assert_called_once_with(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"
        )
        mock_fallback.assert_called_once_with(params)

        # Verify result
        assert result == mock_fallback_card


def test_build_entity_change_message_render_entity_fails(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test entity change message when entity card rendering fails."""
    params = {
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest.users%2CPROD%29",
        "actorName": "John Doe",
        "operation": "added",
        "modifierType": "tag",
        "modifier0Name": "PII",
    }

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=params,
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    mock_entity_data = {
        "type": "DATASET",
        "name": "test.users",
        "platform": {"name": "mysql"},
    }

    mock_fallback_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {"type": "AdaptiveCard", "body": []},
    }

    with (
        patch.object(teams_sink, "_extract_entity_urn_from_params") as mock_extract,
        patch.object(teams_sink, "_fetch_entity_details") as mock_fetch,
        patch(
            "datahub_integrations.notifications.sinks.teams.teams_sink.render_entity_card"
        ) as mock_render,
        patch.object(teams_sink, "_build_fallback_notification_card") as mock_fallback,
    ):
        # Setup mocks
        mock_extract.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"
        )
        mock_fetch.return_value = mock_entity_data
        mock_render.return_value = None
        mock_fallback.return_value = mock_fallback_card

        # Execute method
        result = teams_sink._build_entity_change_message(request)

        # Verify calls
        mock_extract.assert_called_once_with(params)
        mock_fetch.assert_called_once_with(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"
        )
        mock_render.assert_called_once_with(mock_entity_data, fields=[])
        mock_fallback.assert_called_once_with(params)

        # Verify result
        assert result == mock_fallback_card


def test_extract_entity_urn_from_params_dataset_path(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test extracting entity URN from dataset path."""
    params = {
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest.users%2CPROD%29?foo=bar"
    }

    result = teams_sink._extract_entity_urn_from_params(params)
    expected = "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"

    assert result == expected


def test_extract_entity_urn_from_params_no_info(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test extracting entity URN when no usable information is available."""
    params = {
        "actorName": "John Doe",
        "operation": "added",
    }

    result = teams_sink._extract_entity_urn_from_params(params)

    assert result is None


def test_fetch_entity_details_success(teams_sink: TeamsNotificationSink) -> None:
    """Test successful entity details fetching."""
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"

    mock_entity_data = {
        "type": "DATASET",
        "name": "test.users",
        "platform": {"name": "mysql"},
    }

    mock_graph_response = {"entity": mock_entity_data}

    # Mock the graph client
    teams_sink.graph = MagicMock()
    teams_sink.graph.execute_graphql.return_value = mock_graph_response

    result = teams_sink._fetch_entity_details(entity_urn)

    assert result == mock_entity_data
    teams_sink.graph.execute_graphql.assert_called_once()


def test_fetch_entity_details_no_graph_client(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test entity details fetching when graph client is not initialized."""
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"

    teams_sink.graph = None

    result = teams_sink._fetch_entity_details(entity_urn)

    assert result is None


def test_fetch_entity_details_graphql_error(teams_sink: TeamsNotificationSink) -> None:
    """Test entity details fetching when GraphQL query fails."""
    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"

    # Mock the graph client to raise an exception
    teams_sink.graph = MagicMock()
    teams_sink.graph.execute_graphql.side_effect = Exception("GraphQL error")

    result = teams_sink._fetch_entity_details(entity_urn)

    assert result is None


def test_add_change_banner_to_entity_card_added_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test adding change banner for 'added' operation."""
    entity_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "**Dataset: test.users**",
                    "weight": "Bolder",
                    "size": "Medium",
                }
            ],
        },
    }

    params = {
        "actorName": "John Doe",
        "operation": "added",
        "modifierType": "tag",
        "modifier0Name": "PII",
    }

    with patch.object(teams_sink, "_format_modifier_type") as mock_format:
        mock_format.return_value = "Tag"

        result = teams_sink._add_change_banner_to_entity_card(entity_card, params)

        # Verify change banner was added
        assert len(result["content"]["body"]) == 2
        change_banner = result["content"]["body"][
            0
        ]  # Change banner is now first element
        assert change_banner["type"] == "Container"
        assert change_banner["style"] == "accent"

        # Verify banner content contains expected text
        banner_text = change_banner["items"][0]["columns"][1]["items"][1]["text"]
        assert "John Doe added **PII**" in banner_text


def test_add_change_banner_to_entity_card_removed_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test adding change banner for 'removed' operation."""
    entity_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "**Dataset: test.users**",
                    "weight": "Bolder",
                    "size": "Medium",
                }
            ],
        },
    }

    params = {
        "actorName": "Jane Smith",
        "operation": "removed",
        "modifierType": "owner",
        "modifier0Name": "john.doe",
    }

    with patch.object(teams_sink, "_format_modifier_type") as mock_format:
        mock_format.return_value = "Owner"

        result = teams_sink._add_change_banner_to_entity_card(entity_card, params)

        # Verify change banner was added
        assert len(result["content"]["body"]) == 2
        change_banner = result["content"]["body"][
            0
        ]  # Change banner is now first element

        # Verify banner content contains expected text
        banner_text = change_banner["items"][0]["columns"][1]["items"][1]["text"]
        assert "Jane Smith removed **john.doe**" in banner_text


def test_add_change_banner_to_entity_card_invalid_card(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test adding change banner to invalid entity card."""
    invalid_card = {"invalid": "card"}
    params = {
        "actorName": "John Doe",
        "operation": "added",
        "modifierType": "tag",
        "modifier0Name": "PII",
    }

    result = teams_sink._add_change_banner_to_entity_card(invalid_card, params)

    # Should return the card unchanged
    assert result == invalid_card


def test_build_fallback_notification_card(teams_sink: TeamsNotificationSink) -> None:
    """Test building fallback notification card."""
    params = {
        "entityName": "test.users",
        "actorName": "John Doe",
        "operation": "added",
        "modifier0Name": "PII",
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest.users%2CPROD%29",
    }

    with patch(
        "datahub_integrations.app.DATAHUB_FRONTEND_URL", "https://datahub.example.com"
    ):
        result = teams_sink._build_fallback_notification_card(params)

        # Verify card structure
        assert result["contentType"] == "application/vnd.microsoft.card.adaptive"
        assert result["content"]["type"] == "AdaptiveCard"
        assert result["content"]["version"] == "1.4"
        assert len(result["content"]["body"]) >= 2

        # Verify content contains entity name and actor
        body_text = result["content"]["body"][0]["text"]
        assert "test.users" in body_text
        assert "added" in body_text

    # Invalid config - missing app_details
    invalid_config1 = MagicMock()
    invalid_config1.app_details = None
    assert teams_sink._check_is_teams_config_valid(invalid_config1) is False

    # Invalid config - missing app_id
    invalid_config2 = MagicMock()
    invalid_config2.app_details = MagicMock()
    invalid_config2.app_details.app_id = None
    invalid_config2.app_details.app_password = "test-app-password"
    invalid_config2.app_details.tenant_id = "test-tenant-id"
    assert teams_sink._check_is_teams_config_valid(invalid_config2) is False

    # Invalid config - missing app_password
    invalid_config3 = MagicMock()
    invalid_config3.app_details = MagicMock()
    invalid_config3.app_details.app_id = "test-app-id"
    invalid_config3.app_details.app_password = None
    invalid_config3.app_details.tenant_id = "test-tenant-id"
    assert teams_sink._check_is_teams_config_valid(invalid_config3) is False

    # Invalid config - None config
    assert teams_sink._check_is_teams_config_valid(None) is False


@pytest.fixture
def entity_change_notification_request() -> NotificationRequestClass:
    """Create a test notification request for entity change notifications."""
    return NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters={
                "actorName": "John Doe",
                "operation": "added",
                "modifierType": "Tag(s)",
                "modifier0Name": "PII",
                "entityName": "user_profiles",
                "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cuser_profiles%2CPROD%29",
                "entityPlatform": "snowflake",
                "entityType": "DATASET",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_DM,
                id="test-user@example.com",
            ),
        ],
    )


@pytest.fixture
def mock_entity_data() -> dict:
    """Mock entity data returned from GraphQL."""
    return {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,user_profiles,PROD)",
        "name": "user_profiles",
        "type": "DATASET",
        "platform": {
            "name": "snowflake",
            "displayName": "Snowflake",
        },
        "properties": {
            "name": "user_profiles",
            "description": "User profile data",
        },
        "ownership": {
            "owners": [
                {
                    "owner": {
                        "name": "Data Team",
                        "urn": "urn:li:corpuser:datateam",
                    },
                    "type": "TECHNICAL_OWNER",
                }
            ]
        },
        "tags": {
            "tags": [
                {
                    "tag": {
                        "name": "PII",
                        "description": "Personally Identifiable Information",
                    }
                }
            ]
        },
    }


@pytest.fixture
def mock_entity_card() -> dict:
    """Mock entity card returned from render_entity_card."""
    return {
        "type": "AdaptiveCard",
        "version": "1.3",
        "content": {
            "type": "Container",
            "items": [
                {
                    "type": "TextBlock",
                    "text": "user_profiles",
                    "size": "Medium",
                    "weight": "Bolder",
                }
            ],
        },
        "actions": [
            {
                "type": "Action.OpenUrl",
                "title": "View in DataHub",
                "url": "http://localhost:9002/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cuser_profiles%2CPROD%29",
            }
        ],
    }


def test_build_entity_change_message_no_urn(
    teams_sink: TeamsNotificationSink,
    entity_change_notification_request: NotificationRequestClass,
) -> None:
    """Test entity change message building when URN extraction fails."""
    fallback_card = {
        "type": "AdaptiveCard",
        "version": "1.3",
        "content": {"type": "Container", "items": []},
    }

    with patch.object(
        teams_sink, "_extract_entity_urn_from_params"
    ) as mock_extract_urn:
        with patch.object(
            teams_sink, "_build_fallback_notification_card"
        ) as mock_fallback:
            # Set up mocks
            mock_extract_urn.return_value = None
            mock_fallback.return_value = fallback_card

            # Execute the method
            result = teams_sink._build_entity_change_message(
                entity_change_notification_request
            )

            # Verify the result
            assert result == fallback_card

            # Verify method calls
            mock_extract_urn.assert_called_once_with(
                entity_change_notification_request.message.parameters
            )
            mock_fallback.assert_called_once_with(
                entity_change_notification_request.message.parameters
            )


def test_build_entity_change_message_no_entity_data(
    teams_sink: TeamsNotificationSink,
    entity_change_notification_request: NotificationRequestClass,
) -> None:
    """Test entity change message building when entity data fetching fails."""
    fallback_card = {
        "type": "AdaptiveCard",
        "version": "1.3",
        "content": {"type": "Container", "items": []},
    }

    with patch.object(
        teams_sink, "_extract_entity_urn_from_params"
    ) as mock_extract_urn:
        with patch.object(teams_sink, "_fetch_entity_details") as mock_fetch_details:
            with patch.object(
                teams_sink, "_build_fallback_notification_card"
            ) as mock_fallback:
                # Set up mocks
                mock_extract_urn.return_value = (
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,user_profiles,PROD)"
                )
                mock_fetch_details.return_value = None
                mock_fallback.return_value = fallback_card

                # Execute the method
                result = teams_sink._build_entity_change_message(
                    entity_change_notification_request
                )

                # Verify the result
                assert result == fallback_card

                # Verify method calls
                mock_extract_urn.assert_called_once_with(
                    entity_change_notification_request.message.parameters
                )
                mock_fetch_details.assert_called_once_with(
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,user_profiles,PROD)"
                )
                mock_fallback.assert_called_once_with(
                    entity_change_notification_request.message.parameters
                )


def test_build_entity_change_message_no_entity_card(
    teams_sink: TeamsNotificationSink,
    entity_change_notification_request: NotificationRequestClass,
    mock_entity_data: dict,
) -> None:
    """Test entity change message building when entity card rendering fails."""
    fallback_card = {
        "type": "AdaptiveCard",
        "version": "1.3",
        "content": {"type": "Container", "items": []},
    }

    with patch.object(
        teams_sink, "_extract_entity_urn_from_params"
    ) as mock_extract_urn:
        with patch.object(teams_sink, "_fetch_entity_details") as mock_fetch_details:
            with patch.object(
                teams_sink, "_build_fallback_notification_card"
            ) as mock_fallback:
                with patch(
                    "datahub_integrations.notifications.sinks.teams.teams_sink.render_entity_card"
                ) as mock_render:
                    # Set up mocks
                    mock_extract_urn.return_value = "urn:li:dataset:(urn:li:dataPlatform:snowflake,user_profiles,PROD)"
                    mock_fetch_details.return_value = mock_entity_data
                    mock_render.return_value = None
                    mock_fallback.return_value = fallback_card

                    # Execute the method
                    result = teams_sink._build_entity_change_message(
                        entity_change_notification_request
                    )

                    # Verify the result
                    assert result == fallback_card

                    # Verify method calls
                    mock_extract_urn.assert_called_once_with(
                        entity_change_notification_request.message.parameters
                    )
                    mock_fetch_details.assert_called_once_with(
                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,user_profiles,PROD)"
                    )
                    mock_render.assert_called_once_with(
                        mock_entity_data, fields=[EntityCardRenderField.TAG]
                    )
                    mock_fallback.assert_called_once_with(
                        entity_change_notification_request.message.parameters
                    )


def test_build_entity_change_message_empty_parameters(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test entity change message building with empty parameters."""
    empty_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=None,
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_DM,
                id="test-user@example.com",
            ),
        ],
    )

    fallback_card = {
        "type": "AdaptiveCard",
        "version": "1.3",
        "content": {"type": "Container", "items": []},
    }

    with patch.object(
        teams_sink, "_extract_entity_urn_from_params"
    ) as mock_extract_urn:
        with patch.object(
            teams_sink, "_build_fallback_notification_card"
        ) as mock_fallback:
            # Set up mocks
            mock_extract_urn.return_value = None
            mock_fallback.return_value = fallback_card

            # Execute the method
            result = teams_sink._build_entity_change_message(empty_request)

            # Verify the result
            assert result == fallback_card

            # Verify method calls
            mock_extract_urn.assert_called_once_with({})
            mock_fallback.assert_called_once_with({})


def test_extract_entity_urn_from_params_dataset_path_with_query(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test URN extraction from dataset entity path with query parameters."""
    params = {
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cuser_profiles%2CPROD%29?tab=schema",
        "entityName": "user_profiles",
        "entityPlatform": "snowflake",
    }

    result = teams_sink._extract_entity_urn_from_params(params)
    expected = "urn:li:dataset:(urn:li:dataPlatform:snowflake,user_profiles,PROD)"
    assert result == expected


def test_extract_entity_urn_from_params_no_data(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test URN extraction when no usable data is available."""
    params = {
        "actorName": "John Doe",
        "operation": "added",
    }

    result = teams_sink._extract_entity_urn_from_params(params)
    assert result is None


def test_extract_entity_urn_from_params_empty_params(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test URN extraction with empty parameters."""
    result = teams_sink._extract_entity_urn_from_params({})
    assert result is None


def test_extract_entity_urn_from_params_partial_data(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test URN extraction with partial data (missing platform)."""
    params = {
        "entityName": "user_profiles",
        # Missing entityPlatform
    }

    result = teams_sink._extract_entity_urn_from_params(params)
    assert result is None


def test_fetch_entity_details_no_entity_in_response(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test entity details fetching when no entity is returned."""
    # Mock the graph client
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {"data": "some_other_data"}
    teams_sink.graph = mock_graph

    result = teams_sink._fetch_entity_details(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,user_profiles,PROD)"
    )

    assert result is None


def test_fetch_entity_details_none_response(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test entity details fetching when GraphQL returns None."""
    # Mock the graph client
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = None
    teams_sink.graph = mock_graph

    result = teams_sink._fetch_entity_details(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,user_profiles,PROD)"
    )

    assert result is None


# Additional tests from the teams/ directory that were missing


def test_teams_sink_init_properly() -> None:
    """Test that the Teams sink initializes correctly with proper attributes."""
    sink = TeamsNotificationSink()
    sink.init()
    assert sink.base_url is not None
    assert sink.graph is not None
    assert sink.identity_provider is not None


def test_teams_config_validation_with_real_objects() -> None:
    """Test Teams configuration validation with real TeamsConnection objects."""
    sink = TeamsNotificationSink()

    # Test with None config
    assert not sink._check_is_teams_config_valid(None)

    # Test with incomplete config
    config = TeamsConnection()
    assert not sink._check_is_teams_config_valid(config)

    # Test with complete config
    config = TeamsConnection(
        app_details=TeamsAppDetails(
            app_id="test_app_id",
            app_password="test_password",
            tenant_id="test_tenant_id",
        )
    )
    assert sink._check_is_teams_config_valid(config)


def test_get_teams_recipients_with_null_ids() -> None:
    """Test filtering of Teams recipients including null ID handling."""
    sink = TeamsNotificationSink()

    recipients = [
        NotificationRecipientClass(type="TEAMS_CHANNEL", id="channel1"),
        NotificationRecipientClass(type="TEAMS_DM", id="user1"),
        NotificationRecipientClass(type="SLACK_CHANNEL", id="slack1"),
        NotificationRecipientClass(
            type="TEAMS_CHANNEL", id=None
        ),  # Should be filtered out
    ]

    teams_recipients = sink._get_teams_recipients(recipients)
    assert len(teams_recipients) == 2
    assert "channel1" in teams_recipients
    assert "user1" in teams_recipients
    assert "slack1" not in teams_recipients


def test_build_incident_message_with_parameters() -> None:
    """Test building incident notification messages with different parameters."""
    sink = TeamsNotificationSink()

    # Test with parameters
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_INCIDENT", parameters={"title": "Test Incident"}
        ),
        recipients=[],
    )

    adaptive_card = sink._build_incident_message(request)
    # Verify it's an adaptive card structure
    assert isinstance(adaptive_card, dict)
    assert "content" in adaptive_card
    assert "contentType" in adaptive_card
    assert adaptive_card["contentType"] == "application/vnd.microsoft.card.adaptive"
    # Verify it contains the incident title somewhere in the card content
    card_json = str(adaptive_card["content"])
    assert "📝 Incident Status Changed" in card_json

    # Test without parameters
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_INCIDENT", parameters=None
        ),
        recipients=[],
    )

    adaptive_card = sink._build_incident_message(request)
    # Verify it's an adaptive card structure
    assert isinstance(adaptive_card, dict)
    assert "content" in adaptive_card
    assert "contentType" in adaptive_card
    assert adaptive_card["contentType"] == "application/vnd.microsoft.card.adaptive"
    # Verify it contains the default incident text
    card_json = str(adaptive_card["content"])
    assert "Unknown Incident" in card_json


@patch("datahub_integrations.teams.teams.send_teams_message")
@patch("datahub_integrations.notifications.sinks.teams.teams_sink.teams_config")
def test_send_notification_integration(
    mock_teams_config: Any, mock_send_teams_message: Any
) -> None:
    """Test sending a Teams notification with mocked Teams API integration."""
    # Setup mocks
    mock_config = TeamsConnection(
        app_details=TeamsAppDetails(
            app_id="test_app_id",
            app_password="test_password",
            tenant_id="test_tenant_id",
        )
    )
    mock_teams_config.get_config.return_value = mock_config
    mock_send_teams_message.return_value = AsyncMock()

    # Create sink and set up config
    sink = TeamsNotificationSink()
    sink.init()

    # Create test request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_INCIDENT", parameters={"title": "Test Incident"}
        ),
        recipients=[NotificationRecipientClass(type="TEAMS_CHANNEL", id="channel1")],
    )

    context = NotificationContext()

    # Test send notification
    sink.send(request, context)

    # Verify teams_config was called
    mock_teams_config.get_config.assert_called_once()


# Phase 1: Core Functionality Tests


@patch.object(TeamsNotificationSink, "_maybe_reload_teams_config")
@patch.object(TeamsNotificationSink, "_check_is_teams_config_valid")
@patch.object(TeamsNotificationSink, "_send_new_incident_notification")
def test_send_method_new_incident_success(
    mock_send_incident: MagicMock,
    mock_check_config: MagicMock,
    mock_reload_config: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test send() method routes correctly to new incident notification."""
    # Setup mocks
    mock_check_config.return_value = True
    mock_send_incident.return_value = AsyncMock()

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            parameters={"title": "Test Incident"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    context = NotificationContext()

    # Execute
    teams_sink.send(request, context)

    # Verify
    mock_reload_config.assert_called_once()
    mock_check_config.assert_called_once()
    mock_send_incident.assert_called_once_with(request)


@patch.object(TeamsNotificationSink, "_maybe_reload_teams_config")
@patch.object(TeamsNotificationSink, "_check_is_teams_config_valid")
@patch.object(TeamsNotificationSink, "_update_new_incident_notifications")
def test_send_method_incident_update_success(
    mock_send_update: MagicMock,
    mock_check_config: MagicMock,
    mock_reload_config: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test send() method routes correctly to incident update notification."""
    # Setup mocks
    mock_check_config.return_value = True
    mock_send_update.return_value = AsyncMock()

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT_UPDATE,
            parameters={"title": "Test Update"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    context = NotificationContext()

    # Execute
    teams_sink.send(request, context)

    # Verify
    mock_reload_config.assert_called_once()
    mock_check_config.assert_called_once()
    mock_send_update.assert_called_once_with(request)


@patch.object(TeamsNotificationSink, "_maybe_reload_teams_config")
@patch.object(TeamsNotificationSink, "_check_is_teams_config_valid")
@patch.object(TeamsNotificationSink, "_send_incident_status_change_notification")
def test_send_method_incident_status_change_success(
    mock_send_status_change: MagicMock,
    mock_check_config: MagicMock,
    mock_reload_config: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test send() method routes correctly to incident status change notification."""
    # Setup mocks
    mock_check_config.return_value = True
    mock_send_status_change.return_value = AsyncMock()

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE,
            parameters={"incidentId": "123"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    context = NotificationContext()

    # Execute
    teams_sink.send(request, context)

    # Verify
    mock_reload_config.assert_called_once()
    mock_check_config.assert_called_once()
    mock_send_status_change.assert_called_once_with(request)


@patch.object(TeamsNotificationSink, "_maybe_reload_teams_config")
@patch.object(TeamsNotificationSink, "_check_is_teams_config_valid")
@patch.object(TeamsNotificationSink, "_send_compliance_form_publish_notification")
def test_send_method_compliance_form_success(
    mock_send_compliance: MagicMock,
    mock_check_config: MagicMock,
    mock_reload_config: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test send() method routes correctly to compliance form notification."""
    # Setup mocks
    mock_check_config.return_value = True
    mock_send_compliance.return_value = AsyncMock()

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_COMPLIANCE_FORM_PUBLISH,
            parameters={"formName": "Data Classification"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    context = NotificationContext()

    # Execute
    teams_sink.send(request, context)

    # Verify
    mock_reload_config.assert_called_once()
    mock_check_config.assert_called_once()
    mock_send_compliance.assert_called_once_with(request)


@patch.object(TeamsNotificationSink, "_maybe_reload_teams_config")
@patch.object(TeamsNotificationSink, "_check_is_teams_config_valid")
@patch.object(TeamsNotificationSink, "_send_entity_change_notification")
def test_send_method_entity_change_success(
    mock_send_entity: MagicMock,
    mock_check_config: MagicMock,
    mock_reload_config: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test send() method routes correctly to entity change notification."""
    # Setup mocks
    mock_check_config.return_value = True
    mock_send_entity.return_value = AsyncMock()

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters={"entityName": "test.users"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    context = NotificationContext()

    # Execute
    teams_sink.send(request, context)

    # Verify
    mock_reload_config.assert_called_once()
    mock_check_config.assert_called_once()
    mock_send_entity.assert_called_once_with(request)


@patch.object(TeamsNotificationSink, "_maybe_reload_teams_config")
@patch.object(TeamsNotificationSink, "_check_is_teams_config_valid")
def test_send_method_invalid_config(
    mock_check_config: MagicMock,
    mock_reload_config: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test send() method handles invalid configuration gracefully."""
    # Setup mocks
    mock_check_config.return_value = False

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            parameters={"title": "Test Incident"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    context = NotificationContext()

    # Execute
    teams_sink.send(request, context)

    # Verify
    mock_reload_config.assert_called_once()
    mock_check_config.assert_called_once()
    # Should not attempt to send any notification


@patch.object(TeamsNotificationSink, "_maybe_reload_teams_config")
@patch.object(TeamsNotificationSink, "_check_is_teams_config_valid")
def test_send_method_unsupported_template(
    mock_check_config: MagicMock,
    mock_reload_config: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test send() method handles unsupported notification templates."""
    # Setup mocks
    mock_check_config.return_value = True

    # Create request with unsupported template
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.CUSTOM,
            parameters={"customMessage": "Test"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    context = NotificationContext()

    # Execute
    teams_sink.send(request, context)

    # Verify
    mock_reload_config.assert_called_once()
    mock_check_config.assert_called_once()
    # Should log warning but not crash


@patch.object(TeamsNotificationSink, "_maybe_reload_teams_config")
@patch.object(TeamsNotificationSink, "_check_is_teams_config_valid")
@patch.object(TeamsNotificationSink, "_send_new_incident_notification")
def test_send_method_exception_handling(
    mock_send_incident: MagicMock,
    mock_check_config: MagicMock,
    mock_reload_config: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test send() method properly propagates exceptions after retry attempts."""
    # Setup mocks
    mock_check_config.return_value = True
    mock_send_incident.side_effect = Exception("Teams API Error")

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            parameters={"title": "Test Incident"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    context = NotificationContext()

    # Execute - should raise exception after retry attempts
    with pytest.raises(Exception, match="Teams API Error"):
        teams_sink.send(request, context)

    # Verify
    mock_reload_config.assert_called_once()
    mock_check_config.assert_called_once()
    assert mock_send_incident.call_count == 3  # Called 3 times due to retry logic


# Phase 1: _send_teams_message() and related async methods


@patch.object(TeamsNotificationSink, "_get_teams_recipients")
@patch.object(TeamsNotificationSink, "_send_message_to_recipient")
@pytest.mark.asyncio
async def test_send_teams_message_success(
    mock_send_to_recipient: MagicMock,
    mock_get_recipients: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_teams_message() sends to all recipients successfully."""
    # Setup mocks
    mock_get_recipients.return_value = ["channel1", "user1@example.com"]
    mock_send_to_recipient.return_value = AsyncMock()

    # Create recipients
    recipients = [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
            id="channel1",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_DM,
            id="user1@example.com",
        ),
    ]

    # Execute
    await teams_sink._send_teams_message(recipients, "Test message")

    # Verify
    mock_get_recipients.assert_called_once_with(recipients)
    assert mock_send_to_recipient.call_count == 2
    mock_send_to_recipient.assert_any_call("channel1", "Test message")
    mock_send_to_recipient.assert_any_call("user1@example.com", "Test message")


@patch.object(TeamsNotificationSink, "_get_teams_recipients")
@patch.object(TeamsNotificationSink, "_send_message_to_recipient")
@pytest.mark.asyncio
async def test_send_teams_message_with_recipient_error(
    mock_send_to_recipient: MagicMock,
    mock_get_recipients: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_teams_message() handles recipient errors gracefully."""
    # Setup mocks
    mock_get_recipients.return_value = ["channel1", "user1@example.com"]
    mock_send_to_recipient.side_effect = [
        AsyncMock(),  # First recipient succeeds on first try
        Exception("API Error"),  # Second recipient fails on first try
        Exception("API Error"),  # Second recipient fails on second try
        Exception("API Error"),  # Second recipient fails on third try
    ]

    # Create recipients
    recipients = [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
            id="channel1",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_DM,
            id="user1@example.com",
        ),
    ]

    # Execute - should not raise exception
    await teams_sink._send_teams_message(recipients, "Test message")

    # Verify
    mock_get_recipients.assert_called_once_with(recipients)
    assert (
        mock_send_to_recipient.call_count == 4
    )  # First recipient: 1 call, second recipient: 3 calls (due to retries)


@patch.object(TeamsNotificationSink, "_get_teams_recipients")
@patch.object(TeamsNotificationSink, "_send_adaptive_card_to_recipient")
@pytest.mark.asyncio
async def test_send_teams_adaptive_card_success(
    mock_send_card: MagicMock,
    mock_get_recipients: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_teams_adaptive_card() sends to all recipients successfully."""
    # Setup mocks
    mock_get_recipients.return_value = ["channel1", "user1@example.com"]
    mock_send_card.return_value = AsyncMock()

    # Create recipients and adaptive card
    recipients = [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
            id="channel1",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_DM,
            id="user1@example.com",
        ),
    ]

    adaptive_card = {"type": "AdaptiveCard", "body": []}

    # Execute
    await teams_sink._send_teams_adaptive_card(recipients, adaptive_card)

    # Verify
    mock_get_recipients.assert_called_once_with(recipients)
    assert mock_send_card.call_count == 2
    mock_send_card.assert_any_call("channel1", adaptive_card)
    mock_send_card.assert_any_call("user1@example.com", adaptive_card)


@patch.object(TeamsNotificationSink, "_get_teams_recipients")
@patch.object(TeamsNotificationSink, "_send_adaptive_card_to_recipient_with_request")
@pytest.mark.asyncio
async def test_send_teams_adaptive_card_with_request_success(
    mock_send_card_with_request: MagicMock,
    mock_get_recipients: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_teams_adaptive_card_with_request() sends to all recipients successfully."""
    # Setup mocks
    mock_get_recipients.return_value = ["channel1", "user1@example.com"]
    mock_send_card_with_request.return_value = AsyncMock()

    # Create recipients, adaptive card, and request
    recipients = [
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
            id="channel1",
        ),
        NotificationRecipientClass(
            type=NotificationRecipientTypeClass.TEAMS_DM,
            id="user1@example.com",
        ),
    ]

    adaptive_card = {"type": "AdaptiveCard", "body": []}
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters={"entityName": "test.users"},
        ),
        recipients=recipients,
    )

    # Execute
    await teams_sink._send_teams_adaptive_card_with_request(
        recipients, adaptive_card, request
    )

    # Verify
    mock_get_recipients.assert_called_once_with(recipients)
    assert mock_send_card_with_request.call_count == 2
    mock_send_card_with_request.assert_any_call("channel1", adaptive_card, request)
    mock_send_card_with_request.assert_any_call(
        "user1@example.com", adaptive_card, request
    )


@patch.object(TeamsNotificationSink, "_is_channel_recipient")
@patch.object(TeamsNotificationSink, "_send_message_to_conversation")
@pytest.mark.asyncio
async def test_send_message_to_recipient_channel_success(
    mock_send_conversation: MagicMock,
    mock_is_channel: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_message_to_recipient() sends to channel successfully."""
    # Setup mocks
    mock_is_channel.return_value = True
    mock_send_conversation.return_value = AsyncMock()

    # Setup config
    teams_sink.teams_connection_config = TeamsConnection(
        app_details=TeamsAppDetails(
            app_id="test_app_id",
            app_password="test_password",
            tenant_id="test_tenant_id",
        )
    )

    # Execute
    await teams_sink._send_message_to_recipient("channel1", "Test message")

    # Verify
    mock_is_channel.assert_called_once_with("channel1")
    mock_send_conversation.assert_called_once_with("channel1", "Test message")


@patch.object(TeamsNotificationSink, "_is_channel_recipient")
@patch.object(TeamsNotificationSink, "_send_activity_feed_notification")
@patch.object(TeamsNotificationSink, "_send_direct_message")
@pytest.mark.asyncio
async def test_send_message_to_recipient_user_activity_feed_success(
    mock_send_direct: MagicMock,
    mock_send_activity: MagicMock,
    mock_is_channel: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_message_to_recipient() sends to user via activity feed successfully."""
    # Setup mocks
    mock_is_channel.return_value = False
    mock_send_activity.return_value = True  # Activity feed succeeds
    mock_send_direct.return_value = AsyncMock()

    # Setup config
    teams_sink.teams_connection_config = TeamsConnection(
        app_details=TeamsAppDetails(
            app_id="test_app_id",
            app_password="test_password",
            tenant_id="test_tenant_id",
        )
    )

    # Execute
    await teams_sink._send_message_to_recipient("user1@example.com", "Test message")  # type: ignore

    # Verify
    mock_is_channel.assert_called_once_with("user1@example.com")
    mock_send_activity.assert_called_once_with("user1@example.com", "Test message")
    mock_send_direct.assert_not_called()  # Should not fallback to direct message


@pytest.mark.asyncio
async def test_send_message_to_recipient_no_config(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_message_to_recipient() raises exception when configuration is missing."""
    # Setup - no config
    teams_sink.teams_connection_config = None

    # Execute - should raise exception with clear error message
    with pytest.raises(Exception, match="Teams configuration not available"):
        await teams_sink._send_message_to_recipient("user1@example.com", "Test message")  # type: ignore


@patch.object(TeamsNotificationSink, "_is_channel_recipient")
@patch.object(TeamsNotificationSink, "_send_message_to_conversation")
@pytest.mark.asyncio
async def test_send_message_to_recipient_exception_propagation(
    mock_send_conversation: MagicMock,
    mock_is_channel: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_message_to_recipient() propagates exceptions correctly."""
    # Setup mocks
    mock_is_channel.return_value = True
    mock_send_conversation.side_effect = Exception("Teams API Error")

    # Setup config
    teams_sink.teams_connection_config = TeamsConnection(
        app_details=TeamsAppDetails(
            app_id="test_app_id",
            app_password="test_password",
            tenant_id="test_tenant_id",
        )
    )

    # Execute - should raise exception
    with pytest.raises(Exception, match="Teams API Error"):
        await teams_sink._send_message_to_recipient("channel1", "Test message")  # type: ignore

    # Verify
    mock_is_channel.assert_called_once_with("channel1")
    mock_send_conversation.assert_called_once_with("channel1", "Test message")


# Phase 1: Simple message builder tests (incident and compliance)


def test_build_incident_update_message_with_parameters(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test building incident update messages with different parameters."""
    # Test with parameters
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT_UPDATE,
            parameters={"title": "Test Incident Update"},
        ),
        recipients=[],
    )

    adaptive_card = teams_sink._build_incident_update_message(request)
    # Verify it's an adaptive card structure
    assert isinstance(adaptive_card, dict)
    assert "content" in adaptive_card
    assert "contentType" in adaptive_card
    assert adaptive_card["contentType"] == "application/vnd.microsoft.card.adaptive"
    # Verify it contains the incident update text
    card_json = str(adaptive_card["content"])
    assert "📝 Incident Status Changed" in card_json

    # Test without parameters
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT_UPDATE,
            parameters=None,
        ),
        recipients=[],
    )

    adaptive_card = teams_sink._build_incident_update_message(request)
    # Verify it's an adaptive card structure
    assert isinstance(adaptive_card, dict)
    assert "content" in adaptive_card
    assert "contentType" in adaptive_card
    assert adaptive_card["contentType"] == "application/vnd.microsoft.card.adaptive"
    # Verify it contains the default incident text
    card_json = str(adaptive_card["content"])
    assert "Unknown Incident" in card_json


def test_build_incident_status_change_message_with_parameters(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test building incident status change messages with different parameters."""
    # Test with parameters
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE,
            parameters={"title": "Test Incident", "status": "Resolved"},
        ),
        recipients=[],
    )

    adaptive_card = teams_sink._build_incident_status_change_message(request)
    # Verify it's an adaptive card structure
    assert isinstance(adaptive_card, dict)
    assert "content" in adaptive_card
    assert "contentType" in adaptive_card
    assert adaptive_card["contentType"] == "application/vnd.microsoft.card.adaptive"
    # Verify it contains the status change text
    card_json = str(adaptive_card["content"])
    assert "Incident Status Changed" in card_json

    # Test without parameters
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE,
            parameters=None,
        ),
        recipients=[],
    )

    adaptive_card = teams_sink._build_incident_status_change_message(request)
    # Verify it's an adaptive card structure
    assert isinstance(adaptive_card, dict)
    assert "content" in adaptive_card
    assert "contentType" in adaptive_card
    assert adaptive_card["contentType"] == "application/vnd.microsoft.card.adaptive"
    # Verify it contains the default status change text
    card_json = str(adaptive_card["content"])
    assert "Unknown Incident" in card_json


def test_build_compliance_form_message_with_parameters(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test building compliance form messages with different parameters."""
    # Test with parameters
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_COMPLIANCE_FORM_PUBLISH,
            parameters={"formName": "Data Classification Form"},
        ),
        recipients=[],
    )

    message = teams_sink._build_compliance_form_message(request)
    assert "📋 New Compliance Form: Data Classification Form" == message

    # Test without parameters
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_COMPLIANCE_FORM_PUBLISH,
            parameters=None,
        ),
        recipients=[],
    )

    message = teams_sink._build_compliance_form_message(request)
    assert "📋 New Compliance Form: Unknown" == message


def test_build_compliance_form_message_empty_parameters(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test building compliance form messages with empty parameters."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_COMPLIANCE_FORM_PUBLISH,
            parameters={},
        ),
        recipients=[],
    )

    message = teams_sink._build_compliance_form_message(request)
    assert "📋 New Compliance Form: Unknown" == message


# Phase 1: Async notification handler tests


@patch.object(TeamsNotificationSink, "_build_incident_message")
@patch.object(TeamsNotificationSink, "_send_teams_adaptive_card_with_details")
@pytest.mark.asyncio
async def test_send_new_incident_notification(
    mock_send_teams: MagicMock,
    mock_build_message: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_new_incident_notification() builds and sends message correctly."""
    # Setup mocks - build_incident_message returns an adaptive card
    test_adaptive_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type": "AdaptiveCard",
            "body": [{"text": "🚨 New Incident: Test Incident"}],
        },
    }
    mock_build_message.return_value = test_adaptive_card
    mock_send_teams.return_value = []  # Return empty list of message details

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            parameters={"title": "Test Incident"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    # Execute
    await teams_sink._send_new_incident_notification(request)

    # Verify
    mock_build_message.assert_called_once_with(request)
    mock_send_teams.assert_called_once_with(request.recipients, test_adaptive_card)


@patch.object(TeamsNotificationSink, "_get_saved_message_details")
@patch.object(TeamsNotificationSink, "_build_incident_update_message")
@patch.object(TeamsNotificationSink, "_update_teams_messages")
@pytest.mark.asyncio
async def test_update_new_incident_notifications(
    mock_send_teams: MagicMock,
    mock_build_message: MagicMock,
    mock_get_saved_message_details: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _update_new_incident_notifications() builds and sends message correctly."""
    # Setup mocks - build_incident_update_message returns an adaptive card
    test_adaptive_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type": "AdaptiveCard",
            "body": [{"text": "📢 Incident Update: Test Update"}],
        },
    }
    mock_build_message.return_value = test_adaptive_card
    mock_send_teams.return_value = None
    mock_get_saved_message_details.return_value = []  # Return empty list of saved message details

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT_UPDATE,
            parameters={"title": "Test Update"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    # Execute
    await teams_sink._update_new_incident_notifications(request)

    # Verify
    mock_get_saved_message_details.assert_called_once_with(request)
    mock_build_message.assert_called_once_with(request)
    mock_send_teams.assert_called_once_with([], test_adaptive_card)


@patch.object(TeamsNotificationSink, "_build_incident_status_change_message")
@patch.object(TeamsNotificationSink, "_send_teams_adaptive_card")
@pytest.mark.asyncio
async def test_send_incident_status_change_notification(
    mock_send_teams: MagicMock,
    mock_build_message: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_incident_status_change_notification() builds and sends message correctly."""
    # Setup mocks - build_incident_status_change_message returns an adaptive card
    test_adaptive_card = {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type": "AdaptiveCard",
            "body": [{"text": "✅ Incident Status Changed: Test Incident"}],
        },
    }
    mock_build_message.return_value = test_adaptive_card
    mock_send_teams.return_value = AsyncMock()

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE,
            parameters={"title": "Test Incident", "status": "Resolved"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    # Execute
    await teams_sink._send_incident_status_change_notification(request)

    # Verify
    mock_build_message.assert_called_once_with(request)
    mock_send_teams.assert_called_once_with(request.recipients, test_adaptive_card)


@patch.object(TeamsNotificationSink, "_build_compliance_form_message")
@patch.object(TeamsNotificationSink, "_send_teams_message")
@pytest.mark.asyncio
async def test_send_compliance_form_publish_notification(
    mock_send_teams: MagicMock,
    mock_build_message: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_compliance_form_publish_notification() builds and sends message correctly."""
    # Setup mocks
    mock_build_message.return_value = "📋 New Compliance Form: Data Classification Form"
    mock_send_teams.return_value = AsyncMock()

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_COMPLIANCE_FORM_PUBLISH,
            parameters={"formName": "Data Classification Form"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    # Execute
    await teams_sink._send_compliance_form_publish_notification(request)

    # Verify
    mock_build_message.assert_called_once_with(request)
    mock_send_teams.assert_called_once_with(
        request.recipients, "📋 New Compliance Form: Data Classification Form"
    )


@patch.object(TeamsNotificationSink, "_build_entity_change_message")
@patch.object(TeamsNotificationSink, "_send_teams_adaptive_card_with_request")
@pytest.mark.asyncio
async def test_send_entity_change_notification(
    mock_send_adaptive_card: MagicMock,
    mock_build_entity_message: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_entity_change_notification() builds and sends adaptive card correctly."""
    # Setup mocks
    mock_adaptive_card = {"type": "AdaptiveCard", "body": []}
    mock_build_entity_message.return_value = mock_adaptive_card
    mock_send_adaptive_card.return_value = AsyncMock()

    # Create request
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters={"entityName": "test.users"},
        ),
        recipients=[
            NotificationRecipientClass(
                type=NotificationRecipientTypeClass.TEAMS_CHANNEL,
                id="test-channel",
            )
        ],
    )

    # Execute
    await teams_sink._send_entity_change_notification(request)

    # Verify
    mock_build_entity_message.assert_called_once_with(request)
    mock_send_adaptive_card.assert_called_once_with(
        request.recipients, mock_adaptive_card, request
    )


# Phase 2: Message Building Tests


@patch.object(TeamsNotificationSink, "_extract_entity_name_from_params")
@patch.object(TeamsNotificationSink, "_extract_platform_name_from_params")
@patch.object(TeamsNotificationSink, "_extract_entity_type_from_params")
@patch.object(TeamsNotificationSink, "_extract_container_breadcrumbs_from_params")
@patch.object(TeamsNotificationSink, "_build_full_entity_path")
@patch.object(TeamsNotificationSink, "_format_action_message")
@patch.object(TeamsNotificationSink, "_determine_activity_type_from_params")
@patch.object(TeamsNotificationSink, "_build_template_parameters_from_params")
def test_build_activity_feed_message_success(
    mock_build_template_params: MagicMock,
    mock_determine_activity_type: MagicMock,
    mock_format_action: MagicMock,
    mock_build_full_path: MagicMock,
    mock_extract_breadcrumbs: MagicMock,
    mock_extract_entity_type: MagicMock,
    mock_extract_platform: MagicMock,
    mock_extract_entity_name: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_activity_feed_message() builds complete activity feed message."""
    # Setup mocks
    mock_extract_entity_name.return_value = "user_profiles"
    mock_extract_platform.return_value = "snowflake"
    mock_extract_entity_type.return_value = "dataset"
    mock_extract_breadcrumbs.return_value = "data_warehouse"
    mock_build_full_path.return_value = "snowflake dataset data_warehouse.user_profiles"
    mock_format_action.return_value = "➕ John Doe added tag PII"
    mock_determine_activity_type.return_value = "tag_added"
    mock_build_template_params.return_value = {
        "entity": "user_profiles",
        "actor": "John Doe",
    }

    # Create parameters
    params = {
        "actorName": "John Doe",
        "operation": "added",
        "modifierType": "tags",
        "modifier0Name": "PII",
        "entityName": "user_profiles",
        "entityPlatform": "snowflake",
        "entityType": "DATASET",
    }

    # Execute
    activity_type, preview_text, template_parameters, entity_url = (
        teams_sink._build_activity_feed_message(params)
    )

    # Verify results
    assert activity_type == "tag_added"
    assert preview_text == "➕ John Doe added tag PII"
    assert template_parameters == {"entity": "user_profiles", "actor": "John Doe"}
    assert entity_url is None  # Currently disabled

    # Verify all helper methods were called
    mock_extract_entity_name.assert_called_once_with(params)
    mock_extract_platform.assert_called_once_with(params)
    mock_extract_entity_type.assert_called_once_with(params)
    mock_extract_breadcrumbs.assert_called_once_with(params)
    mock_build_full_path.assert_called_once_with(
        "snowflake", "dataset", "data_warehouse", "user_profiles"
    )
    mock_format_action.assert_called_once_with("John Doe", "added", "tags", "PII")
    mock_determine_activity_type.assert_called_once_with("added", "tags")
    mock_build_template_params.assert_called_once_with(
        "tag_added", params, "snowflake dataset data_warehouse.user_profiles"
    )


@patch.object(TeamsNotificationSink, "_extract_entity_name_from_params")
@patch.object(TeamsNotificationSink, "_extract_platform_name_from_params")
@patch.object(TeamsNotificationSink, "_extract_entity_type_from_params")
@patch.object(TeamsNotificationSink, "_extract_container_breadcrumbs_from_params")
@patch.object(TeamsNotificationSink, "_build_full_entity_path")
@patch.object(TeamsNotificationSink, "_format_action_message")
@patch.object(TeamsNotificationSink, "_determine_activity_type_from_params")
@patch.object(TeamsNotificationSink, "_build_template_parameters_from_params")
def test_build_activity_feed_message_default_values(
    mock_build_template_params: MagicMock,
    mock_determine_activity_type: MagicMock,
    mock_format_action: MagicMock,
    mock_build_full_path: MagicMock,
    mock_extract_breadcrumbs: MagicMock,
    mock_extract_entity_type: MagicMock,
    mock_extract_platform: MagicMock,
    mock_extract_entity_name: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_activity_feed_message() with default values for missing parameters."""
    # Setup mocks
    mock_extract_entity_name.return_value = "unknown_entity"
    mock_extract_platform.return_value = "unknown_platform"
    mock_extract_entity_type.return_value = "unknown_type"
    mock_extract_breadcrumbs.return_value = ""
    mock_build_full_path.return_value = "unknown_platform unknown_type unknown_entity"
    mock_format_action.return_value = "📝 Someone changed property"
    mock_determine_activity_type.return_value = "property_changed"
    mock_build_template_params.return_value = {
        "entity": "unknown_entity",
        "actor": "Someone",
    }

    # Create parameters with minimal data
    params: dict[str, str] = {}

    # Execute
    activity_type, preview_text, template_parameters, entity_url = (
        teams_sink._build_activity_feed_message(params)
    )

    # Verify results with default values
    assert activity_type == "property_changed"
    assert preview_text == "📝 Someone changed property"
    assert template_parameters == {"entity": "unknown_entity", "actor": "Someone"}
    assert entity_url is None

    # Verify format_action was called with default values
    mock_format_action.assert_called_once_with("Someone", "changed", "property", "")


def test_format_change_message_added_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_change_message() for added operations."""
    with patch.object(teams_sink, "_build_full_entity_path") as mock_build_path:
        mock_build_path.return_value = "snowflake dataset data_warehouse.user_profiles"

        result = teams_sink._format_change_message(
            actor_name="John Doe",
            operation="added",
            modifier_type="tags",
            modifier_name="PII",
            entity_name="user_profiles",
            platform_name="snowflake",
            entity_type="dataset",
            container_breadcrumbs="data_warehouse",
        )

        expected = "➕ John Doe added tag PII to snowflake dataset data_warehouse.user_profiles"
        assert result == expected
        mock_build_path.assert_called_once_with(
            "snowflake", "dataset", "data_warehouse", "user_profiles"
        )


def test_format_change_message_removed_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_change_message() for removed operations."""
    with patch.object(teams_sink, "_build_full_entity_path") as mock_build_path:
        mock_build_path.return_value = "mysql dataset analytics.user_data"

        result = teams_sink._format_change_message(
            actor_name="Jane Smith",
            operation="removed",
            modifier_type="terms",
            modifier_name="Sensitive",
            entity_name="user_data",
            platform_name="mysql",
            entity_type="dataset",
            container_breadcrumbs="analytics",
        )

        expected = "➖ Jane Smith removed term Sensitive from mysql dataset analytics.user_data"
        assert result == expected


def test_format_change_message_updated_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_change_message() for updated operations."""
    with patch.object(teams_sink, "_build_full_entity_path") as mock_build_path:
        mock_build_path.return_value = "bigquery dataset project.dataset.table"

        result = teams_sink._format_change_message(
            actor_name="Bob Wilson",
            operation="updated",
            modifier_type="description",
            modifier_name="",
            entity_name="table",
            platform_name="bigquery",
            entity_type="dataset",
            container_breadcrumbs="project.dataset",
        )

        expected = (
            "✏️ Bob Wilson updated description on bigquery dataset project.dataset.table"
        )
        assert result == expected


def test_format_change_message_various_modifier_types(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_change_message() with various modifier types."""
    with patch.object(teams_sink, "_build_full_entity_path") as mock_build_path:
        mock_build_path.return_value = "platform type entity"

        # Test Tag(s) modifier type
        result = teams_sink._format_change_message(
            actor_name="User",
            operation="added",
            modifier_type="Tag(s)",
            modifier_name="Confidential",
            entity_name="entity",
            platform_name="platform",
            entity_type="type",
            container_breadcrumbs="",
        )
        assert "tag Confidential" in result

        # Test Term(s) modifier type
        result = teams_sink._format_change_message(
            actor_name="User",
            operation="added",
            modifier_type="Term(s)",
            modifier_name="BusinessCritical",
            entity_name="entity",
            platform_name="platform",
            entity_type="type",
            container_breadcrumbs="",
        )
        assert "term BusinessCritical" in result

        # Test ownership modifier type
        result = teams_sink._format_change_message(
            actor_name="User",
            operation="changed",
            modifier_type="ownership",
            modifier_name="",
            entity_name="entity",
            platform_name="platform",
            entity_type="type",
            container_breadcrumbs="",
        )
        assert "ownership" in result

        # Test schema modifier type
        result = teams_sink._format_change_message(
            actor_name="User",
            operation="updated",
            modifier_type="schema",
            modifier_name="",
            entity_name="entity",
            platform_name="platform",
            entity_type="type",
            container_breadcrumbs="",
        )
        assert "schema" in result

        # Test custom modifier type
        result = teams_sink._format_change_message(
            actor_name="User",
            operation="added",
            modifier_type="custom_field",
            modifier_name="value",
            entity_name="entity",
            platform_name="platform",
            entity_type="type",
            container_breadcrumbs="",
        )
        assert "custom_field value" in result


def test_format_change_message_unknown_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_change_message() with unknown operation."""
    with patch.object(teams_sink, "_build_full_entity_path") as mock_build_path:
        mock_build_path.return_value = "platform type entity"

        result = teams_sink._format_change_message(
            actor_name="User",
            operation="unknown_operation",
            modifier_type="tags",
            modifier_name="Test",
            entity_name="entity",
            platform_name="platform",
            entity_type="type",
            container_breadcrumbs="",
        )

        # Should use "changed" as default verb and 📝 as default emoji
        assert "📝 User changed tag Test on platform type entity" == result


def test_format_action_message_added_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_action_message() for added operations."""
    result = teams_sink._format_action_message(
        actor_name="John Doe",
        operation="added",
        modifier_type="tags",
        modifier_name="PII",
    )

    expected = "➕ John Doe added tag PII"
    assert result == expected


def test_format_action_message_removed_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_action_message() for removed operations."""
    result = teams_sink._format_action_message(
        actor_name="Jane Smith",
        operation="removed",
        modifier_type="terms",
        modifier_name="Sensitive",
    )

    expected = "➖ Jane Smith removed term Sensitive"
    assert result == expected


def test_format_action_message_updated_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_action_message() for updated operations."""
    result = teams_sink._format_action_message(
        actor_name="Bob Wilson",
        operation="updated",
        modifier_type="description",
        modifier_name="",
    )

    expected = "✏️ Bob Wilson updated description"
    assert result == expected


def test_format_action_message_changed_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_action_message() for changed operations."""
    result = teams_sink._format_action_message(
        actor_name="Alice Brown",
        operation="changed",
        modifier_type="ownership",
        modifier_name="",
    )

    expected = "✏️ Alice Brown updated ownership"
    assert result == expected


def test_format_action_message_various_modifier_types(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_action_message() with various modifier types."""
    # Test Tag(s) modifier type
    result = teams_sink._format_action_message(
        actor_name="User",
        operation="added",
        modifier_type="Tag(s)",
        modifier_name="Confidential",
    )
    assert result == "➕ User added tag Confidential"

    # Test Term(s) modifier type
    result = teams_sink._format_action_message(
        actor_name="User",
        operation="added",
        modifier_type="Term(s)",
        modifier_name="BusinessCritical",
    )
    assert result == "➕ User added term BusinessCritical"

    # Test description modifier type
    result = teams_sink._format_action_message(
        actor_name="User",
        operation="updated",
        modifier_type="description",
        modifier_name="",
    )
    assert result == "✏️ User updated description"

    # Test schema modifier type
    result = teams_sink._format_action_message(
        actor_name="User",
        operation="updated",
        modifier_type="schema",
        modifier_name="",
    )
    assert result == "✏️ User updated schema"

    # Test ownership modifier type
    result = teams_sink._format_action_message(
        actor_name="User",
        operation="changed",
        modifier_type="ownership",
        modifier_name="",
    )
    assert result == "✏️ User updated ownership"

    # Test custom modifier type with name
    result = teams_sink._format_action_message(
        actor_name="User",
        operation="added",
        modifier_type="custom_field",
        modifier_name="value",
    )
    assert result == "➕ User added custom_field value"

    # Test custom modifier type without name
    result = teams_sink._format_action_message(
        actor_name="User",
        operation="added",
        modifier_type="custom_field",
        modifier_name="",
    )
    assert result == "➕ User added custom_field"


def test_format_action_message_unknown_operation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _format_action_message() with unknown operation."""
    result = teams_sink._format_action_message(
        actor_name="User",
        operation="unknown_operation",
        modifier_type="tags",
        modifier_name="Test",
    )

    # Should use "changed" as default verb and 📝 as default emoji
    assert result == "📝 User changed tag Test"


# ==============================================
# Phase 3: Parameter Extraction Methods Tests
# ==============================================


def test_extract_entity_name_from_params_with_entity_name(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_name_from_params() with entityName parameter."""
    params = {"entityName": "test.users"}
    result = teams_sink._extract_entity_name_from_params(params)
    assert result == "test.users"


def test_extract_entity_name_from_params_with_entity_path(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_name_from_params() with entityPath parameter."""
    params = {
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest.users%2CPROD%29"
    }

    with patch.object(teams_sink, "_extract_entity_name", return_value="test.users"):
        result = teams_sink._extract_entity_name_from_params(params)
        assert result == "test.users"


def test_extract_entity_name_from_params_with_platform_fallback(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_name_from_params() with platform fallback."""
    params = {"entityPlatform": "mysql"}
    result = teams_sink._extract_entity_name_from_params(params)
    assert result == "mysql entity"


def test_extract_entity_name_from_params_default_fallback(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_name_from_params() with default fallback."""
    params: dict[str, str] = {}
    result = teams_sink._extract_entity_name_from_params(params)
    assert result == "entity"


def test_extract_entity_name_from_params_invalid_path(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_name_from_params() with invalid entityPath."""
    params = {"entityPath": "/invalid/path", "entityPlatform": "mysql"}
    result = teams_sink._extract_entity_name_from_params(params)
    assert result == "mysql entity"


def test_extract_platform_name_from_params_with_platform(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_platform_name_from_params() with platform parameter."""
    params = {"entityPlatform": "mysql"}
    result = teams_sink._extract_platform_name_from_params(params)
    assert result == "Mysql"


def test_extract_platform_name_from_params_empty(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_platform_name_from_params() with empty parameters."""
    params: dict[str, str] = {}
    result = teams_sink._extract_platform_name_from_params(params)
    assert result == ""


def test_extract_entity_type_from_params_dataset(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_type_from_params() with DATASET type."""
    params = {"entityType": "DATASET"}
    result = teams_sink._extract_entity_type_from_params(params)
    assert result == "table"


def test_extract_entity_type_from_params_chart(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_type_from_params() with CHART type."""
    params = {"entityType": "CHART"}
    result = teams_sink._extract_entity_type_from_params(params)
    assert result == "chart"


def test_extract_entity_type_from_params_unknown_type(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_type_from_params() with unknown type."""
    params = {"entityType": "UNKNOWN_TYPE"}
    result = teams_sink._extract_entity_type_from_params(params)
    assert result == "unknown_type"


def test_extract_entity_type_from_params_empty(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_type_from_params() with empty parameters."""
    params: dict[str, str] = {}
    result = teams_sink._extract_entity_type_from_params(params)
    assert result == ""


def test_extract_container_breadcrumbs_from_params_with_entity_path(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_container_breadcrumbs_from_params() with entityPath."""
    params = {
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest.users%2CPROD%29"
    }

    with patch.object(teams_sink, "_extract_breadcrumbs_from_urn", return_value="test"):
        result = teams_sink._extract_container_breadcrumbs_from_params(params)
        assert result == "test"


def test_extract_container_breadcrumbs_from_params_with_entity_name(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_container_breadcrumbs_from_params() with dotted entityName."""
    params = {"entityName": "database.schema.table"}
    result = teams_sink._extract_container_breadcrumbs_from_params(params)
    assert result == "database.schema"


def test_extract_container_breadcrumbs_from_params_empty(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_container_breadcrumbs_from_params() with empty parameters."""
    params: dict[str, str] = {}
    result = teams_sink._extract_container_breadcrumbs_from_params(params)
    assert result == ""


def test_extract_breadcrumbs_from_urn_dataset(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_breadcrumbs_from_urn() with dataset URN."""
    urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,database.schema.table,PROD)"
    result = teams_sink._extract_breadcrumbs_from_urn(urn)
    assert result == "database.schema"


def test_extract_breadcrumbs_from_urn_simple_name(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_breadcrumbs_from_urn() with simple name."""
    urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,table,PROD)"
    result = teams_sink._extract_breadcrumbs_from_urn(urn)
    assert result == ""


def test_extract_breadcrumbs_from_urn_invalid_format(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_breadcrumbs_from_urn() with invalid URN format."""
    urn = "invalid:urn:format"
    result = teams_sink._extract_breadcrumbs_from_urn(urn)
    assert result == ""


def test_build_full_entity_path_with_all_parts(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_full_entity_path() with all parts provided."""
    result = teams_sink._build_full_entity_path(
        platform_name="MySQL",
        entity_type="table",
        container_breadcrumbs="database.schema",
        entity_name="users",
    )
    assert result == "MySQL table database.schema.users"


def test_build_full_entity_path_no_container(teams_sink: TeamsNotificationSink) -> None:
    """Test _build_full_entity_path() without container breadcrumbs."""
    result = teams_sink._build_full_entity_path(
        platform_name="MySQL",
        entity_type="table",
        container_breadcrumbs="",
        entity_name="users",
    )
    assert result == "MySQL table users"


def test_build_full_entity_path_minimal_parts(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_full_entity_path() with minimal parts."""
    result = teams_sink._build_full_entity_path(
        platform_name="", entity_type="", container_breadcrumbs="", entity_name="users"
    )
    assert result == "users"


def test_build_full_entity_path_fallback_to_entity(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_full_entity_path() with fallback to 'entity'."""
    result = teams_sink._build_full_entity_path(
        platform_name="", entity_type="", container_breadcrumbs="", entity_name=""
    )
    assert result == "entity"


def test_extract_entity_url_from_params_with_entity_urn(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_url_from_params() with entityUrn parameter."""
    params = {
        "entityType": "DATASET",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)",
    }

    with patch(
        "datahub_integrations.notifications.sinks.teams.teams_sink.get_type_url",
        return_value="http://test.com/dataset/123",
    ):
        result = teams_sink._extract_entity_url_from_params(params)
        assert result == "http://test.com/dataset/123"


def test_extract_entity_url_from_params_with_entity_path_dataset(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_url_from_params() with entityPath for dataset."""
    params = {
        "entityPath": "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Amysql%2Ctest.users%2CPROD%29"
    }

    with patch(
        "datahub_integrations.notifications.sinks.teams.teams_sink.get_type_url",
        return_value="http://test.com/dataset/123",
    ):
        result = teams_sink._extract_entity_url_from_params(params)
        assert result == "http://test.com/dataset/123"


def test_extract_entity_url_from_params_with_entity_path_chart(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_url_from_params() with entityPath for chart."""
    params = {"entityPath": "/chart/urn%3Ali%3Achart%3A%28test%29"}

    with patch(
        "datahub_integrations.notifications.sinks.teams.teams_sink.get_type_url",
        return_value="http://test.com/chart/123",
    ):
        result = teams_sink._extract_entity_url_from_params(params)
        assert result == "http://test.com/chart/123"


def test_extract_entity_url_from_params_with_entity_path_dashboard(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_url_from_params() with entityPath for dashboard."""
    params = {"entityPath": "/dashboard/urn%3Ali%3Adashboard%3A%28test%29"}

    with patch(
        "datahub_integrations.notifications.sinks.teams.teams_sink.get_type_url",
        return_value="http://test.com/dashboard/123",
    ):
        result = teams_sink._extract_entity_url_from_params(params)
        assert result == "http://test.com/dashboard/123"


def test_extract_entity_url_from_params_fallback_to_frontend_url(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_url_from_params() with fallback to frontend URL."""
    params = {"entityPath": "/custom/path"}

    with patch("datahub_integrations.app.DATAHUB_FRONTEND_URL", "http://frontend.com"):
        result = teams_sink._extract_entity_url_from_params(params)
        assert result == "http://frontend.com/custom/path"


def test_extract_entity_url_from_params_no_url_data(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_url_from_params() with no URL data."""
    params: dict[str, str] = {}
    result = teams_sink._extract_entity_url_from_params(params)
    assert result is None


def test_extract_entity_url_from_params_exception_handling(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_url_from_params() with exception in URL parsing."""
    params = {"entityPath": "/dataset/invalid%path"}

    # Should handle exception gracefully and fallback to frontend URL
    result = teams_sink._extract_entity_url_from_params(params)
    # It falls back to building frontend URL from entity path
    assert result is not None and "/dataset/invalid%path" in result


# ==============================================
# Phase 3: Utility Methods Tests
# ==============================================


def test_determine_activity_type_from_params_entity_changed(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _determine_activity_type_from_params() with entity change modifiers."""
    test_cases = [
        ("added", "tags", "entityChanged"),
        ("removed", "terms", "entityChanged"),
        ("updated", "description", "entityChanged"),
        ("changed", "schema", "entityChanged"),
        ("modified", "ownership", "entityChanged"),
    ]

    for operation, modifier_type, expected in test_cases:
        result = teams_sink._determine_activity_type_from_params(
            operation, modifier_type
        )
        assert result == expected


def test_determine_activity_type_from_params_dataset_updated(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _determine_activity_type_from_params() with dataset modifiers."""
    result = teams_sink._determine_activity_type_from_params(
        "updated", "dataset_schema"
    )
    assert result == "datasetUpdated"


def test_determine_activity_type_from_params_general_notification(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _determine_activity_type_from_params() with general modifiers."""
    result = teams_sink._determine_activity_type_from_params("added", "custom_field")
    assert result == "generalNotification"


def test_build_template_parameters_from_params_entity_changed(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_template_parameters_from_params() with entityChanged activity."""
    result = teams_sink._build_template_parameters_from_params(
        "entityChanged", {}, "MySQL table test.users"
    )
    assert result == {"entityName": "MySQL table test.users"}


def test_build_template_parameters_from_params_dataset_updated(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_template_parameters_from_params() with datasetUpdated activity."""
    result = teams_sink._build_template_parameters_from_params(
        "datasetUpdated", {}, "MySQL table test.users"
    )
    assert result == {"datasetName": "MySQL table test.users"}


def test_build_template_parameters_from_params_general_notification(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_template_parameters_from_params() with generalNotification activity."""
    result = teams_sink._build_template_parameters_from_params(
        "generalNotification", {}, "MySQL table test.users"
    )
    assert result == {"message": "MySQL table test.users"}


def test_build_template_parameters_from_params_unknown_activity(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _build_template_parameters_from_params() with unknown activity."""
    result = teams_sink._build_template_parameters_from_params(
        "unknownActivity", {}, "MySQL table test.users"
    )
    assert result == {"message": "MySQL table test.users"}


def test_extract_entity_name_urn_pattern(teams_sink: TeamsNotificationSink) -> None:
    """Test _extract_entity_name() with URN pattern."""
    message = "urn:li:dataset:test.users has been updated"
    result = teams_sink._extract_entity_name(message)
    assert result == "test.users"


def test_extract_entity_name_quoted_pattern(teams_sink: TeamsNotificationSink) -> None:
    """Test _extract_entity_name() with quoted pattern."""
    message = 'Entity "test.users" has been updated'
    result = teams_sink._extract_entity_name(message)
    assert result == "test.users"


def test_extract_entity_name_fallback_truncation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_entity_name() with fallback truncation."""
    message = "A very long message that should be truncated to fifty characters maximum"
    result = teams_sink._extract_entity_name(message)
    assert result == "A very long message that should be truncated to fi"
    assert len(result) == 50


def test_extract_incident_title_multiline(teams_sink: TeamsNotificationSink) -> None:
    """Test _extract_incident_title() with multiline text."""
    message = "Critical Data Quality Issue\nAffecting production tables\nNeeds immediate attention"
    result = teams_sink._extract_incident_title(message)
    assert result == "Critical Data Quality Issue"


def test_extract_incident_title_single_line(teams_sink: TeamsNotificationSink) -> None:
    """Test _extract_incident_title() with single line text."""
    message = "Critical Data Quality Issue"
    result = teams_sink._extract_incident_title(message)
    assert result == "Critical Data Quality Issue"


def test_extract_incident_title_empty_message(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_incident_title() with empty message."""
    message = ""
    result = teams_sink._extract_incident_title(message)
    assert result == ""


def test_extract_form_name_with_form_pattern(teams_sink: TeamsNotificationSink) -> None:
    """Test _extract_form_name() with form pattern."""
    message = "Please complete the form: Data Quality Assessment"
    result = teams_sink._extract_form_name(message)
    assert result == "Data Quality Assessment"


def test_extract_form_name_with_form_colon_pattern(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_form_name() with form colon pattern."""
    message = "New form: Compliance Review for Q4"
    result = teams_sink._extract_form_name(message)
    assert result == "Compliance Review for Q4"


def test_extract_form_name_fallback_first_line(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_form_name() with fallback to first line."""
    message = "Data Quality Assessment\nPlease complete by Friday"
    result = teams_sink._extract_form_name(message)
    assert result == "Data Quality Assessment"


def test_extract_form_name_empty_message_truncation(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_form_name() with empty message truncation."""
    message = ""  # Empty message
    result = teams_sink._extract_form_name(message)
    # Should return empty string
    assert result == ""


def test_is_channel_recipient_teams_channel(teams_sink: TeamsNotificationSink) -> None:
    """Test _is_channel_recipient() with Teams channel format."""
    channel_id = "19:abc123def456@thread.teams"
    result = teams_sink._is_channel_recipient(channel_id)
    assert result is True


def test_is_channel_recipient_skype_channel(teams_sink: TeamsNotificationSink) -> None:
    """Test _is_channel_recipient() with Skype channel format."""
    channel_id = "19:abc123def456@thread.skype"
    result = teams_sink._is_channel_recipient(channel_id)
    assert result is True


def test_is_channel_recipient_azure_user_id(teams_sink: TeamsNotificationSink) -> None:
    """Test _is_channel_recipient() with Azure AD user ID (UUID)."""
    user_id = "12345678-1234-1234-1234-123456789abc"
    result = teams_sink._is_channel_recipient(user_id)
    assert result is False


def test_is_channel_recipient_unknown_format(teams_sink: TeamsNotificationSink) -> None:
    """Test _is_channel_recipient() with unknown format."""
    unknown_id = "some-unknown-format"
    result = teams_sink._is_channel_recipient(unknown_id)
    assert result is False  # Default to treating as user (safer for DM notifications)


def test_is_channel_recipient_mixed_case_uuid(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _is_channel_recipient() with mixed case UUID."""
    user_id = "12345678-ABCD-1234-abcd-123456789abc"
    result = teams_sink._is_channel_recipient(user_id)
    assert result is False


def test_is_channel_recipient_invalid_uuid(teams_sink: TeamsNotificationSink) -> None:
    """Test _is_channel_recipient() with invalid UUID format."""
    invalid_uuid = "12345678-1234-1234-1234-123456789abcd"  # Too long
    result = teams_sink._is_channel_recipient(invalid_uuid)
    assert result is False  # Default to treating as user (safer for DM notifications)


# Additional tests for enhanced coverage


def create_test_notification_request(
    recipient_id: str, recipient_type: str
) -> NotificationRequestClass:
    """Helper to create a test notification request."""
    return NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters={
                "subject": "Test entity change",
                "body": "Test entity change notification",
                "entityName": "test entity",
                "entityType": "dataset",
                "platform": "test platform",
                "action": "added",
                "modifier": "tag1",
                "actor": "test-user",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                id=recipient_id,
                type=getattr(NotificationRecipientTypeClass, recipient_type),
            )
        ],
    )


# @pytest.mark.asyncio
# async def test_send_teams_message_channel_direct_bot_framework(
#     teams_sink: TeamsNotificationSink,
# ) -> None:
#     """Test _send_teams_message with channel recipient goes directly to Bot Framework."""
#     # Setup notification request with channel recipient
#     notification_request = NotificationRequestClass(
#         message=NotificationMessageClass(
#             template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
#             parameters={
#                 "subject": "Test entity change",
#                 "body": "Test entity change notification",
#                 "entityName": "test entity",
#                 "entityType": "dataset",
#                 "platform": "test platform",
#                 "action": "added",
#                 "modifier": "tag1",
#                 "actor": "test-user",
#             }
#         ),
#         recipients=[NotificationRecipientClass(id="channel-id", type=NotificationRecipientTypeClass.TEAMS_CHANNEL)]
#     )
#
#     # Mock Bot Framework success
#     with patch(
#         "datahub_integrations.teams.teams.send_teams_message"
#     ) as mock_send_message:
#         with patch(
#             "datahub_integrations.teams.teams.send_activity_feed_notification"
#         ) as mock_activity_feed:
#             mock_send_message.return_value = {"id": "message-123"}
#
#             # Call the method
#             await teams_sink._send_teams_message(  # type: ignore
#                 notification_request, "channel-id", "Test message"
#             )
#
#             # Verify Activity Feed was NOT attempted for channel
#             mock_activity_feed.assert_not_called()
#
#             # Verify direct Bot Framework call
#             mock_send_message.assert_called_once()
#
#
# @pytest.mark.asyncio
# async def test_send_message_to_recipient_success(
#     teams_sink: TeamsNotificationSink,
# ) -> None:
#     """Test _send_message_to_recipient successful execution."""
#     # Setup notification request
#     notification_request = NotificationRequestClass(
#         scenarioType="ENTITY_TAG_CHANGE",
#         recipients=[NotificationRecipientClass(id="user-id", type="TEAMS_DM")],
#         content={
#             "subject": "Test entity change",
#             "body": "Test entity change notification",
#             "entityName": "test entity",
#             "entityType": "dataset",
#             "platform": "test platform",
#             "action": "added",
#             "modifier": "tag1",
#             "actor": "test-user",
#         },
#     )
#
#     with patch.object(
#         teams_sink, "_send_teams_adaptive_card_with_request"
#     ) as mock_send_card:
#         mock_send_card.return_value = None
#
#         # Call the method
#         await teams_sink._send_message_to_recipient(  # type: ignorenotification_request, "user-id")
#
#         # Verify adaptive card was sent
#         mock_send_card.assert_called_once_with(notification_request, "user-id")
#
#


def test_maybe_reload_teams_config_success(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _maybe_reload_teams_config successful reload."""
    # Setup mock config
    mock_config = MagicMock()
    mock_config.is_configured = True

    with patch(
        "datahub_integrations.teams.config.teams_config.get_config",
        return_value=mock_config,
    ):
        # Call the method
        teams_sink._maybe_reload_teams_config()

        # Verify config was reloaded
        assert teams_sink.teams_connection_config == mock_config


def test_maybe_reload_teams_config_failure(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _maybe_reload_teams_config with failure."""
    original_config = teams_sink.teams_connection_config

    with patch(
        "datahub_integrations.teams.config.teams_config.get_config",
        side_effect=Exception("Config error"),
    ):
        # Call the method - should not raise exception
        teams_sink._maybe_reload_teams_config()

        # Verify config was not changed
        assert teams_sink.teams_connection_config == original_config


def test_extract_preview_from_card_success(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_preview_from_card successful extraction."""
    # Mock adaptive card with text content
    adaptive_card = {
        "type": "AdaptiveCard",
        "body": [
            {"type": "TextBlock", "text": "This is preview text"},
            {
                "type": "Container",
                "items": [{"type": "TextBlock", "text": "Additional text"}],
            },
        ],
    }

    preview = teams_sink._extract_preview_from_card(adaptive_card)
    assert preview == "This is preview text Additional text"


#
#
def test_extract_preview_from_card_empty(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_preview_from_card with empty card."""
    adaptive_card = {"type": "AdaptiveCard", "body": []}

    preview = teams_sink._extract_preview_from_card(adaptive_card)
    assert preview == "DataHub notification"


def test_extract_preview_from_card_none(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_preview_from_card with None card."""
    preview = teams_sink._extract_preview_from_card(None)
    assert preview == ""


#
#
def test_extract_preview_from_card_no_text(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _extract_preview_from_card with no text blocks."""
    adaptive_card = {
        "type": "AdaptiveCard",
        "body": [{"type": "Image", "url": "https://example.com/image.png"}],
    }

    preview = teams_sink._extract_preview_from_card(adaptive_card)
    assert preview == "DataHub notification"


# @pytest.mark.asyncio
# async def test_send_activity_feed_notification_success(
#     teams_sink: TeamsNotificationSink,
# ) -> None:
#     """Test _send_activity_feed_notification successful execution."""
#     with patch(
#         "datahub_integrations.teams.teams.send_activity_feed_notification"
#     ) as mock_send:
#         mock_send.return_value = {"id": "activity-123"}
#
#         result = await teams_sink._send_activity_feed_notification(
#             "user-id",
#             "datasetUpdated",
#             "Test activity",
#             {"entityName": "test entity"},
#             "https://example.com/entity",
#         )
#
#         assert result == {"id": "activity-123"}
#         mock_send.assert_called_once()
#
#
# @pytest.mark.asyncio
# async def test_send_activity_feed_notification_failure(
#     teams_sink: TeamsNotificationSink,
# ) -> None:
#     """Test _send_activity_feed_notification with failure."""
#     with patch(
#         "datahub_integrations.teams.teams.send_activity_feed_notification"
#     ) as mock_send:
#         mock_send.side_effect = Exception("Activity Feed error")
#
#         with pytest.raises(Exception, match="Activity Feed error"):
#             await teams_sink._send_activity_feed_notification(
#                 "user-id",
#                 "datasetUpdated",
#                 "Test activity",
#                 {"entityName": "test entity"},
#                 "https://example.com/entity",
#             )
#
#
# @pytest.mark.asyncio
# async def test_send_activity_feed_notification_with_request_success(


#
# Tests for New Notification Types
#


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_send_custom_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test custom notification handling."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="CUSTOM",
            parameters={
                "title": "Test Custom Title",
                "body": "Test custom message body",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_CHANNEL",
                id="test-channel",
            ),
        ],
    )

    with patch.object(teams_sink, "_send_teams_message") as mock_send:
        teams_sink.send(request, NotificationContext())

        # Verify the method was called
        mock_send.assert_called_once()

        # Verify the arguments
        args, kwargs = mock_send.call_args
        recipients, message_text = args
        assert len(recipients) == 1
        assert recipients[0].id == "test-channel"
        assert "Test Custom Title" in message_text
        assert "Test custom message body" in message_text


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_send_new_proposal_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test new proposal notification handling."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_PROPOSAL",
            parameters={
                "actorName": "John Doe",
                "entityName": "TestDataset",
                "operation": "add",
                "modifierType": "Tag(s)",
                "entityPath": "/dataset/test",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_DM",
                id="user123",
            ),
        ],
    )

    with patch.object(teams_sink, "_send_teams_adaptive_card") as mock_send:
        teams_sink.send(request, NotificationContext())

        # Verify the method was called
        mock_send.assert_called_once()

        # Verify adaptive card structure
        args, kwargs = mock_send.call_args
        recipients, adaptive_card = args
        assert len(recipients) == 1
        assert recipients[0].id == "user123"

        # Check adaptive card content
        content = adaptive_card["content"]
        assert content["type"] == "AdaptiveCard"
        assert len(content["body"]) >= 2  # Title and description blocks
        assert "New Proposal Raised" in content["body"][0]["text"]
        assert "John Doe" in content["body"][1]["text"]
        assert "TestDataset" in content["body"][1]["text"]


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_send_proposal_status_change_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test proposal status change notification handling."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_PROPOSAL_STATUS_CHANGE",
            parameters={
                "actorName": "Jane Smith",
                "entityName": "TestDataset",
                "operation": "add",
                "modifierType": "Tag(s)",
                "action": "accepted",
                "entityPath": "/dataset/test",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_DM",
                id="user456",
            ),
        ],
    )

    with patch.object(teams_sink, "_send_teams_adaptive_card") as mock_send:
        teams_sink.send(request, NotificationContext())

        # Verify the method was called
        mock_send.assert_called_once()

        # Verify adaptive card structure
        args, kwargs = mock_send.call_args
        recipients, adaptive_card = args
        assert len(recipients) == 1
        assert recipients[0].id == "user456"

        # Check adaptive card content for status change
        content = adaptive_card["content"]
        assert content["type"] == "AdaptiveCard"
        assert "Proposal Status Changed" in content["body"][0]["text"]
        assert "Jane Smith" in content["body"][1]["text"]
        assert "accepted" in content["body"][1]["text"]
        # Should have good color for accepted status
        assert content["body"][0]["color"] == "Good"


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_send_ingestion_run_change_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test ingestion run change notification handling."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_INGESTION_RUN_CHANGE",
            parameters={
                "sourceName": "snowflake-prod",
                "sourceType": "snowflake",
                "statusText": "failed",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_CHANNEL",
                id="data-engineering",
            ),
        ],
    )

    with patch.object(teams_sink, "_send_teams_adaptive_card") as mock_send:
        teams_sink.send(request, NotificationContext())

        # Verify the method was called
        mock_send.assert_called_once()

        # Verify adaptive card structure
        args, kwargs = mock_send.call_args
        recipients, adaptive_card = args
        assert len(recipients) == 1
        assert recipients[0].id == "data-engineering"

        # Check adaptive card content for ingestion status
        content = adaptive_card["content"]
        assert content["type"] == "AdaptiveCard"
        assert "Ingestion Status Update" in content["body"][0]["text"]
        assert "snowflake-prod" in content["body"][1]["text"]
        assert "failed" in content["body"][1]["text"]
        # Should have attention color for failed status
        assert content["body"][0]["color"] == "Attention"


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_send_assertion_status_change_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test assertion status change notification handling."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_ASSERTION_STATUS_CHANGE",
            parameters={
                "assertionType": "freshness",
                "entityName": "user_events",
                "entityPath": "/dataset/user_events",
                "result": "SUCCESS",
                "resultReason": "Data arrived within expected timeframe",
                "description": "Data should be updated daily",
                "externalUrl": "https://dbt.example.com/results",
                "externalPlatform": "dbt",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_DM",
                id="data-owner-123",
            ),
        ],
    )

    with patch.object(teams_sink, "_send_teams_adaptive_card") as mock_send:
        teams_sink.send(request, NotificationContext())

        # Verify the method was called
        mock_send.assert_called_once()

        # Verify adaptive card structure
        args, kwargs = mock_send.call_args
        recipients, adaptive_card = args
        assert len(recipients) == 1
        assert recipients[0].id == "data-owner-123"

        # Check adaptive card content for assertion status
        content = adaptive_card["content"]
        assert content["type"] == "AdaptiveCard"
        assert "Assertion Passed" in content["body"][0]["text"]
        assert "user_events" in content["body"][1]["text"]
        assert "Data should be updated daily" in content["body"][1]["text"]
        # Should have good color for success
        assert content["body"][0]["color"] == "Good"
        # Should have reason if provided
        assert "Data arrived within expected timeframe" in content["body"][2]["text"]
        # Should have external link action
        assert len(content["actions"]) == 1
        assert content["actions"][0]["title"] == "View in dbt"
        assert content["actions"][0]["url"] == "https://dbt.example.com/results"


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_send_workflow_form_request_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test workflow form request notification handling."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST",
            parameters={
                "formName": "Data Access Request",
                "actorName": "John Manager",
                "entityName": "customer_data",
                "assigneeName": "Jane Analyst",
                "dueDate": "2024-02-15",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_DM",
                id="jane-analyst-456",
            ),
        ],
    )

    with patch.object(teams_sink, "_send_teams_adaptive_card") as mock_send:
        teams_sink.send(request, NotificationContext())

        # Verify the method was called
        mock_send.assert_called_once()

        # Verify adaptive card structure
        args, kwargs = mock_send.call_args
        recipients, adaptive_card = args
        assert len(recipients) == 1
        assert recipients[0].id == "jane-analyst-456"

        # Check adaptive card content for workflow form assignment
        content = adaptive_card["content"]
        assert content["type"] == "AdaptiveCard"
        assert "New Workflow Form Assignment" in content["body"][0]["text"]
        assert "John Manager" in content["body"][1]["text"]
        assert "Data Access Request" in content["body"][1]["text"]
        assert "Jane Analyst" in content["body"][1]["text"]
        assert "customer_data" in content["body"][1]["text"]
        assert "2024-02-15" in content["body"][2]["text"]


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_send_workflow_form_status_change_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test workflow form status change notification handling."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
            parameters={
                "formName": "Data Access Request",
                "actorName": "Jane Analyst",
                "entityName": "customer_data",
                "status": "completed",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_DM",
                id="john-manager-789",
            ),
        ],
    )

    with patch.object(teams_sink, "_send_teams_adaptive_card") as mock_send:
        teams_sink.send(request, NotificationContext())

        # Verify the method was called
        mock_send.assert_called_once()

        # Verify adaptive card structure
        args, kwargs = mock_send.call_args
        recipients, adaptive_card = args
        assert len(recipients) == 1
        assert recipients[0].id == "john-manager-789"

        # Check adaptive card content for workflow form status change
        content = adaptive_card["content"]
        assert content["type"] == "AdaptiveCard"
        assert "Workflow Form Status Changed" in content["body"][0]["text"]
        assert "Jane Analyst" in content["body"][1]["text"]
        assert "completed" in content["body"][1]["text"]
        assert "Data Access Request" in content["body"][1]["text"]
        assert "customer_data" in content["body"][1]["text"]
        # Should have good color for completed status
        assert content["body"][0]["color"] == "Good"


def test_build_custom_message(teams_sink: TeamsNotificationSink) -> None:
    """Test custom message building."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="CUSTOM",
            parameters={
                "title": "System Alert",
                "body": "Scheduled maintenance tonight at 10 PM",
            },
        ),
        recipients=[],
    )

    message = teams_sink._build_custom_message(request)
    assert "System Alert" in message
    assert "Scheduled maintenance tonight at 10 PM" in message
    assert message == "**System Alert**\nScheduled maintenance tonight at 10 PM"


def test_build_custom_message_title_only(teams_sink: TeamsNotificationSink) -> None:
    """Test custom message building with title only."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="CUSTOM",
            parameters={
                "title": "Quick Update",
            },
        ),
        recipients=[],
    )

    message = teams_sink._build_custom_message(request)
    assert message == "Quick Update"


def test_build_proposal_message_basic(teams_sink: TeamsNotificationSink) -> None:
    """Test basic proposal message building."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_PROPOSAL",
            parameters={
                "actorName": "Alice",
                "entityName": "UserTable",
                "operation": "add",
                "modifierType": "Documentation",
            },
        ),
        recipients=[],
    )

    adaptive_card = teams_sink._build_proposal_message(request)
    content = adaptive_card["content"]

    assert content["type"] == "AdaptiveCard"
    assert "New Proposal Raised" in content["body"][0]["text"]
    assert "Alice" in content["body"][1]["text"]
    assert "add Documentation for UserTable" in content["body"][1]["text"]


def test_build_ingestion_run_change_message_statuses(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test ingestion run change message with different statuses."""
    # Test failed status
    request_failed = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_INGESTION_RUN_CHANGE",
            parameters={
                "sourceName": "postgres-prod",
                "sourceType": "postgres",
                "statusText": "failed",
            },
        ),
        recipients=[],
    )

    card_failed = teams_sink._build_ingestion_run_change_message(request_failed)
    assert card_failed["content"]["body"][0]["color"] == "Attention"
    assert "❌" in card_failed["content"]["body"][0]["text"]

    # Test completed status
    request_completed = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_INGESTION_RUN_CHANGE",
            parameters={
                "sourceName": "snowflake-dev",
                "sourceType": "snowflake",
                "statusText": "completed successfully",
            },
        ),
        recipients=[],
    )

    card_completed = teams_sink._build_ingestion_run_change_message(request_completed)
    assert card_completed["content"]["body"][0]["color"] == "Good"
    assert "✅" in card_completed["content"]["body"][0]["text"]

    # Test running status
    request_running = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_INGESTION_RUN_CHANGE",
            parameters={
                "sourceName": "bigquery-analytics",
                "sourceType": "bigquery",
                "statusText": "started",
            },
        ),
        recipients=[],
    )

    card_running = teams_sink._build_ingestion_run_change_message(request_running)
    assert card_running["content"]["body"][0]["color"] == "Accent"
    assert "🔄" in card_running["content"]["body"][0]["text"]


@patch("datahub_integrations.teams.config.teams_config.get_config")
async def test_send_new_action_workflow_form_request_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test sending new action workflow form request notification."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config
    # Also set it directly on the sink since private methods check this attribute directly
    teams_sink.teams_connection_config = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST",
            parameters={
                "workflowName": "Data Access Request",
                "actorName": "John Doe",
                "entityName": "user_table",
                "entityType": "DATASET",
                "entityPlatform": "snowflake",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_CHANNEL",
                id="test-channel",
            ),
        ],
    )

    with patch.object(
        teams_sink, "_send_teams_adaptive_card", new_callable=AsyncMock
    ) as mock_send:
        await teams_sink._send_workflow_form_request_notification(request)

        # Verify message was sent
        mock_send.assert_called_once()
        args = mock_send.call_args[0]
        recipients, card = args
        assert len(recipients) == 1
        assert recipients[0].id == "test-channel"
        assert card["contentType"] == "application/vnd.microsoft.card.adaptive"


@patch("datahub_integrations.teams.config.teams_config.get_config")
async def test_send_action_workflow_form_request_status_change_notification(
    mock_get_config: MagicMock, teams_sink: TeamsNotificationSink
) -> None:
    """Test sending action workflow form request status change notification."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config
    # Also set it directly on the sink since private methods check this attribute directly
    teams_sink.teams_connection_config = mock_config

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
            parameters={
                "workflowName": "Data Access Request",
                "actorName": "Jane Smith",
                "result": "APPROVED",
                "entityName": "user_table",
                "entityType": "DATASET",
                "entityPlatform": "snowflake",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_DM",
                id="requester@company.com",
            ),
        ],
    )

    with patch.object(
        teams_sink, "_send_teams_adaptive_card", new_callable=AsyncMock
    ) as mock_send:
        await teams_sink._send_workflow_form_status_change_notification(request)

        # Verify message was sent
        mock_send.assert_called_once()
        args = mock_send.call_args[0]
        recipients, card = args
        assert len(recipients) == 1
        assert recipients[0].id == "requester@company.com"
        assert card["contentType"] == "application/vnd.microsoft.card.adaptive"


def test_build_assertion_status_change_message_failure(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test assertion status change message for failure."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_ASSERTION_STATUS_CHANGE",
            parameters={
                "assertionType": "volume",
                "entityName": "orders_table",
                "entityPath": "/dataset/orders",
                "result": "FAILURE",
                "resultReason": "Row count dropped below expected threshold",
                "description": "Expected at least 1000 rows daily",
            },
        ),
        recipients=[],
    )

    adaptive_card = teams_sink._build_assertion_status_change_message(request)
    content = adaptive_card["content"]

    assert "Assertion Failed" in content["body"][0]["text"]
    assert content["body"][0]["color"] == "Attention"
    assert "❌" in content["body"][0]["text"]
    assert "orders_table" in content["body"][1]["text"]
    assert "Expected at least 1000 rows daily" in content["body"][1]["text"]
    assert "Row count dropped below expected threshold" in content["body"][2]["text"]


def test_build_workflow_form_request_message(teams_sink: TeamsNotificationSink) -> None:
    """Test building workflow form request message."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST",
            parameters={
                "workflowName": "Data Access Request",
                "actorName": "John Doe",
                "entityName": "user_table",
                "entityType": "DATASET",
                "entityPlatform": "snowflake",
            },
        ),
        recipients=[],
    )

    card = teams_sink._build_workflow_form_request_message(request)

    # Verify card structure
    assert card["contentType"] == "application/vnd.microsoft.card.adaptive"
    assert card["content"]["type"] == "AdaptiveCard"
    assert card["content"]["version"] == "1.4"

    # Verify content includes workflow and entity information
    header_text = card["content"]["body"][0]["text"]
    detail_text = card["content"]["body"][1]["text"]

    # Header should contain the workflow name
    assert "Data Access Request" in header_text

    # Details should contain actor and entity information
    assert "John Doe" in detail_text
    assert "user_table" in detail_text


def test_build_workflow_form_status_change_message_approved(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test building workflow form status change message for approved request."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
            parameters={
                "workflowName": "Data Access Request",
                "actorName": "Jane Smith",
                "result": "APPROVED",
                "entityName": "user_table",
                "entityType": "DATASET",
                "entityPlatform": "snowflake",
            },
        ),
        recipients=[],
    )

    card = teams_sink._build_workflow_form_status_change_message(request)

    # Verify card structure
    assert card["contentType"] == "application/vnd.microsoft.card.adaptive"
    assert card["content"]["type"] == "AdaptiveCard"
    assert card["content"]["version"] == "1.4"

    # Verify content includes approval information
    body_text = card["content"]["body"][0]["text"]
    assert "✅" in body_text  # Approved emoji
    assert "APPROVED" in body_text
    assert "Jane Smith" in body_text
    assert card["content"]["body"][0]["color"] == "Good"


def test_build_workflow_form_status_change_message_rejected(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test building workflow form status change message for rejected request."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
            parameters={
                "workflowName": "Data Access Request",
                "actorName": "Jane Smith",
                "result": "REJECTED",
                "entityName": "user_table",
                "entityType": "DATASET",
                "entityPlatform": "snowflake",
            },
        ),
        recipients=[],
    )

    card = teams_sink._build_workflow_form_status_change_message(request)

    # Verify content includes rejection information
    body_text = card["content"]["body"][0]["text"]
    assert "❌" in body_text  # Rejected emoji
    assert "REJECTED" in body_text
    assert "Jane Smith" in body_text
    assert card["content"]["body"][0]["color"] == "Attention"


def test_get_teams_recipients_filters_correctly(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test that Teams recipients are filtered correctly."""
    recipients = [
        NotificationRecipientClass(type="TEAMS_CHANNEL", id="teams-channel"),
        NotificationRecipientClass(type="TEAMS_DM", id="teams-user"),
        NotificationRecipientClass(type="SLACK_CHANNEL", id="slack-channel"),
        NotificationRecipientClass(type="EMAIL", id="email@example.com"),
    ]

    teams_recipients = teams_sink._get_teams_recipients(recipients)

    assert len(teams_recipients) == 2
    assert "teams-channel" in teams_recipients
    assert "teams-user" in teams_recipients
    assert "slack-channel" not in teams_recipients
    assert "email@example.com" not in teams_recipients


def test_format_modifier_type(teams_sink: TeamsNotificationSink) -> None:
    """Test modifier type formatting."""
    assert teams_sink._format_modifier_type("tag(s)") == "Tag(s)"
    assert teams_sink._format_modifier_type("term(s)") == "Term(s)"
    assert teams_sink._format_modifier_type("owner(s)") == "Owner(s)"
    assert teams_sink._format_modifier_type("documentation") == "Documentation"
    assert (
        teams_sink._format_modifier_type("structured property") == "Structured Property"
    )


def test_unsupported_template_type_handling(teams_sink: TeamsNotificationSink) -> None:
    """Test that unsupported template types are handled gracefully."""
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="UNSUPPORTED_TEMPLATE_TYPE",
            parameters={},
        ),
        recipients=[
            NotificationRecipientClass(
                type="TEAMS_CHANNEL",
                id="test-channel",
            ),
        ],
    )

    # Mock valid Teams configuration
    teams_sink.teams_connection_config = MagicMock()
    teams_sink.teams_connection_config.app_details = MagicMock()
    teams_sink.teams_connection_config.app_details.app_id = "test-app-id"
    teams_sink.teams_connection_config.app_details.app_password = "test-app-password"
    teams_sink.teams_connection_config.app_details.tenant_id = "test-tenant-id"

    # Should not raise an exception - should log warning and return gracefully
    teams_sink.send(request, NotificationContext())


@patch("datahub_integrations.teams.config.teams_config.get_config")
def test_all_new_notification_types_supported(mock_get_config: MagicMock) -> None:
    """Test that all new notification types are supported in the routing."""
    # Mock valid Teams configuration
    mock_config = MagicMock()
    mock_config.app_details = MagicMock()
    mock_config.app_details.app_id = "test-app-id"
    mock_config.app_details.app_password = "test-app-password"
    mock_config.app_details.tenant_id = "test-tenant-id"
    mock_get_config.return_value = mock_config

    sink = TeamsNotificationSink()

    # List of all new notification types that should be supported
    new_notification_types = [
        "CUSTOM",
        "BROADCAST_NEW_PROPOSAL",
        "BROADCAST_PROPOSAL_STATUS_CHANGE",
        "BROADCAST_INGESTION_RUN_CHANGE",
        "BROADCAST_ASSERTION_STATUS_CHANGE",
        "BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST",
        "BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
    ]

    for template_type in new_notification_types:
        request = NotificationRequestClass(
            message=NotificationMessageClass(
                template=template_type,
                parameters={"actorName": "Test Actor"},
            ),
            recipients=[],
        )

        # Mock the async methods to avoid actual execution
        with (
            patch.object(sink, "_send_custom_notification") as mock_custom,
            patch.object(sink, "_send_new_proposal_notification") as mock_proposal,
            patch.object(
                sink, "_send_proposal_status_change_notification"
            ) as mock_proposal_status,
            patch.object(
                sink, "_send_ingestion_run_change_notification"
            ) as mock_ingestion,
            patch.object(
                sink, "_send_assertion_status_change_notification"
            ) as mock_assertion,
            patch.object(
                sink, "_send_workflow_form_request_notification"
            ) as mock_workflow_request,
            patch.object(
                sink, "_send_workflow_form_status_change_notification"
            ) as mock_workflow_status,
        ):
            # Should not raise exception - should route to appropriate handler
            sink.send(request, NotificationContext())

            # Verify the correct method was called based on template type
            if template_type == "CUSTOM":
                mock_custom.assert_called_once()
            elif template_type == "BROADCAST_NEW_PROPOSAL":
                mock_proposal.assert_called_once()
            elif template_type == "BROADCAST_PROPOSAL_STATUS_CHANGE":
                mock_proposal_status.assert_called_once()
            elif template_type == "BROADCAST_INGESTION_RUN_CHANGE":
                mock_ingestion.assert_called_once()
            elif template_type == "BROADCAST_ASSERTION_STATUS_CHANGE":
                mock_assertion.assert_called_once()
            elif template_type == "BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST":
                mock_workflow_request.assert_called_once()
            elif (
                template_type == "BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE"
            ):
                mock_workflow_status.assert_called_once()


#     teams_sink: TeamsNotificationSink,
# ) -> None:
#     """Test _send_activity_feed_notification_with_request successful execution."""
#     # Setup notification request
#     notification_request = NotificationRequestClass(
#         scenarioType="ENTITY_TAG_CHANGE",
#         recipients=[NotificationRecipientClass(id="user-id", type="TEAMS_DM")],
#         content={
#             "subject": "Test entity change",
#             "body": "Test entity change notification",
#             "entityName": "test entity",
#             "entityType": "dataset",
#             "platform": "test platform",
#             "action": "added",
#             "modifier": "tag1",
#             "actor": "test-user",
#         },
#     )
#
#     with patch.object(teams_sink, "_send_activity_feed_notification") as mock_send:
#         mock_send.return_value = {"id": "activity-123"}
#
#         result = await teams_sink._send_activity_feed_notification_with_request(
#             notification_request, "user-id"
#         )
#
#         assert result == {"id": "activity-123"}
#         mock_send.assert_called_once()
#
#
def test_get_teams_recipients_success(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _get_teams_recipients successful execution."""
    # Setup notification request
    notification_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            parameters={
                "subject": "Test entity change",
                "body": "Test entity change notification",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                id="user-id", type=NotificationRecipientTypeClass.TEAMS_DM
            ),
            NotificationRecipientClass(
                id="channel-id", type=NotificationRecipientTypeClass.TEAMS_CHANNEL
            ),
        ],
    )

    recipients = teams_sink._get_teams_recipients(notification_request.recipients)  # type: ignore

    assert len(recipients) == 2
    assert "user-id" in recipients
    assert "channel-id" in recipients


def test_get_teams_recipients_empty(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _get_teams_recipients with no Teams recipients."""
    # Setup notification request with no Teams recipients
    notification_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            parameters={
                "subject": "Test entity change",
                "body": "Test entity change notification",
            },
        ),
        recipients=[
            NotificationRecipientClass(
                id="user@example.com", type=NotificationRecipientTypeClass.EMAIL
            )
        ],
    )

    recipients = teams_sink._get_teams_recipients(notification_request.recipients)  # type: ignore

    assert len(recipients) == 0


def test_get_teams_recipients_none(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _get_teams_recipients with None recipients."""
    # Setup notification request with empty recipients
    notification_request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            parameters={
                "subject": "Test entity change",
                "body": "Test entity change notification",
            },
        ),
        recipients=[],
    )

    recipients = teams_sink._get_teams_recipients(notification_request.recipients)  # type: ignore

    assert len(recipients) == 0


class TestTeamsRecipientIdParsing:
    """Test class for Teams recipient ID parsing functionality."""

    def test_parse_teams_recipient_id_json_format(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test parsing JSON format recipient ID."""
        recipient_id = '{"email":"user@example.com","azure":"8ae17cee-2c46-487f-bdd6-dd66770163c2"}'
        result = teams_sink._parse_teams_recipient_id(recipient_id)

        expected = {
            "email": "user@example.com",
            "azure": "8ae17cee-2c46-487f-bdd6-dd66770163c2",
        }
        assert result == expected

    def test_parse_teams_recipient_id_empty_email_json(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test parsing JSON format with empty email field."""
        recipient_id = '{"email":"","azure":"8ae17cee-2c46-487f-bdd6-dd66770163c2"}'
        result = teams_sink._parse_teams_recipient_id(recipient_id)

        expected = {"email": "", "azure": "8ae17cee-2c46-487f-bdd6-dd66770163c2"}
        assert result == expected

    def test_parse_teams_recipient_id_email_format(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test parsing simple email format."""
        recipient_id = "user@example.com"
        result = teams_sink._parse_teams_recipient_id(recipient_id)

        expected = {"email": "user@example.com"}
        assert result == expected

    def test_parse_teams_recipient_id_teams_format(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test parsing Teams ID format."""
        recipient_id = "19:8ae17cee-2c46-487f-bdd6-dd66770163c2_fcfde16d-63d4-49f4-9e4a-60140562e2cb@unq.gbl.spaces"
        result = teams_sink._parse_teams_recipient_id(recipient_id)

        expected = {"teams": recipient_id}
        assert result == expected

    def test_parse_teams_recipient_id_bot_format(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test parsing Bot Framework format."""
        recipient_id = "28:fcfde16d-63d4-49f4-9e4a-60140562e2cb"
        result = teams_sink._parse_teams_recipient_id(recipient_id)

        expected = {"teams": recipient_id}
        assert result == expected

    def test_parse_teams_recipient_id_guid_format(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test parsing simple GUID format."""
        recipient_id = "8ae17cee-2c46-487f-bdd6-dd66770163c2"
        result = teams_sink._parse_teams_recipient_id(recipient_id)

        expected = {"teams": recipient_id}
        assert result == expected

    def test_parse_teams_recipient_id_invalid_json(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test parsing invalid JSON format falls back to simple ID."""
        recipient_id = '{"invalid":"json"'
        result = teams_sink._parse_teams_recipient_id(recipient_id)

        expected = {"teams": recipient_id}
        assert result == expected

    def test_extract_user_guid_from_recipient_azure_id(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test extracting GUID from JSON with azure field."""
        recipient_id = '{"email":"","azure":"8ae17cee-2c46-487f-bdd6-dd66770163c2"}'
        result = teams_sink._extract_user_guid_from_recipient(recipient_id)

        assert result == "8ae17cee-2c46-487f-bdd6-dd66770163c2"

    def test_extract_user_guid_from_recipient_teams_guid(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test extracting GUID from simple GUID format."""
        recipient_id = "8ae17cee-2c46-487f-bdd6-dd66770163c2"
        result = teams_sink._extract_user_guid_from_recipient(recipient_id)

        assert result == "8ae17cee-2c46-487f-bdd6-dd66770163c2"

    def test_extract_user_guid_from_recipient_email(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test extracting GUID from email format."""
        recipient_id = "user@example.com"
        result = teams_sink._extract_user_guid_from_recipient(recipient_id)

        assert result == "user@example.com"

    def test_extract_user_guid_from_recipient_teams_prefixed_id(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test that Teams prefixed IDs are not returned as GUIDs."""
        recipient_id = "19:8ae17cee-2c46-487f-bdd6-dd66770163c2_fcfde16d-63d4-49f4-9e4a-60140562e2cb@unq.gbl.spaces"
        result = teams_sink._extract_user_guid_from_recipient(recipient_id)

        # Should return None since prefixed Teams IDs are not valid for Graph API
        assert result is None

    def test_extract_user_guid_from_recipient_priority_order(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test that azure ID takes priority over other identifiers."""
        recipient_id = '{"email":"user@example.com","azure":"8ae17cee-2c46-487f-bdd6-dd66770163c2","teams":"19:someother_id@unq.gbl.spaces"}'
        result = teams_sink._extract_user_guid_from_recipient(recipient_id)

        # Should return the azure GUID, not the email or teams ID
        assert result == "8ae17cee-2c46-487f-bdd6-dd66770163c2"

    def test_extract_user_guid_from_recipient_no_valid_guid(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test that None is returned when no valid GUID can be extracted."""
        recipient_id = '{"teams":"19:someid@unq.gbl.spaces"}'
        result = teams_sink._extract_user_guid_from_recipient(recipient_id)

        assert result is None

    def test_extract_user_guid_from_recipient_fallback_to_guid_pattern(
        self, teams_sink: TeamsNotificationSink
    ) -> None:
        """Test fallback to GUID pattern matching."""
        recipient_id = "12345678-1234-1234-1234-123456789012"  # Valid GUID pattern
        result = teams_sink._extract_user_guid_from_recipient(recipient_id)

        assert result == "12345678-1234-1234-1234-123456789012"


# Tests for time-based throttling and force refresh behavior
class TestTeamsSinkTimeBasedThrottling:
    """Test cases for time-based throttling and force refresh in Teams sink."""

    @patch("datahub_integrations.teams.config.teams_config.get_config")
    def test_maybe_reload_teams_config_force_refresh(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test that _maybe_reload_teams_config calls get_config with force_refresh=True."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config

        sink = TeamsNotificationSink()

        # Call with force_refresh=True
        sink._maybe_reload_teams_config(force_refresh=True)

        # Verify get_config was called with force_refresh=True
        mock_get_config.assert_called_once_with(force_refresh=True)
        assert sink.teams_connection_config == mock_config

    @patch("datahub_integrations.teams.config.teams_config.get_config")
    def test_maybe_reload_teams_config_normal_refresh(
        self, mock_get_config: MagicMock
    ) -> None:
        """Test that _maybe_reload_teams_config calls get_config with force_refresh=False."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config

        sink = TeamsNotificationSink()

        # Call with force_refresh=False (default)
        sink._maybe_reload_teams_config(force_refresh=False)

        # Verify get_config was called with force_refresh=False
        mock_get_config.assert_called_once_with(force_refresh=False)
        assert sink.teams_connection_config == mock_config

    @patch(
        "datahub_integrations.notifications.sinks.teams.teams_sink.TeamsNotificationSink._is_test_notification"
    )
    @patch("datahub_integrations.teams.config.teams_config.get_config")
    def test_send_method_force_refresh_for_test_notifications(
        self, mock_get_config: MagicMock, mock_is_test_notification: MagicMock
    ) -> None:
        """Test that send() method forces refresh for test notifications."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config
        mock_is_test_notification.return_value = True

        sink = TeamsNotificationSink()

        # Create a CUSTOM notification request
        request = MagicMock()
        request.message.template = "CUSTOM"
        request.message.parameters = {"requestName": "notificationConnectionTest"}

        context = MagicMock()

        # Mock the config validation to pass
        with patch.object(sink, "_check_is_teams_config_valid", return_value=True):
            with patch.object(sink, "_send_custom_notification"):
                sink.send(request, context)

        # Verify get_config was called with force_refresh=True for test notification
        mock_get_config.assert_called_with(force_refresh=True)
        mock_is_test_notification.assert_called_once_with(request.message.parameters)

    @patch(
        "datahub_integrations.notifications.sinks.teams.teams_sink.TeamsNotificationSink._is_test_notification"
    )
    @patch("datahub_integrations.teams.config.teams_config.get_config")
    def test_send_method_normal_refresh_for_regular_notifications(
        self, mock_get_config: MagicMock, mock_is_test_notification: MagicMock
    ) -> None:
        """Test that send() method uses normal refresh for regular notifications."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config
        mock_is_test_notification.return_value = False

        sink = TeamsNotificationSink()

        # Create a regular notification request
        request = MagicMock()
        request.message.template = "CUSTOM"
        request.message.parameters = {"requestName": "regular_notification"}

        context = MagicMock()

        # Mock the config validation to pass
        with patch.object(sink, "_check_is_teams_config_valid", return_value=True):
            with patch.object(sink, "_send_custom_notification"):
                sink.send(request, context)

        # Verify get_config was called with force_refresh=False for regular notification
        mock_get_config.assert_called_with(force_refresh=False)
        mock_is_test_notification.assert_called_once_with(request.message.parameters)

    @patch(
        "datahub_integrations.notifications.sinks.teams.teams_sink.TeamsNotificationSink._is_test_notification"
    )
    @patch("datahub_integrations.teams.config.teams_config.get_config")
    def test_send_method_force_refresh_for_non_custom_test_notifications(
        self, mock_get_config: MagicMock, mock_is_test_notification: MagicMock
    ) -> None:
        """Test that send() method uses normal refresh for non-CUSTOM notifications even if they're test notifications."""
        mock_config = TeamsConnection(webhook_url="https://example.com/webhook")
        mock_get_config.return_value = mock_config
        mock_is_test_notification.return_value = True

        sink = TeamsNotificationSink()

        # Create a non-CUSTOM notification request
        request = MagicMock()
        request.message.template = "NEW_INCIDENT"  # Not CUSTOM
        request.message.parameters = {"requestName": "notificationConnectionTest"}

        context = MagicMock()

        # Mock the config validation to pass
        with patch.object(sink, "_check_is_teams_config_valid", return_value=True):
            with patch.object(sink, "_send_new_incident_notification"):
                sink.send(request, context)

        # Verify get_config was called with force_refresh=False (not CUSTOM template)
        mock_get_config.assert_called_with(force_refresh=False)
        # _is_test_notification should not be called for non-CUSTOM templates
        mock_is_test_notification.assert_not_called()

    def test_is_test_notification_returns_true_for_connection_test(self) -> None:
        """Test that _is_test_notification returns True for notificationConnectionTest."""
        sink = TeamsNotificationSink()

        params = {"requestName": "notificationConnectionTest"}
        result = sink._is_test_notification(params)

        assert result is True

    def test_is_test_notification_returns_false_for_other_requests(self) -> None:
        """Test that _is_test_notification returns False for other request names."""
        sink = TeamsNotificationSink()

        params = {"requestName": "regular_notification"}
        result = sink._is_test_notification(params)

        assert result is False

    def test_is_test_notification_returns_false_for_missing_request_name(self) -> None:
        """Test that _is_test_notification returns False when requestName is missing."""
        sink = TeamsNotificationSink()

        params: dict[str, str] = {}
        result = sink._is_test_notification(params)

        assert result is False


# Tests for error handling improvements


@patch.object(TeamsNotificationSink, "_find_existing_bot_conversation")
@pytest.mark.asyncio
async def test_send_direct_message_raises_when_no_conversation_found(
    mock_find_conversation: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_direct_message raises exception when no conversation exists."""
    # Setup
    mock_find_conversation.return_value = None

    teams_sink.teams_connection_config = TeamsConnection(
        app_details=TeamsAppDetails(
            app_id="test_app_id",
            app_password="test_password",
            tenant_id="test_tenant_id",
        )
    )

    # Execute and verify exception is raised
    with pytest.raises(
        Exception,
        match="No existing conversation found for.*User may need to install the bot",
    ):
        await teams_sink._send_direct_message("user_id", "Test message")  # type: ignore

    # Verify the conversation lookup was attempted
    mock_find_conversation.assert_called_once()


@patch.object(TeamsNotificationSink, "_is_channel_recipient")
@patch.object(TeamsNotificationSink, "_find_existing_bot_conversation")
@pytest.mark.asyncio
async def test_send_direct_adaptive_card_raises_when_no_conversation_found(
    mock_find_conversation: MagicMock,
    mock_is_channel: MagicMock,
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_direct_adaptive_card raises exception when no conversation exists for user DMs."""
    # Setup
    mock_is_channel.return_value = False
    mock_find_conversation.return_value = None

    teams_sink.teams_connection_config = TeamsConnection(
        app_details=TeamsAppDetails(
            app_id="test_app_id",
            app_password="test_password",
            tenant_id="test_tenant_id",
        )
    )

    adaptive_card = {"type": "AdaptiveCard", "body": []}

    # Mock Graph API client to fail (triggering Bot Framework fallback)
    with patch(
        "datahub_integrations.teams.graph_api.GraphApiClient"
    ) as mock_graph_client:
        mock_graph_client.return_value._get_graph_access_token.side_effect = Exception(
            "Failed to create chat: 403"
        )

        # Execute and verify exception is raised
        with pytest.raises(
            Exception,
            match="No existing conversation found for.*User may need to install the bot",
        ):
            await teams_sink._send_direct_adaptive_card("user_id", adaptive_card)  # type: ignore


@pytest.mark.asyncio
async def test_send_adaptive_card_to_recipient_raises_when_no_config(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_adaptive_card_to_recipient raises exception when config is missing."""
    # Setup - no config
    teams_sink.teams_connection_config = None

    adaptive_card = {"type": "AdaptiveCard", "body": []}

    # Execute - should raise exception with clear error message
    with pytest.raises(Exception, match="Teams configuration not available"):
        await teams_sink._send_adaptive_card_to_recipient(
            "user1@example.com", adaptive_card
        )  # type: ignore


@pytest.mark.asyncio
async def test_send_adaptive_card_to_recipient_with_request_raises_when_no_config(
    teams_sink: TeamsNotificationSink,
) -> None:
    """Test _send_adaptive_card_to_recipient_with_request raises exception when config is missing."""
    # Setup - no config
    teams_sink.teams_connection_config = None

    adaptive_card = {"type": "AdaptiveCard", "body": []}
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template="CUSTOM",
            parameters={"title": "Test"},
        ),
        recipients=[],
    )

    # Execute - should raise exception with clear error message
    with pytest.raises(Exception, match="Teams configuration not available"):
        await teams_sink._send_adaptive_card_to_recipient_with_request(  # type: ignore
            "user1@example.com", adaptive_card, request
        )
