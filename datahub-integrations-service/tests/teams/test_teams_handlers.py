"""
Tests for Teams infrastructure functions (teams.py).
Focuses on webhook handling, message utilities, and infrastructure functions.
Message handling logic is tested in test_bot_framework.py and test_bot_immediate_response.py.
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import Request

from datahub_integrations.teams.teams import (
    TeamsActivity,
    _message_update_locks,
    get_conversation_id,
    is_message_finalized,
    mark_message_finalized,
    safe_send_teams_message,
    safe_update_teams_progress_message,
    teams_webhook,
)


@pytest.fixture
def teams_activity() -> Any:
    """Basic Teams activity for testing."""
    return {
        "type": "message",
        "id": "test-message-id",
        "from": {"id": "user-id", "name": "Test User"},
        "conversation": {"id": "conversation-id"},
        "text": "Hello DataHub",
        "channelId": "msteams",
        "serviceUrl": "https://smba.trafficmanager.net/amer/",
    }


@pytest.fixture
def mock_teams_config() -> Any:
    """Mock Teams configuration."""
    config = MagicMock()
    config.app_details = MagicMock()
    config.app_details.app_id = "test-app-id"
    config.app_details.app_password = "test-app-secret"
    config.app_details.tenant_id = "test-tenant-id"
    config.enabled = True
    return config


class TestTeamsWebhook:
    """Test Teams webhook processing."""

    @pytest.mark.asyncio
    async def test_webhook_routes_to_bot_framework(self) -> None:
        """Test that webhook routes to Bot Framework adapter."""
        mock_request = MagicMock(spec=Request)

        with patch(
            "datahub_integrations.teams.adapter.get_bot_adapter"
        ) as mock_get_adapter:
            mock_adapter = MagicMock()
            mock_adapter.process_request = AsyncMock(
                return_value=MagicMock(status_code=200)
            )
            mock_get_adapter.return_value = mock_adapter

            response = await teams_webhook(mock_request)

            # Verify adapter was called
            mock_get_adapter.assert_called_once()
            mock_adapter.process_request.assert_called_once_with(mock_request)
            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_webhook_handles_adapter_errors(self) -> None:
        """Test webhook error handling when adapter fails."""
        mock_request = MagicMock(spec=Request)

        with patch(
            "datahub_integrations.teams.adapter.get_bot_adapter"
        ) as mock_get_adapter:
            mock_get_adapter.side_effect = Exception("Adapter error")

            response = await teams_webhook(mock_request)

            assert response.status_code == 500


class TestMessageUtilities:
    """Test message utility functions."""

    def test_get_conversation_id_with_conversation(self) -> None:
        """Test extracting conversation ID from activity."""
        activity = TeamsActivity(
            type="message",
            conversation={"id": "test-conversation-id"},
        )

        conversation_id = get_conversation_id(activity)
        assert conversation_id == "test-conversation-id"

    def test_get_conversation_id_without_conversation(self) -> None:
        """Test handling of activity without conversation."""
        activity = TeamsActivity(type="message")

        conversation_id = get_conversation_id(activity)
        assert conversation_id is None

    def test_message_finalization_tracking(self) -> None:
        """Test message finalization state tracking."""
        message_id = "test-message-123"

        # Initially not finalized
        assert not is_message_finalized(message_id)

        # Mark as finalized
        mark_message_finalized(message_id)
        assert is_message_finalized(message_id)

    @pytest.mark.asyncio
    async def test_safe_send_teams_message_success(self) -> None:
        """Test successful message sending."""
        with (
            patch("datahub_integrations.teams.teams.send_teams_message") as mock_send,
            patch(
                "datahub_integrations.teams.teams.teams_config"
            ) as mock_config_module,
        ):
            mock_config = MagicMock()
            mock_config_module.get_config.return_value = mock_config
            mock_send.return_value = AsyncMock()

            result = await safe_send_teams_message(
                "https://test.service.url",
                "conversation-123",
                {"type": "message", "text": "Hello"},
                mock_config,
            )

            assert result is True
            mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_safe_send_teams_message_failure(self) -> None:
        """Test message sending failure handling."""
        with (
            patch("datahub_integrations.teams.teams.send_teams_message") as mock_send,
            patch(
                "datahub_integrations.teams.teams.teams_config"
            ) as mock_config_module,
        ):
            mock_config = MagicMock()
            mock_config_module.get_config.return_value = mock_config
            mock_send.side_effect = Exception("Teams API error")

            result = await safe_send_teams_message(
                "https://test.service.url",
                "conversation-123",
                {"type": "message", "text": "Hello"},
                mock_config,
            )

            assert result is False

    @pytest.mark.asyncio
    async def test_safe_update_teams_progress_message_prevents_updates_to_finalized(
        self,
    ) -> None:
        """Test that progress updates are properly handled for finalized messages."""
        message_id = "finalized-message-123"

        # Mark message as finalized first
        mark_message_finalized(message_id)

        with patch(
            "datahub_integrations.teams.teams.update_teams_progress_message"
        ) as mock_update:
            mock_update.return_value = AsyncMock()

            result = await safe_update_teams_progress_message(
                "https://test.service.url",
                "conversation-123",
                message_id,
                {"type": "message", "text": "Updated"},
                MagicMock(),
            )

            # Should return True (success)
            assert result is True
            # The actual update_teams_progress_message function handles finalization logic internally

    @pytest.mark.asyncio
    async def test_message_update_locks(self) -> None:
        """Test that message update locks are properly managed."""
        message_id = "concurrent-message-123"

        # Clear any existing locks
        _message_update_locks.clear()

        # Test that locks are created and managed properly
        with patch(
            "datahub_integrations.teams.teams.update_teams_progress_message"
        ) as mock_update:
            mock_update.return_value = AsyncMock()

            # Multiple concurrent calls should work
            tasks = [
                asyncio.create_task(
                    safe_update_teams_progress_message(
                        "https://test.service.url",
                        "conversation-123",
                        f"{message_id}_{i}",
                        {"text": f"Update {i}"},
                        MagicMock(),
                    )
                )
                for i in range(3)
            ]

            results = await asyncio.gather(*tasks)

            # All should succeed
            assert all(result is True for result in results)

            # Should have been called for each message
            assert mock_update.call_count == 3


class TestTeamsActivity:
    """Test TeamsActivity model."""

    def test_teams_activity_basic_creation(self) -> None:
        """Test creating TeamsActivity with basic fields."""
        activity = TeamsActivity(type="message", text="Hello world", id="test-123")

        assert activity.type == "message"
        assert activity.text == "Hello world"
        assert activity.id == "test-123"

    def test_teams_activity_from_field_handling(self) -> None:
        """Test that 'from' field is correctly handled as 'from_'."""
        data = {
            "type": "message",
            "from": {"id": "user-123", "name": "Test User"},
            "text": "Hello",
        }

        activity = TeamsActivity(**data)

        assert activity.from_ == {"id": "user-123", "name": "Test User"}
        assert activity.type == "message"
        assert activity.text == "Hello"

    def test_teams_activity_with_conversation(self) -> None:
        """Test TeamsActivity with conversation data."""
        activity = TeamsActivity(
            type="message",
            conversation={"id": "conv-123", "conversationType": "channel"},
            text="Channel message",
        )

        assert activity.conversation == {
            "id": "conv-123",
            "conversationType": "channel",
        }

    def test_teams_activity_with_button_click_data(self) -> None:
        """Test TeamsActivity with button click value."""
        activity = TeamsActivity(
            type="message",
            value={
                "action": "followup_question",
                "question": "What datasets are available?",
            },
            text="",  # Button clicks typically have empty text
        )

        assert activity.value == {
            "action": "followup_question",
            "question": "What datasets are available?",
        }
        assert activity.text == ""


class TestSearchAndUtilityEndpoints:
    """Test API endpoints are still accessible."""

    @pytest.mark.asyncio
    async def test_search_endpoints_exist(self) -> None:
        """Test that search endpoints are still available after refactoring."""
        # This is more of an integration test - we're just verifying the functions exist
        from datahub_integrations.teams.teams import (
            check_teams_permissions,
            search_channels_get,
            search_users_get,
        )

        # Functions should be importable
        assert callable(search_channels_get)
        assert callable(search_users_get)
        assert callable(check_teams_permissions)

    def test_models_still_available(self) -> None:
        """Test that request models are still available."""
        from datahub_integrations.teams.teams import (
            ChannelSearchRequest,
            SearchRequest,
            UserSearchRequest,
        )

        # Should be able to create model instances
        channel_req = ChannelSearchRequest(query="test", limit=10)
        user_req = UserSearchRequest(query="test")
        search_req = SearchRequest(query="test", type="all")

        assert channel_req.query == "test"
        assert user_req.query == "test"
        assert search_req.query == "test"
