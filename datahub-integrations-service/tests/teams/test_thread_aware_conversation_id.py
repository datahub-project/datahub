from unittest.mock import Mock

from datahub_integrations.teams.conversation_utils import (
    get_conversation_context_info,
    get_teams_conversation_id,
    is_channel_conversation,
    is_direct_chat_conversation,
)


class TestThreadAwareConversationID:
    """Test thread-aware conversation ID extraction for Teams."""

    def test_direct_chat_conversation_id(self) -> None:
        """Test conversation ID extraction for direct chat (1-on-1 with bot)."""
        # Mock a direct chat activity
        activity = Mock()
        activity.conversation = Mock()
        activity.conversation.id = "19:user123@tenant.com;messageid=1234567890"
        activity.channel_data = None  # No channel data for direct chat

        conversation_id = get_teams_conversation_id(activity)

        # Should return the conversation ID as-is for direct chat
        assert conversation_id == "19:user123@tenant.com;messageid=1234567890"
        assert is_direct_chat_conversation(activity)
        assert not is_channel_conversation(activity)

    def test_channel_conversation_with_thread_id(self) -> None:
        """Test conversation ID extraction for channel conversation with thread ID."""
        # Mock a channel conversation activity
        activity = Mock()
        activity.conversation = Mock()
        activity.conversation.id = "19:channel123@thread.tacv2;messageid=1234567890"
        activity.channel_data = {
            "channelId": "19:channel123@thread.tacv2",
            "threadId": "19:thread456@thread.tacv2",
            "teamId": "team789",
            "tenantId": "tenant123",
        }

        conversation_id = get_teams_conversation_id(activity)

        # Should return conversation ID with thread ID for channel conversations
        expected_id = "19:channel123@thread.tacv2;messageid=1234567890:thread:19:thread456@thread.tacv2"
        assert conversation_id == expected_id
        assert is_channel_conversation(activity)
        assert not is_direct_chat_conversation(activity)

    def test_channel_conversation_without_thread_id(self) -> None:
        """Test conversation ID extraction for channel conversation without thread ID."""
        # Mock a channel conversation activity without thread ID
        activity = Mock()
        activity.conversation = Mock()
        activity.conversation.id = "19:channel123@thread.tacv2;messageid=1234567890"
        activity.channel_data = {
            "channelId": "19:channel123@thread.tacv2",
            "teamId": "team789",
            "tenantId": "tenant123",
            # No threadId
        }

        conversation_id = get_teams_conversation_id(activity)

        # Should return conversation ID with channel marker
        expected_id = "19:channel123@thread.tacv2;messageid=1234567890:channel"
        assert conversation_id == expected_id
        assert is_channel_conversation(activity)
        assert not is_direct_chat_conversation(activity)

    def test_activity_without_conversation(self) -> None:
        """Test handling of activity without conversation information."""
        # Mock an activity without conversation
        activity = Mock()
        activity.conversation = None
        activity.channel_data = None

        conversation_id = get_teams_conversation_id(activity)

        # Should return None
        assert conversation_id is None

    def test_activity_with_empty_conversation(self) -> None:
        """Test handling of activity with empty conversation."""
        # Mock an activity with empty conversation
        activity = Mock()
        activity.conversation = Mock()
        activity.conversation.id = None
        activity.channel_data = None

        conversation_id = get_teams_conversation_id(activity)

        # Should return None
        assert conversation_id is None

    def test_conversation_context_info_direct_chat(self) -> None:
        """Test conversation context info extraction for direct chat."""
        activity = Mock()
        activity.conversation = Mock()
        activity.conversation.id = "19:user123@tenant.com;messageid=1234567890"
        activity.channel_data = None

        context = get_conversation_context_info(activity)

        expected_context = {
            "conversation_id": "19:user123@tenant.com;messageid=1234567890",
            "conversation_type": "direct_chat",
            "is_channel": False,
            "is_direct_chat": True,
        }

        assert context == expected_context

    def test_conversation_context_info_channel(self) -> None:
        """Test conversation context info extraction for channel conversation."""
        activity = Mock()
        activity.conversation = Mock()
        activity.conversation.id = "19:channel123@thread.tacv2;messageid=1234567890"
        activity.channel_data = {
            "channelId": "19:channel123@thread.tacv2",
            "threadId": "19:thread456@thread.tacv2",
            "teamId": "team789",
            "tenantId": "tenant123",
        }

        context = get_conversation_context_info(activity)

        expected_context = {
            "conversation_id": "19:channel123@thread.tacv2;messageid=1234567890",
            "conversation_type": "channel",
            "is_channel": True,
            "is_direct_chat": False,
            "channel_id": "19:channel123@thread.tacv2",
            "thread_id": "19:thread456@thread.tacv2",
            "team_id": "team789",
            "tenant_id": "tenant123",
        }

        assert context == expected_context

    def test_conversation_context_info_no_activity(self) -> None:
        """Test conversation context info extraction with no activity."""
        context = get_conversation_context_info(None)
        assert context == {}

    def test_different_thread_ids_create_different_conversation_ids(self) -> None:
        """Test that different thread IDs create different conversation IDs."""
        base_conversation_id = "19:channel123@thread.tacv2;messageid=1234567890"

        # Mock two activities with different thread IDs
        activity1 = Mock()
        activity1.conversation = Mock()
        activity1.conversation.id = base_conversation_id
        activity1.channel_data = {"threadId": "19:thread456@thread.tacv2"}

        activity2 = Mock()
        activity2.conversation = Mock()
        activity2.conversation.id = base_conversation_id
        activity2.channel_data = {"threadId": "19:thread789@thread.tacv2"}

        conversation_id1 = get_teams_conversation_id(activity1)
        conversation_id2 = get_teams_conversation_id(activity2)

        # Should be different conversation IDs
        assert conversation_id1 != conversation_id2
        assert conversation_id1 is not None
        assert conversation_id2 is not None
        assert conversation_id1.endswith(":thread:19:thread456@thread.tacv2")
        assert conversation_id2.endswith(":thread:19:thread789@thread.tacv2")

    def test_same_thread_id_creates_same_conversation_id(self) -> None:
        """Test that same thread ID creates same conversation ID."""
        base_conversation_id = "19:channel123@thread.tacv2;messageid=1234567890"
        thread_id = "19:thread456@thread.tacv2"

        # Mock two activities with same thread ID
        activity1 = Mock()
        activity1.conversation = Mock()
        activity1.conversation.id = base_conversation_id
        activity1.channel_data = {"threadId": thread_id}

        activity2 = Mock()
        activity2.conversation = Mock()
        activity2.conversation.id = base_conversation_id
        activity2.channel_data = {"threadId": thread_id}

        conversation_id1 = get_teams_conversation_id(activity1)
        conversation_id2 = get_teams_conversation_id(activity2)

        # Should be same conversation ID
        assert conversation_id1 == conversation_id2
        assert conversation_id1 is not None
        assert conversation_id1.endswith(f":thread:{thread_id}")
