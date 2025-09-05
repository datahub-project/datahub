"""
Tests for bot name caching and mention detection in Teams Bot Framework integration.

These tests cover:
- Bot name initialization and caching
- Display name extraction from mention entities
- Dynamic bot name usage in responses
- Mention detection logic with various entity structures
"""

from unittest.mock import MagicMock

import pytest
from botbuilder.core import TurnContext
from botbuilder.schema import Activity, ChannelAccount

from datahub_integrations.teams.bot import DataHubTeamsBot


class TestBotNameCaching:
    """Test bot name caching functionality."""

    @pytest.fixture
    def bot(self) -> DataHubTeamsBot:
        """Create a DataHubTeamsBot instance for testing."""
        return DataHubTeamsBot()

    def test_set_bot_name_from_config_with_app_id(self, bot: DataHubTeamsBot) -> None:
        """Test setting bot name from app_id during initialization."""
        app_id = "fcfde16d-63d4-49f4-9e4a-60140562e2cb"

        bot.set_bot_name_from_config(app_id)

        assert bot._cached_bot_name == app_id

    def test_set_bot_name_from_config_no_app_id(self, bot: DataHubTeamsBot) -> None:
        """Test handling of missing app_id."""
        bot.set_bot_name_from_config(None)

        assert bot._cached_bot_name is None

    def test_set_bot_name_from_config_already_cached(
        self, bot: DataHubTeamsBot
    ) -> None:
        """Test that cached name is not overwritten if already set."""
        initial_name = "Initial Bot Name"
        bot._cached_bot_name = initial_name

        bot.set_bot_name_from_config("new-app-id")

        # Should not overwrite existing cached name
        assert bot._cached_bot_name == initial_name


class TestMentionDetection:
    """Test mention detection with various entity structures."""

    @pytest.fixture
    def bot(self) -> DataHubTeamsBot:
        """Create a DataHubTeamsBot instance for testing."""
        return DataHubTeamsBot()

    def create_activity_with_mention_entities(
        self,
        bot_id: str = "28:bot-id",
        mentioned_name: str = "DataHub Dev Bot",
        use_additional_properties: bool = True,
    ) -> Activity:
        """Helper to create activity with mention entities."""
        activity = Activity(
            type="message",
            text="<at>DataHub Dev Bot</at> help me",
            recipient=ChannelAccount(id=bot_id, name="bot-internal-name"),
        )

        # Create mention entity
        mention_entity = MagicMock()
        mention_entity.type = "mention"

        if use_additional_properties:
            # Bot Framework Python SDK pattern - data in additional_properties
            mention_entity.additional_properties = {
                "mentioned": {"id": bot_id, "name": mentioned_name},
                "text": f"<at>{mentioned_name}</at>",
            }
            # No direct 'mentioned' attribute - should raise AttributeError
            del mention_entity.mentioned
        else:
            # Standard Bot Framework pattern (if it existed)
            mention_entity.mentioned = MagicMock()
            mention_entity.mentioned.id = bot_id
            mention_entity.mentioned.name = mentioned_name
            mention_entity.additional_properties = {}

        activity.entities = [mention_entity]
        return activity

    def test_is_mention_with_additional_properties(self, bot: DataHubTeamsBot) -> None:
        """Test mention detection using additional_properties structure."""
        bot_id = "28:bot-id"
        activity = self.create_activity_with_mention_entities(
            bot_id=bot_id,
            mentioned_name="DataHub Dev Bot",
            use_additional_properties=True,
        )

        result = bot._is_mention(activity)

        assert result is True

    def test_is_mention_with_standard_mentioned_attribute(
        self, bot: DataHubTeamsBot
    ) -> None:
        """Test mention detection using standard mentioned attribute."""
        bot_id = "28:bot-id"
        activity = self.create_activity_with_mention_entities(
            bot_id=bot_id,
            mentioned_name="DataHub Dev Bot",
            use_additional_properties=False,
        )

        result = bot._is_mention(activity)

        assert result is True

    def test_is_mention_wrong_bot_id(self, bot: DataHubTeamsBot) -> None:
        """Test mention detection when mentioned bot has different ID."""
        activity = self.create_activity_with_mention_entities(
            bot_id="28:different-bot-id",  # Different from recipient ID
            mentioned_name="Other Bot",
        )
        activity.recipient.id = "28:our-bot-id"

        result = bot._is_mention(activity)

        assert result is False

    def test_is_mention_no_entities(self, bot: DataHubTeamsBot) -> None:
        """Test mention detection with no entities."""
        activity = Activity(
            type="message",
            text="hello",
            recipient=ChannelAccount(id="28:bot-id"),
            entities=None,
        )

        result = bot._is_mention(activity)

        assert result is False

    def test_is_mention_no_mention_entities(self, bot: DataHubTeamsBot) -> None:
        """Test mention detection with entities but no mention types."""
        activity = Activity(
            type="message",
            text="hello",
            recipient=ChannelAccount(id="28:bot-id"),
            entities=[MagicMock(type="clientInfo")],  # Non-mention entity
        )

        result = bot._is_mention(activity)

        assert result is False

    def test_is_mention_multiple_mentions_bot_included(
        self, bot: DataHubTeamsBot
    ) -> None:
        """Test mention detection with multiple mentions including the bot."""
        bot_id = "28:bot-id"
        activity = Activity(
            type="message",
            text="<at>DataHub</at> and <at>User</at> help me",
            recipient=ChannelAccount(id=bot_id),
        )

        # Create multiple mention entities
        bot_mention = MagicMock()
        bot_mention.type = "mention"
        bot_mention.additional_properties = {
            "mentioned": {"id": bot_id, "name": "DataHub"},
            "text": "<at>DataHub</at>",
        }
        del bot_mention.mentioned

        user_mention = MagicMock()
        user_mention.type = "mention"
        user_mention.additional_properties = {
            "mentioned": {"id": "user-id", "name": "User"},
            "text": "<at>User</at>",
        }
        del user_mention.mentioned

        activity.entities = [bot_mention, user_mention]

        result = bot._is_mention(activity)

        assert result is True

    def test_is_mention_text_fallback(self, bot: DataHubTeamsBot) -> None:
        """Test mention detection fallback to text parsing."""
        bot_name = "DataHub Bot"
        activity = Activity(
            type="message",
            text=f"<at>{bot_name}</at> help me",
            recipient=ChannelAccount(id="28:bot-id", name=bot_name),
            entities=[],  # No entities to parse
        )

        result = bot._is_mention(activity)

        assert result is True


class TestDisplayNameExtraction:
    """Test display name extraction from mention entities."""

    @pytest.fixture
    def bot(self) -> DataHubTeamsBot:
        """Create a DataHubTeamsBot instance for testing."""
        return DataHubTeamsBot()

    def test_extract_bot_display_name_from_additional_properties(
        self, bot: DataHubTeamsBot
    ) -> None:
        """Test extracting display name from additional_properties."""
        bot_id = "28:bot-id"
        display_name = "DataHub Dev sdas Aug 14 07:10"

        activity = Activity(recipient=ChannelAccount(id=bot_id))

        mention_entity = MagicMock()
        mention_entity.type = "mention"
        mention_entity.additional_properties = {
            "mentioned": {"id": bot_id, "name": display_name}
        }
        del mention_entity.mentioned

        activity.entities = [mention_entity]

        result = bot._extract_bot_display_name_from_mentions(activity)

        assert result == display_name

    def test_extract_bot_display_name_from_standard_attribute(
        self, bot: DataHubTeamsBot
    ) -> None:
        """Test extracting display name from standard mentioned attribute."""
        bot_id = "28:bot-id"
        display_name = "DataHub Production Bot"

        activity = Activity(recipient=ChannelAccount(id=bot_id))

        mention_entity = MagicMock()
        mention_entity.type = "mention"
        mention_entity.mentioned = MagicMock()
        mention_entity.mentioned.id = bot_id
        mention_entity.mentioned.name = display_name

        activity.entities = [mention_entity]

        result = bot._extract_bot_display_name_from_mentions(activity)

        assert result == display_name

    def test_extract_bot_display_name_no_entities(self, bot: DataHubTeamsBot) -> None:
        """Test extraction when no entities present."""
        activity = Activity(entities=None)

        result = bot._extract_bot_display_name_from_mentions(activity)

        assert result is None

    def test_extract_bot_display_name_wrong_bot_id(self, bot: DataHubTeamsBot) -> None:
        """Test extraction when mention is for different bot."""
        activity = Activity(recipient=ChannelAccount(id="28:our-bot-id"))

        mention_entity = MagicMock()
        mention_entity.type = "mention"
        mention_entity.additional_properties = {
            "mentioned": {
                "id": "28:other-bot-id",  # Different bot
                "name": "Other Bot",
            }
        }
        del mention_entity.mentioned

        activity.entities = [mention_entity]

        result = bot._extract_bot_display_name_from_mentions(activity)

        assert result is None


class TestBotNameCacheIntegration:
    """Test the complete bot name caching flow."""

    @pytest.fixture
    def bot(self) -> DataHubTeamsBot:
        """Create a DataHubTeamsBot instance for testing."""
        return DataHubTeamsBot()

    @pytest.fixture
    def turn_context(self) -> MagicMock:
        """Create a mock TurnContext."""
        context = MagicMock(spec=TurnContext)
        return context

    def test_get_cached_bot_name_first_time_from_mention(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test getting bot name for the first time from mention entities."""
        bot_id = "28:bot-id"
        display_name = "DataHub Dev sdas Aug 14 07:10"
        initial_name = "fcfde16d-63d4-49f4-9e4a-60140562e2cb"

        # Set initial cached name (like from app_id)
        bot._cached_bot_name = initial_name

        # Create activity with mention
        mention_entity = MagicMock()
        mention_entity.type = "mention"
        mention_entity.additional_properties = {
            "mentioned": {"id": bot_id, "name": display_name}
        }
        del mention_entity.mentioned

        turn_context.activity = Activity(
            recipient=ChannelAccount(id=bot_id), entities=[mention_entity]
        )

        result = bot._get_cached_bot_name(turn_context)

        assert result == display_name
        assert bot._cached_bot_name == display_name

    def test_get_cached_bot_name_already_cached(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test getting bot name when already cached."""
        bot_id = "28:bot-id"
        cached_name = "DataHub Dev Bot"

        # Set cached name
        bot._cached_bot_name = cached_name

        # Create activity with same display name
        mention_entity = MagicMock()
        mention_entity.type = "mention"
        mention_entity.additional_properties = {
            "mentioned": {"id": bot_id, "name": cached_name}
        }
        del mention_entity.mentioned

        turn_context.activity = Activity(
            recipient=ChannelAccount(id=bot_id), entities=[mention_entity]
        )

        result = bot._get_cached_bot_name(turn_context)

        assert result == cached_name
        assert bot._cached_bot_name == cached_name

    def test_get_cached_bot_name_fallback_to_recipient(
        self, bot: DataHubTeamsBot, turn_context: MagicMock
    ) -> None:
        """Test fallback to recipient name when no mention entities."""
        recipient_name = "datahub-teams-bot-internal"
        initial_name = "app-id"

        bot._cached_bot_name = initial_name

        turn_context.activity = Activity(
            recipient=ChannelAccount(id="28:bot-id", name=recipient_name),
            entities=[],  # No mention entities
        )

        result = bot._get_cached_bot_name(turn_context)

        assert result == recipient_name
        assert bot._cached_bot_name == recipient_name


class TestFinalResponseBuilding:
    """Test building final responses with dynamic bot names."""

    @pytest.fixture
    def bot(self) -> DataHubTeamsBot:
        """Create a DataHubTeamsBot instance for testing."""
        return DataHubTeamsBot()

    def test_build_teams_final_response_with_bot_name(
        self, bot: DataHubTeamsBot
    ) -> None:
        """Test building response with dynamic bot name."""
        response_text = "I can help you with that!"
        followup_suggestions = ["Tell me more", "Show examples"]
        bot_name = "DataHub Dev sdas Aug 14 07:10"

        main_message, suggestions_message = bot._build_teams_final_response(
            response_text, followup_suggestions, bot_name
        )

        # Check main message content
        assert main_message.text.startswith(response_text)
        assert f"Mention @{bot_name} for responses" in main_message.text
        assert "Was this helpful?" in main_message.text

    def test_build_teams_final_response_no_bot_name(self, bot: DataHubTeamsBot) -> None:
        """Test building response with fallback bot name."""
        response_text = "Here's your answer!"

        main_message, suggestions_message = bot._build_teams_final_response(
            response_text, [], None
        )

        # Should use fallback @DataHub
        assert "Mention @DataHub for responses" in main_message.text

    def test_build_teams_final_response_no_user_mention(
        self, bot: DataHubTeamsBot
    ) -> None:
        """Test that response doesn't include user mentions or salutations."""
        response_text = "This is the AI response."

        main_message, suggestions_message = bot._build_teams_final_response(
            response_text, [], "DataHub Bot"
        )

        # Should start directly with response text, no user mentions
        assert main_message.text.startswith(response_text)
        # Should not contain user mention patterns
        assert not main_message.text.startswith("<at>")
        assert not main_message.text.startswith("Hey!")

    def test_build_teams_final_response_with_suggestions(
        self, bot: DataHubTeamsBot
    ) -> None:
        """Test building response with follow-up suggestions."""
        response_text = "Here's information about datasets."
        suggestions = ["Show lineage", "List owners", "View schema"]

        main_message, suggestions_message = bot._build_teams_final_response(
            response_text, suggestions, "DataHub"
        )

        # Should have both main message and suggestions
        assert main_message is not None
        assert suggestions_message is not None

        # Main message should not contain suggestions
        assert main_message.text.startswith(response_text)

        # Suggestions should be in separate message
        assert suggestions_message.attachments is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
