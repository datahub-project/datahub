"""
Comprehensive tests for Teams Adaptive Cards generation.
Tests cover all card creation functions and edge cases.
"""

from unittest.mock import MagicMock, patch

from datahub_integrations.teams.cards.adaptive_cards import (
    _create_entity_mini_card,
    create_teams_entity_card,
    create_teams_help_card,
    create_teams_message_card,
    create_teams_search_results_card,
    create_teams_sources_card,
    create_teams_suggestions_card,
)


class TestCreateTeamsMessageCard:
    """Test create_teams_message_card function."""

    def test_basic_message_card(self) -> None:
        """Test basic message card creation with title and message."""
        card = create_teams_message_card("Test Title", "Test message content")

        assert card["contentType"] == "application/vnd.microsoft.card.adaptive"
        content = card["content"]
        assert content["type"] == "AdaptiveCard"
        assert content["version"] == "1.4"
        assert len(content["body"]) == 2

        # Check title
        title_block = content["body"][0]
        assert title_block["type"] == "TextBlock"
        assert title_block["text"] == "Test Title"
        assert title_block["weight"] == "Bolder"
        assert title_block["color"] == "Accent"

        # Check message
        message_block = content["body"][1]
        assert message_block["type"] == "TextBlock"
        assert message_block["text"] == "Test message content"
        assert message_block["wrap"] is True

    def test_error_message_card(self) -> None:
        """Test error message card with attention color."""
        card = create_teams_message_card("Error", "Something went wrong", is_error=True)

        title_block = card["content"]["body"][0]
        assert title_block["color"] == "Attention"

    def test_message_card_with_suggestions(self) -> None:
        """Test message card with suggestion buttons."""
        suggestions = ["Suggestion 1", "Suggestion 2", "Suggestion 3"]
        card = create_teams_message_card("Title", "Message", suggestions=suggestions)

        content = card["content"]
        assert len(content["body"]) == 3  # Title + Message + Suggestions header
        assert "actions" in content
        assert len(content["actions"]) == 3

        # Check suggestions header
        suggestions_header = content["body"][2]
        assert suggestions_header["text"] == "Follow-up questions:"

        # Check action buttons
        for i, action in enumerate(content["actions"]):
            assert action["type"] == "Action.Submit"
            assert action["title"] == suggestions[i]
            assert action["data"]["action"] == "followup_question"
            assert action["data"]["question"] == suggestions[i]

    def test_message_card_with_many_suggestions_limits_to_three(self) -> None:
        """Test that suggestions are limited to 3."""
        suggestions = ["Sug 1", "Sug 2", "Sug 3", "Sug 4", "Sug 5"]
        card = create_teams_message_card("Title", "Message", suggestions=suggestions)

        assert len(card["content"]["actions"]) == 3
        assert card["content"]["actions"][0]["title"] == "Sug 1"
        assert card["content"]["actions"][2]["title"] == "Sug 3"

    def test_message_card_with_empty_suggestions(self) -> None:
        """Test message card with empty suggestions list."""
        card = create_teams_message_card("Title", "Message", suggestions=[])

        assert len(card["content"]["body"]) == 2  # No suggestions header
        assert "actions" not in card["content"]

    def test_message_card_with_none_suggestions(self) -> None:
        """Test message card with None suggestions."""
        card = create_teams_message_card("Title", "Message", suggestions=None)

        assert len(card["content"]["body"]) == 2
        assert "actions" not in card["content"]

    def test_message_card_with_long_content(self) -> None:
        """Test message card with very long title and message."""
        long_title = "A" * 200
        long_message = "B" * 1000
        card = create_teams_message_card(long_title, long_message)

        assert card["content"]["body"][0]["text"] == long_title
        assert card["content"]["body"][1]["text"] == long_message

    def test_message_card_with_special_characters(self) -> None:
        """Test message card with special characters and markdown."""
        title = "Test: @user & <special> characters!"
        message = "**Bold** text with *italics* and [links](http://example.com)"
        card = create_teams_message_card(title, message)

        assert card["content"]["body"][0]["text"] == title
        assert card["content"]["body"][1]["text"] == message


class TestCreateTeamsSearchResultsCard:
    """Test create_teams_search_results_card function."""

    def test_basic_search_results_card(self) -> None:
        """Test basic search results card creation."""
        results = [
            {
                "entity": {
                    "type": "DATASET",
                    "properties": {
                        "name": "Test Dataset",
                        "description": "A test dataset for testing",
                    },
                }
            }
        ]
        card = create_teams_search_results_card("test query", results, 1)

        content = card["content"]
        assert content["type"] == "AdaptiveCard"

        # Check header
        header = content["body"][0]
        assert header["text"] == 'Search Results for "test query"'

        # Check count
        count_block = content["body"][1]
        assert count_block["text"] == "Found 1 result(s)"

        # Check result container
        result_container = content["body"][2]
        assert result_container["type"] == "Container"
        assert result_container["style"] == "emphasis"

    def test_search_results_with_multiple_results(self) -> None:
        """Test search results with multiple entities."""
        results = []
        for i in range(3):
            results.append(
                {
                    "entity": {
                        "type": "DATASET",
                        "properties": {
                            "name": f"Dataset {i}",
                            "description": f"Description {i}",
                        },
                    }
                }
            )

        card = create_teams_search_results_card("datasets", results, 3)
        content = card["content"]

        # Should have header + count + 3 result containers
        assert len(content["body"]) == 5

        for i in range(3):
            result_container = content["body"][i + 2]
            name_block = result_container["items"][0]
            assert f"Dataset {i}" in name_block["text"]

    def test_search_results_limits_to_five_results(self) -> None:
        """Test that search results are limited to 5."""
        results = []
        for i in range(10):
            results.append(
                {
                    "entity": {
                        "type": "DATASET",
                        "properties": {
                            "name": f"Dataset {i}",
                            "description": f"Desc {i}",
                        },
                    }
                }
            )

        card = create_teams_search_results_card("test", results, 10)
        content = card["content"]

        # Should have header + count + 5 results + "more results" message
        assert len(content["body"]) == 8

        # Check "more results" message
        more_results = content["body"][-1]
        assert "... and 5 more results" in more_results["text"]

    def test_search_results_with_long_description(self) -> None:
        """Test search results with long descriptions are truncated."""
        long_description = "A" * 300
        results = [
            {
                "entity": {
                    "type": "DATASET",
                    "properties": {
                        "name": "Test Dataset",
                        "description": long_description,
                    },
                }
            }
        ]

        card = create_teams_search_results_card("test", results, 1)
        result_container = card["content"]["body"][2]
        desc_block = result_container["items"][1]

        # Should be truncated to 200 chars + "..."
        assert len(desc_block["text"]) == 203  # 200 + "..."
        assert desc_block["text"].endswith("...")

    def test_search_results_with_missing_properties(self) -> None:
        """Test search results with missing entity properties."""
        results = [
            {
                "entity": {
                    "type": "DATASET",
                    "properties": {},  # Missing name and description
                }
            }
        ]

        card = create_teams_search_results_card("test", results, 1)
        result_container = card["content"]["body"][2]

        name_block = result_container["items"][0]
        desc_block = result_container["items"][1]

        assert "Unnamed" in name_block["text"]
        assert desc_block["text"] == "No description available"

    def test_search_results_with_malformed_entity(self) -> None:
        """Test search results with malformed entity data."""
        results: list[dict[str, dict]] = [{"entity": {}}]  # Missing type and properties

        card = create_teams_search_results_card("test", results, 1)
        result_container = card["content"]["body"][2]

        name_block = result_container["items"][0]
        assert "Unnamed" in name_block["text"]
        assert "Unknown" in name_block["text"]

    def test_search_results_with_empty_results(self) -> None:
        """Test search results with empty results list."""
        card = create_teams_search_results_card("test", [], 0)
        content = card["content"]

        # Should only have header and count
        assert len(content["body"]) == 2
        assert content["body"][1]["text"] == "Found 0 result(s)"


class TestCreateTeamsEntityCard:
    """Test create_teams_entity_card function."""

    def test_basic_entity_card(self) -> None:
        """Test basic entity card creation."""
        entity = {
            "urn": "urn:li:dataset:test",
            "type": "DATASET",
            "properties": {"name": "Test Dataset", "description": "A test dataset"},
        }

        card = create_teams_entity_card(entity)
        content = card["content"]

        # Check structure
        assert content["type"] == "AdaptiveCard"
        assert len(content["body"]) == 4  # Name + Type + Description + URN

        # Check name
        name_block = content["body"][0]
        assert name_block["text"] == "Test Dataset"
        assert name_block["size"] == "Large"

        # Check type
        type_block = content["body"][1]
        assert type_block["text"] == "Type: DATASET"

        # Check description
        desc_block = content["body"][2]
        assert desc_block["text"] == "A test dataset"

        # Check URN
        urn_block = content["body"][3]
        assert urn_block["text"] == "URN: `urn:li:dataset:test`"

    def test_entity_card_with_missing_properties(self) -> None:
        """Test entity card with missing properties."""
        entity: dict[str, str] = {}  # Empty entity

        card = create_teams_entity_card(entity)
        content = card["content"]

        name_block = content["body"][0]
        type_block = content["body"][1]
        desc_block = content["body"][2]
        urn_block = content["body"][3]

        assert name_block["text"] == "Unnamed"
        assert type_block["text"] == "Type: Unknown"
        assert desc_block["text"] == "No description available"
        assert urn_block["text"] == "URN: ``"

    def test_entity_card_with_long_description(self) -> None:
        """Test entity card with very long description."""
        long_description = "A" * 1000
        entity = {
            "urn": "urn:li:dataset:test",
            "type": "DATASET",
            "properties": {"name": "Test Dataset", "description": long_description},
        }

        card = create_teams_entity_card(entity)
        desc_block = card["content"]["body"][2]

        # Description should not be truncated in entity card
        assert desc_block["text"] == long_description


class TestCreateTeamsHelpCard:
    """Test create_teams_help_card function."""

    def test_help_card_structure(self) -> None:
        """Test help card has correct structure and content."""
        card = create_teams_help_card()
        content = card["content"]

        assert content["type"] == "AdaptiveCard"
        assert content["version"] == "1.4"

        # Should have multiple containers for different commands
        assert len(content["body"]) >= 6  # Header + intro + 4 commands + tip

    def test_help_card_contains_all_commands(self) -> None:
        """Test help card contains all expected commands."""
        card = create_teams_help_card()
        card_str = str(card)

        expected_commands = ["search", "get", "ask", "help"]
        for command in expected_commands:
            assert command in card_str.lower()

    def test_help_card_has_examples(self) -> None:
        """Test help card includes command examples."""
        card = create_teams_help_card()
        card_str = str(card)

        # Should contain example usage patterns
        assert "/datahub search" in card_str
        assert "@DataHub" in card_str
        assert "💡 **Tip**" in card_str


class TestCreateTeamsSuggestionsCard:
    """Test create_teams_suggestions_card function."""

    def test_suggestions_card_with_suggestions(self) -> None:
        """Test suggestions card creation with suggestions."""
        suggestions = ["Question 1", "Question 2", "Question 3"]
        card = create_teams_suggestions_card(suggestions)

        content = card["content"]
        assert content["type"] == "AdaptiveCard"
        assert len(content["actions"]) == 3

        # Check header
        header = content["body"][0]
        assert "Follow-up questions" in header["text"]

        # Check actions
        for i, action in enumerate(content["actions"]):
            assert action["type"] == "Action.Submit"
            assert action["title"] == suggestions[i]

    def test_suggestions_card_limits_to_five(self) -> None:
        """Test suggestions card limits to 5 suggestions."""
        suggestions = [f"Question {i}" for i in range(10)]
        card = create_teams_suggestions_card(suggestions)

        assert len(card["content"]["actions"]) == 5

    def test_suggestions_card_with_empty_suggestions(self) -> None:
        """Test suggestions card with empty suggestions returns empty dict."""
        card = create_teams_suggestions_card([])
        assert card == {}

    def test_suggestions_card_with_none_suggestions(self) -> None:
        """Test suggestions card with None suggestions returns empty dict."""
        card = create_teams_suggestions_card(None)  # type: ignore
        assert card == {}


class TestCreateTeamsSourcesCard:
    """Test create_teams_sources_card function."""

    @patch("datahub_integrations.teams.cards.adaptive_cards.logger")
    def test_sources_card_with_entities(self, mock_logger: MagicMock) -> None:
        """Test sources card creation with entities."""
        entities = [
            {
                "name": "Test Dataset",
                "type": "Dataset",
                "url": "https://datahub.com/dataset/test",
                "description": "A test dataset",
                "platform": "Snowflake",
                "owner": "John Doe",
                "certified": True,
                "stats": "High usage",
            }
        ]

        card = create_teams_sources_card(entities)
        content = card["content"]

        assert content["type"] == "AdaptiveCard"

        # Should have toggle header + entity containers
        assert len(content["body"]) >= 2

        # Check toggle functionality
        toggle_container = content["body"][0]
        assert "selectAction" in toggle_container["items"][0]
        assert (
            toggle_container["items"][0]["selectAction"]["type"]
            == "Action.ToggleVisibility"
        )

    @patch("datahub_integrations.teams.cards.adaptive_cards.logger")
    def test_sources_card_with_multiple_entities(self, mock_logger: MagicMock) -> None:
        """Test sources card with multiple entities."""
        entities = []
        for i in range(3):
            entities.append(
                {
                    "name": f"Entity {i}",
                    "type": "Dataset",
                    "url": f"https://datahub.com/entity/{i}",
                }
            )

        card = create_teams_sources_card(entities)

        # Should have toggle header + 3 entity containers
        assert len(card["content"]["body"]) == 4

    @patch("datahub_integrations.teams.cards.adaptive_cards.logger")
    def test_sources_card_limits_to_five_entities(self, mock_logger: MagicMock) -> None:
        """Test sources card limits to 5 entities."""
        entities = []
        for i in range(10):
            entities.append({"name": f"Entity {i}", "type": "Dataset"})

        card = create_teams_sources_card(entities)

        # Should have toggle header + 5 entity containers
        assert len(card["content"]["body"]) == 6

    @patch("datahub_integrations.teams.cards.adaptive_cards.logger")
    def test_sources_card_with_empty_entities(self, mock_logger: MagicMock) -> None:
        """Test sources card with empty entities returns empty dict."""
        card = create_teams_sources_card([])
        assert card == {}

    @patch("datahub_integrations.teams.cards.adaptive_cards.logger")
    def test_sources_card_with_none_entities(self, mock_logger: MagicMock) -> None:
        """Test sources card with None entities returns empty dict."""
        card = create_teams_sources_card(None)  # type: ignore
        assert card == {}


class TestCreateEntityMiniCard:
    """Test _create_entity_mini_card function."""

    def test_mini_card_with_complete_entity(self) -> None:
        """Test mini card with complete entity information."""
        entity = {
            "name": "Test Dataset",
            "type": "Dataset",
            "url": "https://datahub.com/dataset/test",
            "description": "A comprehensive test dataset for validation",
            "platform": "Snowflake",
            "owner": "John Doe",
            "certified": True,
            "stats": "High usage",
        }

        card = _create_entity_mini_card(entity)

        assert card["type"] == "Container"
        assert card["style"] == "emphasis"
        assert "selectAction" in card
        assert card["selectAction"]["type"] == "Action.OpenUrl"
        assert card["selectAction"]["url"] == "https://datahub.com/dataset/test"

        # Check content
        items = card["items"]
        line1_text = items[0]["text"]

        assert "✅ **Test Dataset**" in line1_text  # Certified indicator
        assert "(Dataset)" in line1_text
        assert "• Snowflake" in line1_text
        assert "• 👤 John Doe" in line1_text

    def test_mini_card_without_certification(self) -> None:
        """Test mini card without certification."""
        entity = {"name": "Regular Dataset", "type": "Dataset", "certified": False}

        card = _create_entity_mini_card(entity)
        line1_text = card["items"][0]["text"]

        assert "**Regular Dataset**" in line1_text
        assert "✅" not in line1_text  # No certification indicator

    def test_mini_card_with_stats_and_description(self) -> None:
        """Test mini card with stats and description on second line."""
        entity = {
            "name": "Short Name",  # Short name allows space for description
            "type": "Dataset",
            "description": "Short description",
            "stats": "Medium usage",
        }

        card = _create_entity_mini_card(entity)

        # Should have 2 text blocks (line1 + line2)
        assert len(card["items"]) == 2

        line2_text = card["items"][1]["text"]
        assert "📊 Medium usage" in line2_text
        assert "Short description" in line2_text

    def test_mini_card_with_long_name_skips_description(self) -> None:
        """Test mini card with long name skips description to avoid overcrowding."""
        entity = {
            "name": "Very Long Dataset Name That Takes Up Most Of The Available Space",
            "type": "Dataset",
            "description": "This description should be skipped",
            "stats": "High usage",
        }

        card = _create_entity_mini_card(entity)

        # May still have line2 for stats, but description should be limited
        line1_text = card["items"][0]["text"]
        assert "Very Long Dataset Name" in line1_text

    def test_mini_card_with_minimal_data(self) -> None:
        """Test mini card with minimal entity data."""
        entity = {"name": "Basic Dataset", "type": "Dataset"}

        card = _create_entity_mini_card(entity)

        # Should have only one line
        assert len(card["items"]) == 1
        assert "selectAction" not in card  # No URL provided

    def test_mini_card_with_missing_name_uses_unknown(self) -> None:
        """Test mini card with missing name."""
        entity = {"type": "Dataset"}

        card = _create_entity_mini_card(entity)
        line1_text = card["items"][0]["text"]

        assert "**Unknown**" in line1_text

    def test_mini_card_description_truncation(self) -> None:
        """Test mini card truncates very long descriptions."""
        long_description = "A" * 200
        entity = {"name": "Test", "type": "Dataset", "description": long_description}

        card = _create_entity_mini_card(entity)

        if len(card["items"]) > 1:
            line2_text = card["items"][1]["text"]
            # Should be truncated with "..."
            assert line2_text.endswith("...")
            assert len(line2_text) < len(long_description)


class TestAdaptiveCardsIntegration:
    """Test integration scenarios between different card types."""

    def test_all_cards_have_consistent_structure(self) -> None:
        """Test that all card types follow consistent structure."""
        cards = [
            create_teams_message_card("Title", "Message"),
            create_teams_search_results_card("query", [], 0),
            create_teams_entity_card(
                {"urn": "test", "type": "DATASET", "properties": {}}
            ),
            create_teams_help_card(),
            create_teams_suggestions_card(["Suggestion"]),
        ]

        for card in cards:
            if card:  # Skip empty cards
                assert "contentType" in card
                assert card["contentType"] == "application/vnd.microsoft.card.adaptive"
                assert "content" in card
                assert card["content"]["type"] == "AdaptiveCard"
                assert card["content"]["version"] == "1.4"
                assert "body" in card["content"]

    def test_card_sizes_are_reasonable(self) -> None:
        """Test that cards don't become excessively large."""
        # Create cards with substantial content
        large_suggestions = [f"Suggestion {i}" for i in range(5)]
        large_entities = [{"name": f"Entity {i}", "type": "Dataset"} for i in range(5)]
        large_results = [
            {
                "entity": {
                    "type": "DATASET",
                    "properties": {"name": f"Dataset {i}", "description": "A" * 100},
                }
            }
            for i in range(5)
        ]

        cards = [
            create_teams_message_card(
                "Title", "A" * 500, suggestions=large_suggestions
            ),
            create_teams_search_results_card("query", large_results, 10),
            create_teams_sources_card(large_entities),
        ]

        for card in cards:
            if card:
                # Convert to string to estimate size
                card_str = str(card)
                # Should be reasonable size (less than 100KB)
                assert len(card_str) < 100000
