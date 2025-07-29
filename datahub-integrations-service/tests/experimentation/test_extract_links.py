import pathlib

import pytest

from datahub_integrations.chat.chat_history import ChatHistory, ToolCallRequest
from datahub_integrations.experimentation.link_eval import (
    extract_datahub_links_from_response,
    extract_urns_from_history,
)


class TestExtractDatahubLinks:
    """Tests for the extract_datahub_links_from_response function."""

    @pytest.fixture
    def test_data_dir(self) -> pathlib.Path:
        """Return the path to test data directory."""
        return pathlib.Path(__file__).parent

    def _get_response_from_history(self, history: ChatHistory) -> str:
        """Extract the response_to_user content from chat history."""
        for message in history.messages:
            if (
                isinstance(message, ToolCallRequest)
                and message.tool_name == "respond_to_user"
                and isinstance(message.tool_input, dict)
                and "response" in message.tool_input
            ):
                return message.tool_input["response"]
        return ""

    def test_mixed_valid_invalid_links(self, test_data_dir: pathlib.Path) -> None:
        """Test response with mix of valid and invalid DataHub links across entity types."""
        # Load chat history with mixed URNs from different tool types
        chat_history_path = test_data_dir / "test_invalid_links.json"
        history = ChatHistory.load_file(chat_history_path)

        # Get response content from the JSON file
        response = self._get_response_from_history(history)

        valid_urns = extract_urns_from_history(history)
        response_urns = extract_datahub_links_from_response(response)

        # Valid URNs should include all URNs from tool results
        expected_valid_urns = {
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)",
            "urn:li:corpuser:john.doe@company.com",
            "urn:li:domain:marketing-domain-123",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,raw_data.pets,PROD)",
        }
        assert set(valid_urns) == expected_valid_urns

        # Invalid URNs should include URNs in response that are not in history
        expected_invalid_urns = {
            "urn:li:dataset:(invalid-urn-not-in-history)",
            "urn:li:corpuser:unknown.user@company.com",
        }
        assert set(response_urns) - set(valid_urns) == expected_invalid_urns

    def test_no_links_in_response(self, test_data_dir: pathlib.Path) -> None:
        """Test response with no DataHub links."""
        # Load chat history
        chat_history_path = test_data_dir / "test_no_links.json"
        history = ChatHistory.load_file(chat_history_path)

        # Get response content from the JSON file
        response = self._get_response_from_history(history)

        valid_urns = extract_urns_from_history(history)
        response_urns = extract_datahub_links_from_response(response)

        # Should have valid URNs from history but no invalid URNs since no links in response
        expected_valid_urns = {
            "urn:li:glossaryTerm:data-governance-term-456",
            "urn:li:corpuser:governance.admin@company.com",
        }
        assert set(valid_urns) == expected_valid_urns
        assert set(response_urns) == set()

    def test_all_valid_links(self, test_data_dir: pathlib.Path) -> None:
        """Test response where all DataHub links are valid."""
        # Load chat history
        chat_history_path = test_data_dir / "test_valid_links.json"
        history = ChatHistory.load_file(chat_history_path)

        # Get response content from the JSON file
        response = self._get_response_from_history(history)

        valid_urns = extract_urns_from_history(history)
        response_urns = extract_datahub_links_from_response(response)

        # All URNs should be valid
        expected_valid_urns = {
            "urn:li:dataset:(urn:li:dataPlatform:tableau,customer_analytics_dashboard,PROD)",
            "urn:li:corpuser:analytics.team@company.com",
            "urn:li:domain:customer-analytics-domain-789",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_metrics,PROD)",
        }
        assert set(valid_urns) == expected_valid_urns
        assert set(response_urns) - set(valid_urns) == set()

    def test_empty_response(self) -> None:
        """Test with empty response."""
        history = ChatHistory(messages=[])
        response = ""

        valid_urns = extract_urns_from_history(history)
        response_urns = extract_datahub_links_from_response(response)

        assert set(valid_urns) == set()
        assert set(response_urns) == set()

    def test_empty_history(self) -> None:
        """Test with empty chat history."""
        history = ChatHistory(messages=[])
        response = "Check out this [dataset](https://company.acryl.io/dataset/urn%3Ali%3Adataset%3A%28some-urn%29)"

        valid_urns = extract_urns_from_history(history)
        response_urns = extract_datahub_links_from_response(response)

        assert set(valid_urns) == set()
        assert set(response_urns) == {"urn:li:dataset:(some-urn)"}
