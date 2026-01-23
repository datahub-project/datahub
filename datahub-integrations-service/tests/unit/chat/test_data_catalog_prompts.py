"""Unit tests for data catalog prompts and system message building."""

from unittest.mock import Mock, patch

from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.agents.data_catalog_prompts import (
    DataHubSystemPromptBuilder,
)


class TestDataHubSystemPromptBuilder:
    """Test the DataHub system prompt builder."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mock_client = Mock(spec=DataHubClient)
        # Mock the _graph attribute that is accessed during prompt building
        mock_graph = Mock()
        self.mock_client._graph = mock_graph

    def test_base_system_prompt_always_included(self) -> None:
        """Base system prompt is always included regardless of mutation flag."""
        for env_value in ["true", "false", None]:
            env_dict = {"TOOLS_IS_MUTATION_ENABLED": env_value} if env_value else {}
            with patch.dict("os.environ", env_dict, clear=True):
                with patch(
                    "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
                    return_value=None,
                ):
                    builder = DataHubSystemPromptBuilder()
                    system_messages = builder.build_system_messages(self.mock_client)

                    # First message should always be the base system prompt
                    assert len(system_messages) >= 1
                    assert "DataHub AI" in system_messages[0]["text"]
                    assert "Acryl Data" in system_messages[0]["text"]

    def test_context_included_when_provided(self) -> None:
        """Context message is included when context is provided."""
        with patch.dict("os.environ", {}, clear=True):
            with patch(
                "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
                return_value=None,
            ):
                test_context = "User is viewing the Sales Dashboard"
                builder = DataHubSystemPromptBuilder(context=test_context)
                system_messages = builder.build_system_messages(self.mock_client)

                # Extract all text content from system messages
                all_text = " ".join(msg["text"] for msg in system_messages)

                # Verify context is present
                assert test_context in all_text
                assert "The following context is provided from our UI" in all_text

    def test_extra_instructions_override(self) -> None:
        """Extra instructions override works."""
        with patch.dict("os.environ", {"TOOLS_IS_MUTATION_ENABLED": "true"}):
            # Don't patch get_extra_llm_instructions - it should not be called
            override_instructions = "Custom company guidelines"
            builder = DataHubSystemPromptBuilder(
                extra_instructions_override=override_instructions
            )
            system_messages = builder.build_system_messages(self.mock_client)

            # Extract all text content from system messages
            all_text = " ".join(msg["text"] for msg in system_messages)

            # Verify override instructions are present
            assert override_instructions in all_text

    def test_message_ordering(self) -> None:
        """System messages are ordered correctly: base, context, mutations, extra."""
        with patch.dict("os.environ", {"TOOLS_IS_MUTATION_ENABLED": "true"}):
            with patch(
                "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
                return_value="Extra instructions from API",
            ):
                test_context = "User context"
                builder = DataHubSystemPromptBuilder(context=test_context)
                system_messages = builder.build_system_messages(self.mock_client)

                # Should have 4 messages: base, context, extra
                assert len(system_messages) == 3

                # Verify ordering
                assert "DataHub AI" in system_messages[0]["text"]
                assert test_context in system_messages[1]["text"]
                assert "Extra instructions from API" in system_messages[2]["text"]
