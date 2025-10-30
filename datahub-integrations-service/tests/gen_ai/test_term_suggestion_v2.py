"""
Unit tests for term_suggestion_v2.py, focusing on custom instructions as system messages.
"""

from unittest.mock import patch

import pytest

from datahub_integrations.gen_ai.bedrock import BedrockPromptMessage
from datahub_integrations.gen_ai.term_suggestion_v2 import (
    generate_prompt,
    get_term_recommendations_for_column_splits,
)
from datahub_integrations.gen_ai.term_suggestion_v2_context import GlossaryInfo


class TestGeneratePrompt:
    """Test prompt generation without custom instructions embedded."""

    @pytest.fixture
    def sample_table_info(self):
        """Sample table information for testing."""
        return {
            "name": "users",
            "description": "User information table",
        }

    @pytest.fixture
    def sample_column_info(self):
        """Sample column information for testing."""
        return {
            "email": {
                "column_name": "email",
                "description": "User email address",
                "datatype": "VARCHAR",
                "sample_values": ["user@example.com", "test@test.com"],
            }
        }

    @pytest.fixture
    def sample_glossary_info(self):
        """Sample glossary information for testing."""
        return GlossaryInfo(
            glossary={
                "urn:li:glossaryTerm:PII.Email": {
                    "term_name": "Email",
                    "term_description": "Email address of a person",
                    "parent_node": {
                        "parent_name": "PII",
                        "parent_description": "Personal data",
                    },
                }
            }
        )

    def test_generate_prompt_no_custom_instructions_in_prompt(
        self, sample_table_info, sample_column_info, sample_glossary_info
    ):
        """Test that generate_prompt does not include custom instructions in the prompt text."""

        prompt = generate_prompt(
            table_info=sample_table_info,
            column_info=sample_column_info,
            glossary_info=sample_glossary_info,
            prompt_path=None,
        )

        # Verify custom instructions section is NOT in the prompt
        assert "5. Custom Instructions" not in prompt
        assert "CUSTOM INSTRUCTIONS" not in prompt
        assert "Please take these additional instructions" not in prompt

        # Verify other parts of the prompt are still present
        assert "users" in prompt  # table name
        assert "email" in prompt  # column name
        assert "Email" in prompt  # glossary term
        assert "Remember to adhere strictly" in prompt

    def test_generate_prompt_structure(
        self, sample_table_info, sample_column_info, sample_glossary_info
    ):
        """Test that generated prompt has correct structure."""

        prompt = generate_prompt(
            table_info=sample_table_info,
            column_info=sample_column_info,
            glossary_info=sample_glossary_info,
            prompt_path=None,
        )

        # Verify key sections are present
        assert "Task: Assign Appropriate Glossary Terms" in prompt
        assert "1. Input Information" in prompt
        assert "2. Output Format" in prompt
        assert "3. Confidence Score Guidelines" in prompt
        assert "4. Instructions" in prompt
        assert "Remember to adhere strictly" in prompt

        # Verify data is properly formatted
        assert "users" in prompt
        assert "email" in prompt
        assert "Email" in prompt


class TestCustomInstructionsAsSystemMessages:
    """Test that custom instructions are properly passed as system messages."""

    @pytest.fixture
    def sample_table_info(self):
        """Sample table information for testing."""
        return {
            "name": "users",
            "description": "User information table",
        }

    @pytest.fixture
    def sample_column_info(self):
        """Sample column information for testing."""
        return {
            "email": {
                "column_name": "email",
                "description": "User email address",
                "datatype": "VARCHAR",
                "sample_values": ["user@example.com", "test@test.com"],
            }
        }

    @pytest.fixture
    def sample_glossary_info(self):
        """Sample glossary information for testing."""
        return GlossaryInfo(
            glossary={
                "urn:li:glossaryTerm:PII.Email": {
                    "term_name": "Email",
                    "term_description": "Email address of a person",
                    "parent_node": {
                        "parent_name": "PII",
                        "parent_description": "Personal data",
                    },
                }
            }
        )

    @pytest.mark.asyncio
    async def test_custom_instructions_passed_as_system_messages(
        self, sample_table_info, sample_column_info, sample_glossary_info
    ):
        """Test that custom instructions are formatted and passed as system messages."""

        custom_instructions = (
            "Focus on PII classification and use conservative confidence scores."
        )

        with patch(
            "datahub_integrations.gen_ai.term_suggestion_v2.call_bedrock_llm"
        ) as mock_call_llm:
            # Mock LLM response
            mock_call_llm.return_value = '{"table": [], "email": []}'

            # Call the function
            await get_term_recommendations_for_column_splits(
                column_splits=[["email"]],
                table_info=sample_table_info,
                column_info=sample_column_info,
                glossary_info=sample_glossary_info,
                prompt_path=None,
                custom_instructions=custom_instructions,
            )

            # Verify call_bedrock_llm was called with system_messages
            assert mock_call_llm.called
            call_args = mock_call_llm.call_args

            # Check that system_messages parameter was passed
            assert "system_messages" in call_args.kwargs
            system_messages = call_args.kwargs["system_messages"]

            # Verify system_messages structure
            assert system_messages is not None
            assert len(system_messages) == 1
            assert isinstance(system_messages[0], BedrockPromptMessage)

            # Verify the content
            assert "CUSTOM INSTRUCTIONS" in system_messages[0].text
            assert custom_instructions in system_messages[0].text
            assert (
                "You must follow these in addition to base instructions"
                in system_messages[0].text
            )

    @pytest.mark.asyncio
    async def test_no_system_messages_without_custom_instructions(
        self, sample_table_info, sample_column_info, sample_glossary_info
    ):
        """Test that system_messages is None when custom_instructions is not provided."""

        with patch(
            "datahub_integrations.gen_ai.term_suggestion_v2.call_bedrock_llm"
        ) as mock_call_llm:
            # Mock LLM response
            mock_call_llm.return_value = '{"table": [], "email": []}'

            # Call the function without custom instructions
            await get_term_recommendations_for_column_splits(
                column_splits=[["email"]],
                table_info=sample_table_info,
                column_info=sample_column_info,
                glossary_info=sample_glossary_info,
                prompt_path=None,
                custom_instructions=None,
            )

            # Verify call_bedrock_llm was called
            assert mock_call_llm.called
            call_args = mock_call_llm.call_args

            # Check that system_messages is None
            assert "system_messages" in call_args.kwargs
            assert call_args.kwargs["system_messages"] is None

    @pytest.mark.asyncio
    async def test_no_system_messages_with_empty_custom_instructions(
        self, sample_table_info, sample_column_info, sample_glossary_info
    ):
        """Test that system_messages is None when custom_instructions is empty string."""

        with patch(
            "datahub_integrations.gen_ai.term_suggestion_v2.call_bedrock_llm"
        ) as mock_call_llm:
            # Mock LLM response
            mock_call_llm.return_value = '{"table": [], "email": []}'

            # Call the function with empty custom instructions
            await get_term_recommendations_for_column_splits(
                column_splits=[["email"]],
                table_info=sample_table_info,
                column_info=sample_column_info,
                glossary_info=sample_glossary_info,
                prompt_path=None,
                custom_instructions="",
            )

            # Verify call_bedrock_llm was called
            assert mock_call_llm.called
            call_args = mock_call_llm.call_args

            # Check that system_messages is None for empty string
            assert "system_messages" in call_args.kwargs
            assert call_args.kwargs["system_messages"] is None

    @pytest.mark.asyncio
    async def test_no_system_messages_with_whitespace_only_custom_instructions(
        self, sample_table_info, sample_column_info, sample_glossary_info
    ):
        """Test that system_messages is None when custom_instructions is whitespace-only."""

        with patch(
            "datahub_integrations.gen_ai.term_suggestion_v2.call_bedrock_llm"
        ) as mock_call_llm:
            # Mock LLM response
            mock_call_llm.return_value = '{"table": [], "email": []}'

            # Call the function with whitespace-only custom instructions
            await get_term_recommendations_for_column_splits(
                column_splits=[["email"]],
                table_info=sample_table_info,
                column_info=sample_column_info,
                glossary_info=sample_glossary_info,
                prompt_path=None,
                custom_instructions="   \n  \t  ",
            )

            # Verify call_bedrock_llm was called
            assert mock_call_llm.called
            call_args = mock_call_llm.call_args

            # Check that system_messages is None for whitespace-only
            assert "system_messages" in call_args.kwargs
            assert call_args.kwargs["system_messages"] is None

    @pytest.mark.asyncio
    async def test_multiline_custom_instructions_preserved(
        self, sample_table_info, sample_column_info, sample_glossary_info
    ):
        """Test that multiline custom instructions are preserved in system messages."""

        custom_instructions = """Focus on:
1. PII classification
2. Conservative confidence scores
3. Data privacy considerations"""

        with patch(
            "datahub_integrations.gen_ai.term_suggestion_v2.call_bedrock_llm"
        ) as mock_call_llm:
            # Mock LLM response
            mock_call_llm.return_value = '{"table": [], "email": []}'

            # Call the function
            await get_term_recommendations_for_column_splits(
                column_splits=[["email"]],
                table_info=sample_table_info,
                column_info=sample_column_info,
                glossary_info=sample_glossary_info,
                prompt_path=None,
                custom_instructions=custom_instructions,
            )

            # Verify system_messages content
            call_args = mock_call_llm.call_args
            system_messages = call_args.kwargs["system_messages"]

            # Verify all lines are preserved
            assert "PII classification" in system_messages[0].text
            assert "Conservative confidence scores" in system_messages[0].text
            assert "Data privacy considerations" in system_messages[0].text

    @pytest.mark.asyncio
    async def test_custom_instructions_with_special_characters(
        self, sample_table_info, sample_column_info, sample_glossary_info
    ):
        """Test that custom instructions with special characters are handled correctly."""

        custom_instructions = (
            "Use pattern: email@domain.com for validation. "
            "Confidence >= 8 for exact matches, < 5 for partial."
        )

        with patch(
            "datahub_integrations.gen_ai.term_suggestion_v2.call_bedrock_llm"
        ) as mock_call_llm:
            # Mock LLM response
            mock_call_llm.return_value = '{"table": [], "email": []}'

            # Call the function
            await get_term_recommendations_for_column_splits(
                column_splits=[["email"]],
                table_info=sample_table_info,
                column_info=sample_column_info,
                glossary_info=sample_glossary_info,
                prompt_path=None,
                custom_instructions=custom_instructions,
            )

            # Verify special characters are preserved
            call_args = mock_call_llm.call_args
            system_messages = call_args.kwargs["system_messages"]

            assert "email@domain.com" in system_messages[0].text
            assert ">= 8" in system_messages[0].text
            assert "< 5" in system_messages[0].text

    @pytest.mark.asyncio
    async def test_system_messages_reused_across_splits(
        self, sample_table_info, sample_glossary_info
    ):
        """Test that the same system messages are reused for all column and term splits."""

        custom_instructions = "Focus on PII classification."

        # Create multiple column splits to test reusability
        column_info = {
            "email": {
                "column_name": "email",
                "description": "User email",
                "datatype": "VARCHAR",
            },
            "phone": {
                "column_name": "phone",
                "description": "User phone",
                "datatype": "VARCHAR",
            },
        }

        with patch(
            "datahub_integrations.gen_ai.term_suggestion_v2.call_bedrock_llm"
        ) as mock_call_llm:
            # Mock LLM response
            mock_call_llm.return_value = '{"table": [], "email": [], "phone": []}'

            # Call with multiple column splits
            await get_term_recommendations_for_column_splits(
                column_splits=[["email"], ["phone"]],
                table_info=sample_table_info,
                column_info=column_info,
                glossary_info=sample_glossary_info,
                prompt_path=None,
                custom_instructions=custom_instructions,
            )

            # Verify call_bedrock_llm was called multiple times
            assert mock_call_llm.call_count >= 2

            # Verify all calls use the same system_messages
            first_system_messages = mock_call_llm.call_args_list[0].kwargs[
                "system_messages"
            ]
            for call_args in mock_call_llm.call_args_list:
                assert call_args.kwargs["system_messages"] == first_system_messages
