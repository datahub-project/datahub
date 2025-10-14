"""Tests for documentation generation with extra instructions."""

from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.gen_ai.description_v3 import (
    generate_table_description,
    get_extra_documentation_instructions,
)
from datahub_integrations.gen_ai.litellm import LiteLLM, LiteLLMModel
from datahub_integrations.gen_ai.suggest_query_description import generate_query_desc


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    """Clear the TTL cache before each test."""
    # Clear cache for the get_extra_documentation_instructions function
    if hasattr(get_extra_documentation_instructions, "__wrapped__"):
        # Access the cache dictionary directly
        cache = getattr(
            get_extra_documentation_instructions.__wrapped__, "__self__", None
        )
        if cache and hasattr(cache, "clear"):
            cache.clear()
    # Since cachetools.cached decorator stores cache as function attribute
    if hasattr(get_extra_documentation_instructions, "cache"):
        get_extra_documentation_instructions.cache.clear()
    elif hasattr(get_extra_documentation_instructions, "__cached__"):
        get_extra_documentation_instructions.__cached__.clear()


class TestExtraDocumentationInstructions:
    """Test the get_extra_documentation_instructions function."""

    def test_get_extra_documentation_instructions_success(self) -> None:
        """Test successful retrieval of documentation AI instructions."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "globalSettings": {
                "documentationAi": {
                    "instructions": [
                        {
                            "id": "1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Always use formal language",
                        }
                    ]
                }
            }
        }

        result = get_extra_documentation_instructions(mock_graph)

        assert result == "Always use formal language"
        mock_graph.execute_graphql.assert_called_once()

    def test_get_extra_documentation_instructions_no_settings(self) -> None:
        """Test when no global settings are returned."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {}

        result = get_extra_documentation_instructions(mock_graph)

        assert result is None
        mock_graph.execute_graphql.assert_called_once()

    def test_get_extra_documentation_instructions_no_documentation_ai(self) -> None:
        """Test when global settings exist but no documentation AI settings."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {"globalSettings": {}}

        result = get_extra_documentation_instructions(mock_graph)

        assert result is None

    def test_get_extra_documentation_instructions_empty_instructions(self) -> None:
        """Test when instructions array is empty."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "globalSettings": {"documentationAi": {"instructions": []}}
        }

        result = get_extra_documentation_instructions(mock_graph)

        assert result is None

    def test_get_extra_documentation_instructions_takes_last_active(self) -> None:
        """Test that it takes the last ACTIVE GENERAL_CONTEXT instruction."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "globalSettings": {
                "documentationAi": {
                    "instructions": [
                        {
                            "id": "1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "First instruction",
                        },
                        {
                            "id": "2",
                            "type": "GENERAL_CONTEXT",
                            "state": "INACTIVE",
                            "instruction": "Inactive instruction",
                        },
                        {
                            "id": "3",
                            "type": "SPECIFIC_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Wrong type",
                        },
                        {
                            "id": "4",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Last active instruction",
                        },
                    ]
                }
            }
        }

        result = get_extra_documentation_instructions(mock_graph)

        assert result == "Last active instruction"

    def test_get_extra_documentation_instructions_graphql_failure(self) -> None:
        """Test that GraphQL failures are re-raised."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.side_effect = Exception("GraphQL error")

        with pytest.raises(Exception) as exc_info:
            get_extra_documentation_instructions(mock_graph)

        assert "Failed to fetch documentation AI instructions from GraphQL" in str(
            exc_info.value
        )
        assert "GraphQL error" in str(exc_info.value)

    def test_get_extra_documentation_instructions_caching(self) -> None:
        """Test that results are cached and GraphQL is called only once."""
        mock_graph = MagicMock(spec=DataHubGraph)
        mock_graph.execute_graphql.return_value = {
            "globalSettings": {
                "documentationAi": {
                    "instructions": [
                        {
                            "id": "1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Cached instruction",
                        }
                    ]
                }
            }
        }

        # First call
        result1 = get_extra_documentation_instructions(mock_graph)
        assert result1 == "Cached instruction"

        # Second call should use cache
        result2 = get_extra_documentation_instructions(mock_graph)
        assert result2 == "Cached instruction"

        # GraphQL should only be called once due to caching
        mock_graph.execute_graphql.assert_called_once()


class TestTableDescriptionWithInstructions:
    """Test table description generation with extra instructions."""

    @patch("datahub_integrations.gen_ai.litellm.LiteLLM.call_lite_llm")
    def test_generate_table_description_with_instructions(
        self, mock_llm: MagicMock
    ) -> None:
        """Test that extra instructions are included in table description prompt."""
        from pydantic import ConfigDict

        from datahub_integrations.gen_ai.description_context import (
            ColumnMetadataInfo,
            TableInfo,
        )

        # TODO: Fix test data - these models don't have 'table', 'dataset', 'platform' fields
        # Temporarily allow extra fields for pydantic v1→v2 compatibility
        TableInfo.model_config = ConfigDict(extra="ignore")
        ColumnMetadataInfo.model_config = ConfigDict(extra="ignore")

        # Mock LLM response with expected markdown format
        mock_llm.return_value = "### Table Description\nA test table"

        # Sample data
        table_info = TableInfo(
            table={"name": "test_table"},  # type: ignore[call-arg]
            dataset={"name": "test_dataset"},  # type: ignore[call-arg]
            platform={"name": "test_platform"},  # type: ignore[call-arg]
        )
        column_infos = {
            "col1": ColumnMetadataInfo(name="col1", type="STRING"),  # type: ignore[call-arg]
        }

        litellm = LiteLLM(LiteLLMModel.CLAUDE_3_HAIKU, 500, 0.3)

        # Call with extra instructions
        result = generate_table_description(
            litellm,
            table_info,
            column_infos,
            extra_instructions="Always mention data quality",
        )

        # Verify the result
        assert result == "### Table Description\nA test table"

        # Verify the LLM was called
        mock_llm.assert_called_once()

        # Check that the prompt messages include extra instructions
        call_args = mock_llm.call_args
        prompt_messages = call_args[1]["prompt"]

        # Find the extra instructions message
        found_extra_instructions = False
        for msg in prompt_messages:
            if hasattr(msg, "text") and "ADDITIONAL REQUIREMENTS:" in msg.text:
                assert "Always mention data quality" in msg.text
                found_extra_instructions = True
                break

        assert found_extra_instructions, (
            "Extra instructions not found in prompt messages"
        )

    @patch("datahub_integrations.gen_ai.litellm.LiteLLM.call_lite_llm")
    def test_generate_table_description_without_instructions(
        self, mock_llm: MagicMock
    ) -> None:
        """Test that table description works without extra instructions."""
        from pydantic import ConfigDict

        from datahub_integrations.gen_ai.description_context import (
            ColumnMetadataInfo,
            TableInfo,
        )

        # TODO: Fix test data - these models don't have 'table', 'dataset', 'platform' fields
        # Temporarily allow extra fields for pydantic v1→v2 compatibility
        TableInfo.model_config = ConfigDict(extra="ignore")
        ColumnMetadataInfo.model_config = ConfigDict(extra="ignore")

        # Mock LLM response with expected markdown format
        mock_llm.return_value = "### Table Description\nA test table"

        # Sample data
        table_info = TableInfo(
            table={"name": "test_table"},  # type: ignore[call-arg]
            dataset={"name": "test_dataset"},  # type: ignore[call-arg]
            platform={"name": "test_platform"},  # type: ignore[call-arg]
        )
        column_infos = {
            "col1": ColumnMetadataInfo(name="col1", type="STRING"),  # type: ignore[call-arg]
        }

        litellm = LiteLLM(LiteLLMModel.CLAUDE_3_HAIKU, 500, 0.3)

        # Call without extra instructions
        result = generate_table_description(
            litellm, table_info, column_infos, extra_instructions=None
        )

        # Verify the result
        assert result == "### Table Description\nA test table"

        # Verify the LLM was called
        mock_llm.assert_called_once()

        # Check that no extra instructions are in the prompt
        call_args = mock_llm.call_args
        prompt_messages = call_args[1]["prompt"]

        for msg in prompt_messages:
            if hasattr(msg, "text"):
                assert "ADDITIONAL REQUIREMENTS:" not in msg.text


class TestQueryDescriptionWithInstructions:
    """Test query description generation with extra instructions."""

    @patch(
        "datahub_integrations.gen_ai.suggest_query_description.get_extra_documentation_instructions"
    )
    @patch("datahub_integrations.gen_ai.litellm.LiteLLM.call_lite_llm")
    def test_generate_query_desc_with_instructions(
        self, mock_llm: MagicMock, mock_get_instructions: MagicMock
    ) -> None:
        """Test that extra instructions are included in query description prompt."""
        from datahub_integrations.gen_ai.suggest_query_description import QueryContext

        # Mock extra instructions
        mock_get_instructions.return_value = "Focus on business impact"

        # Mock LLM response
        mock_llm.return_value = "This query analyzes sales data."

        # Mock graph
        mock_graph = MagicMock(spec=DataHubGraph)

        # Sample query context
        query_context = QueryContext(
            query_urn="urn:li:query:test",
            query_text="SELECT * FROM sales WHERE amount > 100",
        )

        # Call the function with graph
        result = generate_query_desc(mock_graph, query_context)

        # Verify extra instructions were fetched
        mock_get_instructions.assert_called_once_with(mock_graph)

        # Verify the LLM was called
        mock_llm.assert_called_once()

        # Check that the prompt includes extra instructions
        call_args = mock_llm.call_args
        prompt = call_args[1]["prompt"]

        assert "CUSTOMER-SPECIFIC REQUIREMENTS:" in prompt
        assert "Focus on business impact" in prompt
        assert result == "This query analyzes sales data."

    @patch(
        "datahub_integrations.gen_ai.suggest_query_description.get_extra_documentation_instructions"
    )
    @patch("datahub_integrations.gen_ai.litellm.LiteLLM.call_lite_llm")
    def test_generate_query_desc_without_instructions(
        self, mock_llm: MagicMock, mock_get_instructions: MagicMock
    ) -> None:
        """Test that query description works without extra instructions."""
        from datahub_integrations.gen_ai.suggest_query_description import QueryContext

        # Mock no extra instructions
        mock_get_instructions.return_value = None

        # Mock LLM response
        mock_llm.return_value = "This query analyzes sales data."

        # Mock graph
        mock_graph = MagicMock(spec=DataHubGraph)

        # Sample query context
        query_context = QueryContext(
            query_urn="urn:li:query:test",
            query_text="SELECT * FROM sales WHERE amount > 100",
        )

        # Call the function with graph
        result = generate_query_desc(mock_graph, query_context)

        # Verify extra instructions were fetched
        mock_get_instructions.assert_called_once_with(mock_graph)

        # Verify the LLM was called
        mock_llm.assert_called_once()

        # Check that no extra instructions are in the prompt
        call_args = mock_llm.call_args
        prompt = call_args[1]["prompt"]

        assert "CUSTOMER-SPECIFIC REQUIREMENTS:" not in prompt
        assert result == "This query analyzes sales data."
