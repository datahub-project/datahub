"""Tests for the _get_extra_llm_instructions function."""

from typing import Any, Dict, Generator
from unittest.mock import MagicMock

import pytest
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.chat_session import _get_extra_llm_instructions


@pytest.fixture(autouse=True)
def clear_cache() -> Generator[None, None, None]:
    """Clear the TTL cache before each test."""
    # Access the cache from the decorator
    cache_func = _get_extra_llm_instructions
    if hasattr(cache_func, "__wrapped__"):
        # Clear the cache if it exists
        cache_func.cache_clear() if hasattr(cache_func, "cache_clear") else None
    # Since we're using cachetools.cached decorator, we need to clear its cache
    # The cache is stored as an attribute on the decorated function
    if hasattr(_get_extra_llm_instructions, "__cache__"):
        _get_extra_llm_instructions.__cache__.clear()
    yield
    # Clear again after test
    if hasattr(_get_extra_llm_instructions, "__cache__"):
        _get_extra_llm_instructions.__cache__.clear()


def create_mock_client(graphql_response: Dict[str, Any]) -> MagicMock:
    """Helper to create a mock DataHubClient with a configured graph."""
    mock_client = MagicMock(spec=DataHubClient)
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = graphql_response
    mock_client._graph = mock_graph
    return mock_client


class TestGetExtraLLMInstructions:
    """Test cases for _get_extra_llm_instructions function."""

    def test_successful_retrieval_single_instruction(self) -> None:
        """Test successful retrieval of a single GENERAL_CONTEXT instruction."""
        # Setup mock response
        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Be helpful and concise.",
                            "lastModified": {
                                "time": 1234567890,
                                "actor": "urn:li:corpuser:admin",
                            },
                        }
                    ]
                }
            }
        }

        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result == "Be helpful and concise."
        mock_client._graph.execute_graphql.assert_called_once()

    def test_multiple_instructions_takes_last(self) -> None:
        """Test that the last instruction in the array is returned."""
        # Setup mock response with multiple instructions
        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "First instruction",
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                        {
                            "id": "inst-2",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Second instruction",
                            "lastModified": {
                                "time": 2000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                        {
                            "id": "inst-3",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Third instruction - should be used",
                            "lastModified": {
                                "time": 3000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                    ]
                }
            }
        }

        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify - should return the last item
        assert result == "Third instruction - should be used"

    def test_filters_inactive_instructions(self) -> None:
        """Test that only ACTIVE instructions are considered."""
        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "INACTIVE",
                            "instruction": "Inactive instruction - should be ignored",
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                        {
                            "id": "inst-2",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Active instruction - should be used",
                            "lastModified": {
                                "time": 2000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                    ]
                }
            }
        }

        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify - only active instruction should be returned
        assert result == "Active instruction - should be used"

    def test_filters_non_general_context_types(self) -> None:
        """Test that only GENERAL_CONTEXT type instructions are considered."""
        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "SPECIFIC_CONTEXT",  # Different type
                            "state": "ACTIVE",
                            "instruction": "Specific context - should be ignored",
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                        {
                            "id": "inst-2",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "General context - should be used",
                            "lastModified": {
                                "time": 2000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                    ]
                }
            }
        }

        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result == "General context - should be used"

    def test_no_global_settings(self) -> None:
        """Test when globalSettings is not present in response."""
        mock_response: Dict[str, Any] = {}
        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result is None

    def test_no_ai_assistant_settings(self) -> None:
        """Test when aiAssistant is not present in globalSettings."""
        mock_response: Dict[str, Any] = {"globalSettings": {}}
        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result is None

    def test_empty_instructions_array(self) -> None:
        """Test when instructions array is empty."""
        mock_response: Dict[str, Any] = {
            "globalSettings": {"aiAssistant": {"instructions": []}}
        }
        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result is None

    def test_no_valid_instructions_after_filtering(self) -> None:
        """Test when no instructions match the filter criteria."""
        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "INACTIVE",  # Not active
                            "instruction": "Inactive",
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                        {
                            "id": "inst-2",
                            "type": "OTHER_TYPE",  # Wrong type
                            "state": "ACTIVE",
                            "instruction": "Wrong type",
                            "lastModified": {
                                "time": 2000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        },
                    ]
                }
            }
        }
        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result is None

    def test_whitespace_trimming(self) -> None:
        """Test that instruction text is properly trimmed."""
        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "  \n  Instruction with whitespace  \n\t ",
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        }
                    ]
                }
            }
        }
        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result == "Instruction with whitespace"

    def test_empty_instruction_text_returns_none(self) -> None:
        """Test that empty instruction text returns None."""
        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "   ",  # Only whitespace
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        }
                    ]
                }
            }
        }
        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result is None

    def test_graphql_exception_returns_none(self) -> None:
        """Test that GraphQL exceptions return None with warning (temporary behavior)."""
        # Changed behavior: We now catch all exceptions and return None instead of
        # raising, to handle cases where GMS instances don't have aiAssistant field yet.
        # This is a temporary solution until all instances are upgraded.
        mock_client = MagicMock(spec=DataHubClient)
        mock_graph = MagicMock()
        mock_graph.execute_graphql.side_effect = Exception("GraphQL connection error")
        mock_client._graph = mock_graph

        # Execute - should return None instead of raising
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result is None

    def test_undefined_aiassistant_field_returns_none(self) -> None:
        """Test that validation error for undefined aiAssistant field returns None with warning."""
        # This simulates the error when a GMS instance doesn't have the aiAssistant field yet
        mock_client = MagicMock(spec=DataHubClient)
        mock_graph = MagicMock()
        error_message = (
            "Error executing graphql query: [{'message': "
            '"Validation error (FieldUndefined@[globalSettings/aiAssistant]) : '
            "Field 'aiAssistant' in type 'GlobalSettings' is undefined\", "
            "'locations': [{'line': 4, 'column': 13}], "
            "'extensions': {'classification': 'ValidationError'}}]"
        )
        mock_graph.execute_graphql.side_effect = Exception(error_message)
        mock_client._graph = mock_graph

        # Execute - should return None instead of raising
        result = _get_extra_llm_instructions(mock_client)

        # Verify
        assert result is None

    def test_caching_behavior(self) -> None:
        """Test that results are cached and reused."""
        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Cached instruction",
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        }
                    ]
                }
            }
        }
        mock_client = create_mock_client(mock_response)

        # First call
        result1 = _get_extra_llm_instructions(mock_client)
        assert result1 == "Cached instruction"

        # Second call - should use cache
        result2 = _get_extra_llm_instructions(mock_client)
        assert result2 == "Cached instruction"

        # Verify GraphQL was only called once due to caching
        assert mock_client._graph.execute_graphql.call_count == 1

    def test_cache_key_includes_client(self) -> None:
        """Test that different clients have separate cache entries."""
        response1 = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Client 1 instruction",
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        }
                    ]
                }
            }
        }

        response2 = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-2",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": "Client 2 instruction",
                            "lastModified": {
                                "time": 2000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        }
                    ]
                }
            }
        }

        mock_client1 = create_mock_client(response1)
        mock_client2 = create_mock_client(response2)

        # Call with different clients
        result1 = _get_extra_llm_instructions(mock_client1)
        result2 = _get_extra_llm_instructions(mock_client2)

        # Verify different results for different clients
        assert result1 == "Client 1 instruction"
        assert result2 == "Client 2 instruction"

        # Both clients should have been called
        assert mock_client1._graph.execute_graphql.call_count == 1
        assert mock_client2._graph.execute_graphql.call_count == 1

    def test_complex_multiline_instruction(self) -> None:
        """Test handling of complex multi-line instructions."""
        instruction_text = """You are an expert data analyst.

When answering questions:
- Be precise and technical
- Reference specific datasets
- Include confidence levels

Always maintain a professional tone."""

        mock_response = {
            "globalSettings": {
                "aiAssistant": {
                    "instructions": [
                        {
                            "id": "inst-1",
                            "type": "GENERAL_CONTEXT",
                            "state": "ACTIVE",
                            "instruction": instruction_text,
                            "lastModified": {
                                "time": 1000,
                                "actor": "urn:li:corpuser:admin",
                            },
                        }
                    ]
                }
            }
        }
        mock_client = create_mock_client(mock_response)

        # Execute
        result = _get_extra_llm_instructions(mock_client)

        # Verify - should preserve the multi-line format
        assert result == instruction_text.strip()
