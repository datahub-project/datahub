"""Tests for the _record_langchain_usage helper method in LLMWrapper base class."""

from typing import cast
from unittest.mock import MagicMock, patch

from langchain_core.messages import AIMessage
from langchain_core.messages.ai import UsageMetadata

from datahub_integrations.observability.metrics_constants import AIModule


class TestRecordLangchainUsage:
    """Tests for LLMWrapper._record_langchain_usage helper method."""

    @patch("datahub_integrations.gen_ai.llm.base.get_cost_tracker")
    def test_records_usage_from_langchain_response(
        self, mock_get_cost_tracker: MagicMock
    ) -> None:
        """Test that _record_langchain_usage extracts and records token usage."""
        from datahub_integrations.gen_ai.llm.base import LLMWrapper

        mock_tracker = MagicMock()
        mock_get_cost_tracker.return_value = mock_tracker

        # Create a concrete wrapper for testing
        class TestWrapper(LLMWrapper):
            provider_name = "test_provider"

            def _initialize_client(self):
                return MagicMock()

            @property
            def exceptions(self):
                return MagicMock()

            def converse(self, **kwargs):
                pass

        wrapper = TestWrapper(model_name="test-model")

        # Create mock LangChain response with usage metadata
        response = AIMessage(content="Test response")
        response.usage_metadata = cast(
            UsageMetadata,
            {
                "input_tokens": 100,
                "output_tokens": 50,
                "total_tokens": 150,
            },
        )

        wrapper._record_langchain_usage(response, AIModule.CHAT)

        # Verify record_llm_call was called with correct parameters
        mock_tracker.record_llm_call.assert_called_once()
        call_kwargs = mock_tracker.record_llm_call.call_args.kwargs

        assert call_kwargs["provider"] == "test_provider"
        assert call_kwargs["model"] == "test-model"
        assert call_kwargs["ai_module"] == AIModule.CHAT
        assert call_kwargs["success"] is True

        # Check TokenUsage values
        usage = call_kwargs["usage"]
        assert usage.prompt_tokens == 100
        assert usage.completion_tokens == 50
        assert usage.total_tokens == 150

    @patch("datahub_integrations.gen_ai.llm.base.get_cost_tracker")
    def test_extracts_cache_tokens_from_input_details(
        self, mock_get_cost_tracker: MagicMock
    ) -> None:
        """Test that cache tokens are extracted from input_token_details."""
        from datahub_integrations.gen_ai.llm.base import LLMWrapper

        mock_tracker = MagicMock()
        mock_get_cost_tracker.return_value = mock_tracker

        class TestWrapper(LLMWrapper):
            provider_name = "test_provider"

            def _initialize_client(self):
                return MagicMock()

            @property
            def exceptions(self):
                return MagicMock()

            def converse(self, **kwargs):
                pass

        wrapper = TestWrapper(model_name="test-model")

        # Create response with cache token details (OpenAI format)
        response = AIMessage(content="Test response")
        response.usage_metadata = cast(
            UsageMetadata,
            {
                "input_tokens": 100,
                "output_tokens": 50,
                "total_tokens": 150,
                "input_token_details": {
                    "cache_read": 80,
                    "cache_creation": 20,
                },
            },
        )

        wrapper._record_langchain_usage(response, AIModule.CHAT)

        call_kwargs = mock_tracker.record_llm_call.call_args.kwargs
        usage = call_kwargs["usage"]

        assert usage.cache_read_tokens == 80
        assert usage.cache_write_tokens == 20

    @patch("datahub_integrations.gen_ai.llm.base.get_cost_tracker")
    def test_handles_missing_usage_metadata_gracefully(
        self, mock_get_cost_tracker: MagicMock
    ) -> None:
        """Test that missing usage_metadata is handled gracefully (no call)."""
        from datahub_integrations.gen_ai.llm.base import LLMWrapper

        mock_tracker = MagicMock()
        mock_get_cost_tracker.return_value = mock_tracker

        class TestWrapper(LLMWrapper):
            provider_name = "test_provider"

            def _initialize_client(self):
                return MagicMock()

            @property
            def exceptions(self):
                return MagicMock()

            def converse(self, **kwargs):
                pass

        wrapper = TestWrapper(model_name="test-model")

        # Create response WITHOUT usage_metadata
        response = AIMessage(content="Test response")
        # Ensure no usage_metadata attribute
        if hasattr(response, "usage_metadata"):
            delattr(response, "usage_metadata")

        wrapper._record_langchain_usage(response, AIModule.CHAT)

        # Should NOT call record_llm_call when usage_metadata is missing
        mock_tracker.record_llm_call.assert_not_called()

    @patch("datahub_integrations.gen_ai.llm.base.get_cost_tracker")
    def test_handles_none_usage_metadata(
        self, mock_get_cost_tracker: MagicMock
    ) -> None:
        """Test that None usage_metadata is handled gracefully."""
        from datahub_integrations.gen_ai.llm.base import LLMWrapper

        mock_tracker = MagicMock()
        mock_get_cost_tracker.return_value = mock_tracker

        class TestWrapper(LLMWrapper):
            provider_name = "test_provider"

            def _initialize_client(self):
                return MagicMock()

            @property
            def exceptions(self):
                return MagicMock()

            def converse(self, **kwargs):
                pass

        wrapper = TestWrapper(model_name="test-model")

        # Create response with None usage_metadata
        response = AIMessage(content="Test response")
        response.usage_metadata = None

        wrapper._record_langchain_usage(response, AIModule.CHAT)

        # Should NOT call record_llm_call
        mock_tracker.record_llm_call.assert_not_called()

    @patch("datahub_integrations.gen_ai.llm.base.get_cost_tracker")
    def test_handles_missing_input_token_details(
        self, mock_get_cost_tracker: MagicMock
    ) -> None:
        """Test that missing input_token_details defaults cache tokens to 0."""
        from datahub_integrations.gen_ai.llm.base import LLMWrapper

        mock_tracker = MagicMock()
        mock_get_cost_tracker.return_value = mock_tracker

        class TestWrapper(LLMWrapper):
            provider_name = "test_provider"

            def _initialize_client(self):
                return MagicMock()

            @property
            def exceptions(self):
                return MagicMock()

            def converse(self, **kwargs):
                pass

        wrapper = TestWrapper(model_name="test-model")

        # Create response WITHOUT input_token_details
        response = AIMessage(content="Test response")
        response.usage_metadata = cast(
            UsageMetadata,
            {
                "input_tokens": 100,
                "output_tokens": 50,
                "total_tokens": 150,
                # No input_token_details
            },
        )

        wrapper._record_langchain_usage(response, AIModule.CHAT)

        call_kwargs = mock_tracker.record_llm_call.call_args.kwargs
        usage = call_kwargs["usage"]

        # Cache tokens should default to 0
        assert usage.cache_read_tokens == 0
        assert usage.cache_write_tokens == 0

    @patch("datahub_integrations.gen_ai.llm.base.get_cost_tracker")
    def test_calculates_total_tokens_when_missing(
        self, mock_get_cost_tracker: MagicMock
    ) -> None:
        """Test that total_tokens is calculated if not provided."""
        from datahub_integrations.gen_ai.llm.base import LLMWrapper

        mock_tracker = MagicMock()
        mock_get_cost_tracker.return_value = mock_tracker

        class TestWrapper(LLMWrapper):
            provider_name = "test_provider"

            def _initialize_client(self):
                return MagicMock()

            @property
            def exceptions(self):
                return MagicMock()

            def converse(self, **kwargs):
                pass

        wrapper = TestWrapper(model_name="test-model")

        # Create response WITHOUT total_tokens
        response = AIMessage(content="Test response")
        response.usage_metadata = cast(
            UsageMetadata,
            {
                "input_tokens": 100,
                "output_tokens": 50,
                # No total_tokens
            },
        )

        wrapper._record_langchain_usage(response, AIModule.CHAT)

        call_kwargs = mock_tracker.record_llm_call.call_args.kwargs
        usage = call_kwargs["usage"]

        # total_tokens should be calculated
        assert usage.total_tokens == 150  # 100 + 50

    @patch("datahub_integrations.gen_ai.llm.base.get_cost_tracker")
    def test_passes_correct_ai_module(self, mock_get_cost_tracker: MagicMock) -> None:
        """Test that different AIModule values are passed correctly."""
        from datahub_integrations.gen_ai.llm.base import LLMWrapper

        mock_tracker = MagicMock()
        mock_get_cost_tracker.return_value = mock_tracker

        class TestWrapper(LLMWrapper):
            provider_name = "test_provider"

            def _initialize_client(self):
                return MagicMock()

            @property
            def exceptions(self):
                return MagicMock()

            def converse(self, **kwargs):
                pass

        wrapper = TestWrapper(model_name="test-model")

        response = AIMessage(content="Test response")
        response.usage_metadata = cast(
            UsageMetadata,
            {
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15,
            },
        )

        # Test with different AIModule values
        for ai_module in [
            AIModule.CHAT,
            AIModule.DESCRIPTION_GENERATION,
            AIModule.TERMS_SUGGESTION,
            AIModule.QUERY_DESCRIPTION,
        ]:
            mock_tracker.reset_mock()
            wrapper._record_langchain_usage(response, ai_module)

            call_kwargs = mock_tracker.record_llm_call.call_args.kwargs
            assert call_kwargs["ai_module"] == ai_module
