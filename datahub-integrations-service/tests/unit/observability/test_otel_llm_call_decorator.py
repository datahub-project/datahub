"""Tests for the @otel_llm_call decorator."""

from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.observability.decorators import otel_llm_call
from datahub_integrations.observability.metrics_constants import AIModule


class TestOtelLlmCallDecorator:
    """Tests for @otel_llm_call decorator parameter validation and ai_module extraction."""

    def test_decorator_validates_parameter_exists_at_decoration_time(self) -> None:
        """Test that decorator raises TypeError if specified parameter doesn't exist."""
        with pytest.raises(TypeError) as exc_info:

            class BadWrapper:
                provider_name = "test"
                model_name = "test"

                @otel_llm_call(ai_module_param="nonexistent_param")
                def converse(
                    self, *, system: List[Any], messages: List[Any], ai_module: AIModule
                ) -> str:
                    return "response"

        assert "has no parameter 'nonexistent_param'" in str(exc_info.value)
        assert "converse" in str(exc_info.value)
        assert "Available parameters:" in str(exc_info.value)

    def test_decorator_accepts_valid_parameter_name(self) -> None:
        """Test that decorator accepts valid parameter name without error."""

        # Should not raise
        class GoodWrapper:
            provider_name = "test"
            model_name = "test"

            @otel_llm_call(ai_module_param="ai_module")
            def converse(
                self, *, system: List[Any], messages: List[Any], ai_module: AIModule
            ) -> str:
                return "response"

        # Method should be callable
        assert callable(GoodWrapper.converse)

    def test_decorator_uses_default_parameter_name(self) -> None:
        """Test that decorator defaults to 'ai_module' parameter name."""

        # Should not raise - uses default ai_module_param="ai_module"
        class GoodWrapper:
            provider_name = "test"
            model_name = "test"

            @otel_llm_call()
            def converse(
                self, *, system: List[Any], messages: List[Any], ai_module: AIModule
            ) -> str:
                return "response"

        assert callable(GoodWrapper.converse)

    def test_decorator_fails_with_default_when_ai_module_missing(self) -> None:
        """Test that decorator fails if function lacks 'ai_module' parameter."""
        with pytest.raises(TypeError) as exc_info:

            class BadWrapper:
                provider_name = "test"
                model_name = "test"

                @otel_llm_call()
                def converse(self, *, system: List[Any], messages: List[Any]) -> str:
                    return "response"

        assert "has no parameter 'ai_module'" in str(exc_info.value)

    @patch("datahub_integrations.observability.decorators._lazy_import_cost_tracker")
    @patch("datahub_integrations.observability.decorators._lazy_import_ai_module")
    def test_decorator_extracts_ai_module_from_kwargs(
        self, mock_ai_module_import: MagicMock, mock_tracker_import: MagicMock
    ) -> None:
        """Test that decorator extracts ai_module from function kwargs."""
        mock_tracker = MagicMock()
        mock_tracker_import.return_value = mock_tracker
        mock_ai_module_import.return_value = AIModule

        class MockWrapper:
            provider_name = "test_provider"
            model_name = "test_model"

            @otel_llm_call(ai_module_param="ai_module")
            def converse(
                self, *, system: list, messages: list, ai_module: AIModule
            ) -> str:
                return "response"

        wrapper = MockWrapper()
        result = wrapper.converse(
            system=[], messages=[], ai_module=AIModule.DESCRIPTION_GENERATION
        )

        assert result == "response"

        # Verify record_llm_call_latency was called with correct ai_module
        mock_tracker.record_llm_call_latency.assert_called_once()
        call_kwargs = mock_tracker.record_llm_call_latency.call_args.kwargs
        assert call_kwargs["ai_module"] == AIModule.DESCRIPTION_GENERATION
        assert call_kwargs["provider"] == "test_provider"
        assert call_kwargs["model"] == "test_model"
        assert call_kwargs["success"] is True

    @patch("datahub_integrations.observability.decorators._lazy_import_cost_tracker")
    @patch("datahub_integrations.observability.decorators._lazy_import_ai_module")
    def test_decorator_defaults_to_chat_when_ai_module_not_provided(
        self, mock_ai_module_import: MagicMock, mock_tracker_import: MagicMock
    ) -> None:
        """Test that decorator defaults to CHAT when ai_module is None."""
        mock_tracker = MagicMock()
        mock_tracker_import.return_value = mock_tracker
        mock_ai_module_import.return_value = AIModule

        class MockWrapper:
            provider_name = "test_provider"
            model_name = "test_model"

            @otel_llm_call(ai_module_param="ai_module")
            def converse(
                self,
                *,
                system: list,
                messages: list,
                ai_module: AIModule | None = None,
            ) -> str:
                return "response"

        wrapper = MockWrapper()
        wrapper.converse(system=[], messages=[])

        # Should default to CHAT
        call_kwargs = mock_tracker.record_llm_call_latency.call_args.kwargs
        assert call_kwargs["ai_module"] == AIModule.CHAT

    @patch("datahub_integrations.observability.decorators._lazy_import_cost_tracker")
    @patch("datahub_integrations.observability.decorators._lazy_import_ai_module")
    def test_decorator_records_failure_on_exception(
        self, mock_ai_module_import: MagicMock, mock_tracker_import: MagicMock
    ) -> None:
        """Test that decorator records success=False when function raises exception."""
        mock_tracker = MagicMock()
        mock_tracker_import.return_value = mock_tracker
        mock_ai_module_import.return_value = AIModule

        class MockWrapper:
            provider_name = "test_provider"
            model_name = "test_model"

            @otel_llm_call(ai_module_param="ai_module")
            def converse(
                self, *, system: list, messages: list, ai_module: AIModule
            ) -> str:
                raise ValueError("Test error")

        wrapper = MockWrapper()

        with pytest.raises(ValueError):
            wrapper.converse(system=[], messages=[], ai_module=AIModule.CHAT)

        # Verify failure was recorded
        call_kwargs = mock_tracker.record_llm_call_latency.call_args.kwargs
        assert call_kwargs["success"] is False

    @patch("datahub_integrations.observability.decorators._lazy_import_cost_tracker")
    @patch("datahub_integrations.observability.decorators._lazy_import_ai_module")
    def test_decorator_handles_string_ai_module_value(
        self, mock_ai_module_import: MagicMock, mock_tracker_import: MagicMock
    ) -> None:
        """Test that decorator can convert string ai_module to enum."""
        mock_tracker = MagicMock()
        mock_tracker_import.return_value = mock_tracker
        mock_ai_module_import.return_value = AIModule

        class MockWrapper:
            provider_name = "test_provider"
            model_name = "test_model"

            @otel_llm_call(ai_module_param="ai_module")
            def converse(self, *, system: list, messages: list, ai_module: str) -> str:
                return "response"

        wrapper = MockWrapper()
        wrapper.converse(system=[], messages=[], ai_module="chat")

        # String should be converted to enum
        call_kwargs = mock_tracker.record_llm_call_latency.call_args.kwargs
        assert call_kwargs["ai_module"] == AIModule.CHAT

    @patch("datahub_integrations.observability.decorators._lazy_import_cost_tracker")
    @patch("datahub_integrations.observability.decorators._lazy_import_ai_module")
    def test_decorator_uses_custom_parameter_name(
        self, mock_ai_module_import: MagicMock, mock_tracker_import: MagicMock
    ) -> None:
        """Test that decorator can use custom parameter name for ai_module."""
        mock_tracker = MagicMock()
        mock_tracker_import.return_value = mock_tracker
        mock_ai_module_import.return_value = AIModule

        class MockWrapper:
            provider_name = "test_provider"
            model_name = "test_model"

            @otel_llm_call(ai_module_param="module_type")
            def converse(
                self, *, system: list, messages: list, module_type: AIModule
            ) -> str:
                return "response"

        wrapper = MockWrapper()
        wrapper.converse(system=[], messages=[], module_type=AIModule.TERMS_SUGGESTION)

        call_kwargs = mock_tracker.record_llm_call_latency.call_args.kwargs
        assert call_kwargs["ai_module"] == AIModule.TERMS_SUGGESTION

    def test_decorator_lists_available_parameters_in_error(self) -> None:
        """Test that error message includes list of available parameters."""
        with pytest.raises(TypeError) as exc_info:

            @otel_llm_call(ai_module_param="wrong")
            def my_function(
                self, *, system: list, messages: list, ai_module: AIModule
            ) -> str:
                return "response"

        error_msg = str(exc_info.value)
        # Should list available parameters
        assert "self" in error_msg
        assert "system" in error_msg
        assert "messages" in error_msg
        assert "ai_module" in error_msg
