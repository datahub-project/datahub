"""Unit tests for the troubleshoot tool."""

from unittest.mock import AsyncMock, Mock, patch

from datahub_integrations.chat.agents.tools.troubleshoot import (
    RunLLMTroubleshootingProvider,
    TroubleshootingResponse,
    _create_troubleshooting_provider,
    is_troubleshoot_available,
    troubleshoot,
)


class TestCreateTroubleshootingProvider:
    """Test the troubleshooting provider factory function."""

    def test_returns_runllm_provider_when_credentials_present(self) -> None:
        """Provider is created when both API key and assistant ID are present."""
        with patch.dict(
            "os.environ",
            {
                "RUNLLM_API_KEY": "test-api-key",
                "RUNLLM_ASSISTANT_ID": "test-assistant-id",
            },
        ):
            provider = _create_troubleshooting_provider()

            assert provider is not None
            assert isinstance(provider, RunLLMTroubleshootingProvider)
            assert provider.api_key == "test-api-key"
            assert provider.assistant_id == "test-assistant-id"

    def test_returns_none_when_api_key_missing(self) -> None:
        """Provider is not created when API key is missing."""
        with patch.dict(
            "os.environ",
            {"RUNLLM_ASSISTANT_ID": "test-assistant-id"},
            clear=True,
        ):
            provider = _create_troubleshooting_provider()

            assert provider is None

    def test_returns_none_when_assistant_id_missing(self) -> None:
        """Provider is not created when assistant ID is missing."""
        with patch.dict(
            "os.environ",
            {"RUNLLM_API_KEY": "test-api-key"},
            clear=True,
        ):
            provider = _create_troubleshooting_provider()

            assert provider is None

    def test_returns_none_when_both_credentials_missing(self) -> None:
        """Provider is not created when both credentials are missing."""
        with patch.dict("os.environ", {}, clear=True):
            provider = _create_troubleshooting_provider()

            assert provider is None


class TestIsTroubleshootAvailable:
    """Test the availability check function."""

    def test_returns_true_when_provider_configured(self) -> None:
        """Availability check returns True when provider is configured."""
        with patch.dict(
            "os.environ",
            {
                "RUNLLM_API_KEY": "test-api-key",
                "RUNLLM_ASSISTANT_ID": "test-assistant-id",
            },
        ):
            assert is_troubleshoot_available() is True

    def test_returns_false_when_provider_not_configured(self) -> None:
        """Availability check returns False when provider is not configured."""
        with patch.dict("os.environ", {}, clear=True):
            assert is_troubleshoot_available() is False


class TestTroubleshoot:
    """Test the troubleshoot function."""

    def test_returns_error_when_no_provider_available(self) -> None:
        """Function returns friendly error when no provider is configured."""
        with patch.dict("os.environ", {}, clear=True):
            result = troubleshoot("How do I configure BigQuery?")

            assert "not currently available" in result["answer"]
            assert result["sources"] == []

    def test_calls_provider_and_returns_result(self) -> None:
        """Function calls provider and returns result when provider is available."""
        mock_provider = Mock()
        mock_query = AsyncMock(
            return_value=TroubleshootingResponse(
                answer="To configure BigQuery, you need...",
                sources=["https://docs.datahub.io/bigquery"],
            )
        )
        mock_provider.query = mock_query

        with patch(
            "datahub_integrations.chat.agents.tools.troubleshoot._create_troubleshooting_provider",
            return_value=mock_provider,
        ):
            result = troubleshoot("How do I configure BigQuery?")

            assert result["answer"] == "To configure BigQuery, you need..."
            assert result["sources"] == ["https://docs.datahub.io/bigquery"]
            mock_query.assert_called_once_with("How do I configure BigQuery?", None)

    def test_passes_context_to_provider(self) -> None:
        """Function passes context parameter to provider."""
        mock_provider = Mock()
        mock_query = AsyncMock(
            return_value=TroubleshootingResponse(answer="Test answer", sources=[])
        )
        mock_provider.query = mock_query

        with patch(
            "datahub_integrations.chat.agents.tools.troubleshoot._create_troubleshooting_provider",
            return_value=mock_provider,
        ):
            troubleshoot("What is the error?", context="Docker quickstart")

            mock_query.assert_called_once_with(
                "What is the error?", "Docker quickstart"
            )


class TestRunLLMTroubleshootingProvider:
    """Test the RunLLM provider implementation."""

    def test_initialization(self) -> None:
        """Provider stores configuration correctly."""
        provider = RunLLMTroubleshootingProvider(
            api_key="test-key",
            assistant_id="test-id",
            base_url="https://test.example.com",
        )

        assert provider.api_key == "test-key"
        assert provider.assistant_id == "test-id"
        assert provider.base_url == "https://test.example.com"

    def test_initialization_with_default_base_url(self) -> None:
        """Provider uses default base URL when not specified."""
        provider = RunLLMTroubleshootingProvider(
            api_key="test-key",
            assistant_id="test-id",
        )

        assert provider.base_url == "https://api.runllm.com"
