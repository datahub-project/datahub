"""Tests for ingestion tools."""

from typing import cast
from unittest.mock import Mock, patch

from datahub_integrations.chat.agents.tools.ingestion import (
    get_full_ingestion_log_size_from_s3,
    get_full_ingestion_log_window_from_s3,
    get_ingestion_execution_logs,
    get_ingestion_execution_request,
    get_ingestion_source,
    grep_full_ingestion_logs_from_s3,
    is_s3_log_streaming_enabled,
)
from datahub_integrations.chat.agents.tools.log_formatting import (
    GrepResult,
    WindowingResult,
)
from datahub_integrations.telemetry.ingestion_events import (
    S3LogStreamingRequestEvent,
    S3LogStreamingResponseEvent,
)

SAMPLE_RECIPE_WITH_SECRETS = """
source:
  type: snowflake
  config:
    account_id: abc123
    password: my-secret-password
    warehouse: COMPUTE_WH
"""


class TestIsS3LogStreamingEnabled:
    """Tests for is_s3_log_streaming_enabled function."""

    def test_returns_true_by_default(self) -> None:
        """Test that function returns True when env var is not set."""
        with patch.dict("os.environ", {}, clear=True):
            # Clear the cache before testing
            is_s3_log_streaming_enabled.cache_clear()
            assert is_s3_log_streaming_enabled() is True

    def test_returns_true_when_explicitly_enabled(self) -> None:
        """Test that function returns True when env var is set to true."""
        with patch.dict("os.environ", {"S3_LOG_STREAMING_ENABLED": "true"}):
            # Clear the cache before testing
            is_s3_log_streaming_enabled.cache_clear()
            assert is_s3_log_streaming_enabled() is True

    def test_returns_true_when_set_to_1(self) -> None:
        """Test that function returns True when env var is set to 1."""
        with patch.dict("os.environ", {"S3_LOG_STREAMING_ENABLED": "1"}):
            # Clear the cache before testing
            is_s3_log_streaming_enabled.cache_clear()
            assert is_s3_log_streaming_enabled() is True

    def test_returns_false_when_explicitly_disabled(self) -> None:
        """Test that function returns False when env var is set to false."""
        with patch.dict("os.environ", {"S3_LOG_STREAMING_ENABLED": "false"}):
            # Clear the cache before testing
            is_s3_log_streaming_enabled.cache_clear()
            assert is_s3_log_streaming_enabled() is False

    def test_returns_false_when_set_to_0(self) -> None:
        """Test that function returns False when env var is set to 0."""
        with patch.dict("os.environ", {"S3_LOG_STREAMING_ENABLED": "0"}):
            # Clear the cache before testing
            is_s3_log_streaming_enabled.cache_clear()
            assert is_s3_log_streaming_enabled() is False


class TestGetIngestionSource:
    """Tests for get_ingestion_source function."""

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_executes_graphql_with_correct_operation(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that function executes GraphQL with correct operation name."""
        mock_execute_graphql.return_value = {"ingestionSource": {"urn": "test"}}

        get_ingestion_source("urn:li:dataHubIngestionSource:test")

        mock_execute_graphql.assert_called_once()
        assert mock_execute_graphql.call_args[1]["operation_name"] == "ingestionSource"

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_returns_cleaned_response(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that function returns cleaned GraphQL response."""
        mock_execute_graphql.return_value = {
            "ingestionSource": {"urn": "test", "name": "Test Source"}
        }

        result = get_ingestion_source("urn:li:dataHubIngestionSource:test")

        assert isinstance(result, dict)
        assert "urn" in result

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_redacts_secrets_in_recipe(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that secrets in recipe are redacted."""
        mock_execute_graphql.return_value = {
            "ingestionSource": {
                "urn": "test",
                "name": "Test Source",
                "config": {"recipe": SAMPLE_RECIPE_WITH_SECRETS, "version": "1.0"},
            }
        }

        result = get_ingestion_source("urn:li:dataHubIngestionSource:test")

        assert "my-secret-password" not in str(result)
        assert "********" in result["config"]["recipe"]
        assert "abc123" in result["config"]["recipe"]
        assert "COMPUTE_WH" in result["config"]["recipe"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_missing_recipe(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that missing recipe is handled gracefully."""
        mock_execute_graphql.return_value = {
            "ingestionSource": {"urn": "test", "name": "Test Source", "config": {}}
        }

        result = get_ingestion_source("urn:li:dataHubIngestionSource:test")

        assert "urn" in result
        assert "name" in result

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_none_recipe(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that None recipe is handled gracefully (cleaned response removes None values)."""
        mock_execute_graphql.return_value = {
            "ingestionSource": {
                "urn": "test",
                "name": "Test Source",
                "config": {"recipe": None, "version": "1.0"},
            }
        }

        result = get_ingestion_source("urn:li:dataHubIngestionSource:test")

        # clean_gql_response removes None values, so recipe field won't be present
        assert "urn" in result
        assert "name" in result


class TestGetIngestionExecutionRequest:
    """Tests for get_ingestion_execution_request function."""

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_executes_graphql_with_correct_operation(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that function executes GraphQL with correct operation name."""
        mock_execute_graphql.return_value = {"executionRequest": {"urn": "test"}}

        get_ingestion_execution_request("urn:li:executionRequest:test")

        mock_execute_graphql.assert_called_once()
        assert mock_execute_graphql.call_args[1]["operation_name"] == "executionRequest"

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_redacts_secrets_in_source_recipe(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that secrets in source recipe are redacted."""
        mock_execute_graphql.return_value = {
            "executionRequest": {
                "urn": "test",
                "source": {
                    "urn": "source-test",
                    "name": "Test Source",
                    "config": {"recipe": SAMPLE_RECIPE_WITH_SECRETS, "version": "1.0"},
                },
            }
        }

        result = get_ingestion_execution_request("urn:li:executionRequest:test")

        assert "my-secret-password" not in str(result)
        assert "********" in result["source"]["config"]["recipe"]
        assert "abc123" in result["source"]["config"]["recipe"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_missing_source(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that missing source is handled gracefully."""
        mock_execute_graphql.return_value = {"executionRequest": {"urn": "test"}}

        result = get_ingestion_execution_request("urn:li:executionRequest:test")

        assert "urn" in result


class TestGetIngestionExecutionLogs:
    """Tests for get_ingestion_execution_logs function."""

    def _create_mock_graphql_result(self, report: str) -> dict:
        """Helper to create mock GraphQL result."""
        return {"executionRequest": {"result": {"report": report}}}

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_windowing_mode_default_behavior(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test windowing mode with default parameters."""
        log_lines = "\n".join([f"Log line {i}" for i in range(1, 201)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = cast(
            WindowingResult,
            get_ingestion_execution_logs("urn:li:executionRequest:test"),
        )

        assert result["total_lines"] == 200
        assert result["lines_returned"] == 150  # Default lines_from_end
        assert result["window_start"] == 51
        assert result["window_end"] == 200

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_windowing_mode_with_offset(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test windowing mode with offset_from_end parameter."""
        log_lines = "\n".join([f"Log line {i}" for i in range(1, 301)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = cast(
            WindowingResult,
            get_ingestion_execution_logs(
                "urn:li:executionRequest:test",
                lines_from_end=100,
                offset_from_end=150,
            ),
        )

        assert result["total_lines"] == 300
        assert result["lines_returned"] == 100
        assert result["window_start"] == 51
        assert result["window_end"] == 150

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_windowing_mode_offset_beyond_logs(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test windowing mode when offset is beyond available logs."""
        log_lines = "\n".join([f"Log line {i}" for i in range(1, 101)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            offset_from_end=200,
        )

        assert result["lines_returned"] == 0
        assert "beyond the available logs" in result["message"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_grep_mode_finds_matches(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test grep mode finds and returns matches."""
        log_lines = "\n".join(
            [
                "Log line 1",
                "ERROR: Something failed",
                "Log line 3",
                "Log line 4",
                "ERROR: Another failure",
                "Log line 6",
            ]
        )
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = cast(
            GrepResult,
            get_ingestion_execution_logs(
                "urn:li:executionRequest:test",
                grep_phrase="ERROR",
                lines_after_match=1,
                lines_before_match=1,
            ),
        )

        assert result["matches_found"] == 2
        assert result["matches_returned"] == 2
        assert result["truncated"] is False
        assert "ERROR" in result["logs"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_grep_mode_no_matches(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test grep mode when no matches are found."""
        log_lines = "\n".join([f"Log line {i}" for i in range(1, 11)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = cast(
            GrepResult,
            get_ingestion_execution_logs(
                "urn:li:executionRequest:test",
                grep_phrase="ERROR",
            ),
        )

        assert result["matches_found"] == 0
        assert result["matches_returned"] == 0
        assert result["lines_returned"] == 0
        assert "No matches found" in result["message"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_grep_mode_truncates_by_max_matches(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test grep mode truncates when max_matches_returned is exceeded."""
        log_lines = "\n".join([f"ERROR line {i}" for i in range(1, 11)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = cast(
            GrepResult,
            get_ingestion_execution_logs(
                "urn:li:executionRequest:test",
                grep_phrase="ERROR",
                max_matches_returned=3,
                lines_after_match=1,
                lines_before_match=0,
            ),
        )

        # Should find all matches but only return limited number
        assert result["matches_found"] > result["matches_returned"]
        assert result["matches_returned"] == 3
        assert result["truncated"] is True
        assert "TRUNCATED" in result["message"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_grep_mode_includes_context_lines(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that grep mode includes context lines before and after match."""
        log_lines = "\n".join(
            [
                "Before 1",
                "Before 2",
                "ERROR: The error",
                "After 1",
                "After 2",
            ]
        )
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            grep_phrase="ERROR",
            lines_before_match=2,
            lines_after_match=2,
        )

        logs = result["logs"]
        assert "Before 1" in logs
        assert "Before 2" in logs
        assert "ERROR: The error" in logs
        assert "After 1" in logs
        assert "After 2" in logs

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_grep_mode_highlights_matching_lines(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that grep mode highlights matching lines with prefix."""
        log_lines = "\n".join(["Log 1", "ERROR: Match", "Log 3"])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            grep_phrase="ERROR",
            lines_before_match=1,
            lines_after_match=1,
        )

        logs = result["logs"]
        assert ">>> " in logs  # Matching line should have >>> prefix
        assert "    " in logs  # Context lines should have space prefix

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_empty_report(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test handling when report field is empty or None."""
        mock_execute_graphql.return_value = {"executionRequest": {"result": {}}}

        result = get_ingestion_execution_logs("urn:li:executionRequest:test")

        assert result["total_lines"] == 0
        assert result["lines_returned"] == 0
        assert "No logs available" in result["message"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_grep_mode_multiple_matches_separated(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that multiple grep matches are separated properly."""
        log_lines = "\n".join(
            ["Log 1", "ERROR: First", "Log 3", "Log 4", "ERROR: Second", "Log 6"]
        )
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            grep_phrase="ERROR",
            lines_before_match=0,
            lines_after_match=0,
        )

        assert "--- Next Match ---" in result["logs"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_windowing_mode_includes_line_numbers(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that windowing mode includes line numbers in output."""
        log_lines = "\n".join([f"Log line {i}" for i in range(1, 11)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test", lines_from_end=5
        )

        logs = result["logs"]
        assert "Line 6:" in logs
        assert "Line 10:" in logs

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_executes_graphql_with_executionRequestResult_operation(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that function uses executionRequestResult operation."""
        log_lines = "Test log"
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        get_ingestion_execution_logs("urn:li:executionRequest:test")

        mock_execute_graphql.assert_called_once()
        assert (
            mock_execute_graphql.call_args[1]["operation_name"]
            == "executionRequestResult"
        )

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_default_parameters_are_conservative(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that default parameters encourage conservative usage."""
        log_lines = "\n".join([f"ERROR line {i}" for i in range(1, 101)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        # Call with defaults
        result = cast(
            GrepResult,
            get_ingestion_execution_logs(
                "urn:li:executionRequest:test", grep_phrase="ERROR"
            ),
        )

        # Defaults should limit output
        assert result["matches_returned"] <= 3  # max_matches_returned default

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_return_value_structure_windowing_mode(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that windowing mode returns expected dictionary structure."""
        log_lines = "\n".join([f"Log {i}" for i in range(1, 11)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = get_ingestion_execution_logs("urn:li:executionRequest:test")

        # Check all expected keys are present
        assert "logs" in result
        assert "total_lines" in result
        assert "lines_returned" in result
        assert "window_start" in result
        assert "window_end" in result
        assert "message" in result

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_return_value_structure_grep_mode(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test that grep mode returns expected dictionary structure."""
        log_lines = "\n".join(["Log 1", "ERROR: Match", "Log 3"])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test", grep_phrase="ERROR"
        )

        # Check all expected keys are present
        assert "logs" in result
        assert "total_lines" in result
        assert "lines_returned" in result
        assert "matches_found" in result
        assert "matches_returned" in result
        assert "truncated" in result
        assert "grep_phrase" in result
        assert "message" in result


class TestGetFullIngestionLogSizeFromS3:
    """Tests for get_full_ingestion_log_size_from_s3 function."""

    @patch("datahub_integrations.chat.agents.tools.ingestion.requests.head")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_returns_file_size_successfully(
        self,
        mock_execute_graphql: Mock,
        mock_get_client: Mock,
        mock_requests_head: Mock,
    ) -> None:
        """Test that function returns file size when S3 logs are available."""
        mock_execute_graphql.return_value = {
            "getExecutionRequestDownloadUrl": {
                "downloadUrl": "https://s3.amazonaws.com/bucket/key?signature=xyz",
                "expiresIn": 3600,
            }
        }

        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {"Content-Length": "5242880"}  # 5 MB
        mock_requests_head.return_value = mock_head_response

        result = get_full_ingestion_log_size_from_s3(
            "urn:li:dataHubExecutionRequest:test"
        )

        assert result["available"] is True
        assert result["file_size_bytes"] == 5242880
        assert result["file_size_mb"] == 5.0

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_missing_download_url(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test handling when S3 logs are not available."""
        mock_execute_graphql.return_value = {"getExecutionRequestDownloadUrl": None}

        result = get_full_ingestion_log_size_from_s3(
            "urn:li:dataHubExecutionRequest:test"
        )

        assert "error" in result
        assert result["available"] is False
        assert "S3 logs not available" in result["error"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_graphql_error(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test handling when GraphQL call fails."""
        mock_execute_graphql.side_effect = Exception("GraphQL error")

        result = get_full_ingestion_log_size_from_s3(
            "urn:li:dataHubExecutionRequest:test"
        )

        assert "error" in result
        assert result["available"] is False
        assert "Failed to get S3 download URL" in result["error"]


class TestGrepFullIngestionLogsFromS3:
    """Tests for grep_full_ingestion_logs_from_s3 function."""

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.requests.head")
    @patch("datahub_integrations.chat.agents.tools.ingestion.stream_grep_mode")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_searches_logs_for_phrase(
        self,
        mock_execute_graphql: Mock,
        mock_get_client: Mock,
        mock_stream_grep: Mock,
        mock_requests_head: Mock,
        mock_track_event: Mock,
    ) -> None:
        """Test that function searches logs for specified phrase."""
        mock_execute_graphql.return_value = {
            "getExecutionRequestDownloadUrl": {
                "downloadUrl": "https://s3.amazonaws.com/bucket/key?signature=xyz",
                "expiresIn": 3600,
            }
        }
        mock_stream_grep.return_value = {
            "logs": "ERROR found",
            "matches_found": 1,
            "matches_returned": 1,
            "total_lines": 100,
            "lines_returned": 30,
        }

        result = grep_full_ingestion_logs_from_s3(
            "urn:li:dataHubExecutionRequest:test",
            grep_phrase="ERROR",
        )

        # Verify grep mode was called with correct parameters
        mock_stream_grep.assert_called_once()
        assert mock_stream_grep.call_args[1]["grep_phrase"] == "ERROR"

        # Verify result is returned correctly
        assert result["logs"] == "ERROR found"
        assert result["matches_found"] == 1

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_missing_download_url(
        self, mock_execute_graphql: Mock, mock_get_client: Mock, mock_track_event: Mock
    ) -> None:
        """Test handling when S3 logs are not available."""
        mock_execute_graphql.return_value = {"getExecutionRequestDownloadUrl": None}

        result = grep_full_ingestion_logs_from_s3(
            "urn:li:dataHubExecutionRequest:test", grep_phrase="ERROR"
        )

        assert "message" in result
        assert "S3 logs not available" in result["message"]
        assert result["total_lines"] == 0

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.requests.head")
    @patch("datahub_integrations.chat.agents.tools.ingestion.stream_grep_mode")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_tracks_grep_parameters(
        self,
        mock_execute_graphql: Mock,
        mock_get_client: Mock,
        mock_stream_grep: Mock,
        mock_requests_head: Mock,
        mock_track_event: Mock,
    ) -> None:
        """Test that grep parameters are tracked correctly."""
        mock_execute_graphql.return_value = {
            "getExecutionRequestDownloadUrl": {
                "downloadUrl": "https://s3.amazonaws.com/bucket/logs/file.log.gz?signature=xyz",
                "expiresIn": 3600,
            }
        }
        mock_stream_grep.return_value = {
            "logs": "test logs",
            "total_lines": 500,
            "lines_returned": 75,
            "matches_found": 10,
            "matches_returned": 3,
            "truncated": True,
        }

        # Mock HEAD request
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {"Content-Length": "5242880"}  # 5 MB
        mock_requests_head.return_value = mock_head_response

        grep_full_ingestion_logs_from_s3(
            "urn:li:dataHubExecutionRequest:test",
            grep_phrase="ERROR",
            lines_after_match=30,
            lines_before_match=10,
            max_matches_returned=3,
        )

        # Verify request event has grep parameters
        request_event = mock_track_event.call_args_list[0][0][0]
        assert request_event.mode == "grep"
        assert request_event.grep_phrase == "ERROR"
        assert request_event.lines_after_match == 30
        assert request_event.lines_before_match == 10
        assert request_event.max_matches_returned == 3

        # Verify response event has grep results
        response_event = mock_track_event.call_args_list[1][0][0]
        assert response_event.mode == "grep"
        assert response_event.matches_found == 10
        assert response_event.matches_returned == 3
        assert response_event.truncated is True


class TestGetFullIngestionLogWindowFromS3:
    """Tests for get_full_ingestion_log_window_from_s3 function."""

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.requests.head")
    @patch("datahub_integrations.chat.agents.tools.ingestion.stream_windowing_mode")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_gets_presigned_url_and_streams_logs(
        self,
        mock_execute_graphql: Mock,
        mock_get_client: Mock,
        mock_stream_windowing: Mock,
        mock_requests_head: Mock,
        mock_track_event: Mock,
    ) -> None:
        """Test that function gets presigned URL and streams logs."""
        mock_execute_graphql.return_value = {
            "getExecutionRequestDownloadUrl": {
                "downloadUrl": "https://s3.amazonaws.com/bucket/key?signature=xyz",
                "expiresIn": 3600,
            }
        }
        mock_stream_windowing.return_value = {
            "logs": "Log content",
            "total_lines": 100,
            "lines_returned": 50,
        }

        result = get_full_ingestion_log_window_from_s3(
            "urn:li:dataHubExecutionRequest:test",
            lines_from_end=50,
        )

        # Verify GraphQL was called with correct operation
        assert (
            mock_execute_graphql.call_args[1]["operation_name"]
            == "getExecutionRequestDownloadUrl"
        )

        # Verify streaming function was called with presigned URL
        mock_stream_windowing.assert_called_once()
        assert (
            mock_stream_windowing.call_args[1]["presigned_url"]
            == "https://s3.amazonaws.com/bucket/key?signature=xyz"
        )

        # Verify result is returned
        assert result["logs"] == "Log content"
        assert result["total_lines"] == 100

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_missing_download_url(
        self, mock_execute_graphql: Mock, mock_get_client: Mock, mock_track_event: Mock
    ) -> None:
        """Test handling when S3 logs are not available."""
        mock_execute_graphql.return_value = {"getExecutionRequestDownloadUrl": None}

        result = get_full_ingestion_log_window_from_s3(
            "urn:li:dataHubExecutionRequest:test"
        )

        assert "message" in result
        assert "S3 logs not available" in result["message"]
        assert result["total_lines"] == 0

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_handles_graphql_error(
        self, mock_execute_graphql: Mock, mock_get_client: Mock, mock_track_event: Mock
    ) -> None:
        """Test handling when GraphQL call fails."""
        mock_execute_graphql.side_effect = Exception("GraphQL error")

        result = get_full_ingestion_log_window_from_s3(
            "urn:li:dataHubExecutionRequest:test"
        )

        assert "message" in result
        assert "Failed to get S3 download URL" in result["message"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.requests.head")
    @patch("datahub_integrations.chat.agents.tools.ingestion.stream_windowing_mode")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_passes_all_parameters_to_streaming_functions(
        self,
        mock_execute_graphql: Mock,
        mock_get_client: Mock,
        mock_stream_windowing: Mock,
        mock_requests_head: Mock,
        mock_track_event: Mock,
    ) -> None:
        """Test that all parameters are passed correctly to streaming functions."""
        mock_execute_graphql.return_value = {
            "getExecutionRequestDownloadUrl": {
                "downloadUrl": "https://s3.amazonaws.com/bucket/key?signature=xyz",
                "expiresIn": 3600,
            }
        }
        mock_stream_windowing.return_value = {"logs": "test"}

        get_full_ingestion_log_window_from_s3(
            "urn:li:dataHubExecutionRequest:test",
            lines_from_end=200,
            offset_from_end=100,
        )

        # Verify parameters were passed
        assert mock_stream_windowing.call_args[1]["lines_from_end"] == 200
        assert mock_stream_windowing.call_args[1]["offset_from_end"] == 100

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.requests.head")
    @patch("datahub_integrations.chat.agents.tools.ingestion.stream_windowing_mode")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_emits_tracking_events_on_success(
        self,
        mock_execute_graphql: Mock,
        mock_get_client: Mock,
        mock_stream_windowing: Mock,
        mock_requests_head: Mock,
        mock_track_event: Mock,
    ) -> None:
        """Test that tracking events are emitted on successful S3 streaming."""
        mock_execute_graphql.return_value = {
            "getExecutionRequestDownloadUrl": {
                "downloadUrl": "https://s3.amazonaws.com/bucket/logs/file.log.gz?signature=xyz",
                "expiresIn": 3600,
            }
        }
        mock_stream_windowing.return_value = {
            "logs": "test logs",
            "total_lines": 100,
            "lines_returned": 50,
        }

        # Mock HEAD request to return file size
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        mock_head_response.headers = {"Content-Length": "1048576"}  # 1 MB
        mock_requests_head.return_value = mock_head_response

        get_full_ingestion_log_window_from_s3(
            "urn:li:dataHubExecutionRequest:test",
            lines_from_end=150,
        )

        # Verify tracking events were emitted
        assert mock_track_event.call_count == 2  # Request and Response events

        # Verify request event
        request_event = mock_track_event.call_args_list[0][0][0]
        assert isinstance(request_event, S3LogStreamingRequestEvent)
        assert (
            request_event.execution_request_urn == "urn:li:dataHubExecutionRequest:test"
        )
        assert request_event.mode == "windowing"
        assert request_event.lines_from_end == 150

        # Verify response event
        response_event = mock_track_event.call_args_list[1][0][0]
        assert isinstance(response_event, S3LogStreamingResponseEvent)
        assert (
            response_event.execution_request_urn
            == "urn:li:dataHubExecutionRequest:test"
        )
        assert response_event.mode == "windowing"
        assert response_event.total_lines == 100
        assert response_event.lines_returned == 50
        assert response_event.s3_file_size_bytes == 1048576
        assert response_event.s3_file_path is not None
        assert "logs/file.log.gz" in response_event.s3_file_path
        assert response_event.stream_duration_ms >= 0  # Can be 0 in mocked test
        assert response_event.error_msg is None

    @patch("datahub_integrations.chat.agents.tools.ingestion.track_saas_event")
    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_emits_tracking_events_on_s3_unavailable(
        self,
        mock_execute_graphql: Mock,
        mock_get_client: Mock,
        mock_track_event: Mock,
    ) -> None:
        """Test that tracking events are emitted when S3 logs are unavailable."""
        mock_execute_graphql.return_value = {"getExecutionRequestDownloadUrl": None}

        get_full_ingestion_log_window_from_s3("urn:li:dataHubExecutionRequest:test")

        # Verify tracking events were emitted
        assert mock_track_event.call_count == 2  # Request and Response events

        # Verify request event
        request_event = mock_track_event.call_args_list[0][0][0]
        assert isinstance(request_event, S3LogStreamingRequestEvent)

        # Verify response event includes error
        response_event = mock_track_event.call_args_list[1][0][0]
        assert isinstance(response_event, S3LogStreamingResponseEvent)
        assert response_event.is_s3_unavailable is True
        assert response_event.error_msg is not None
        assert "S3 logs not available" in response_event.error_msg
