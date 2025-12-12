"""Tests for ingestion tools."""

from unittest.mock import Mock, patch

from datahub_integrations.chat.agents.tools.ingestion import (
    get_ingestion_execution_logs,
    get_ingestion_execution_request,
    get_ingestion_source,
)

SAMPLE_RECIPE_WITH_SECRETS = """
source:
  type: snowflake
  config:
    account_id: abc123
    password: my-secret-password
    warehouse: COMPUTE_WH
"""


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

        result = get_ingestion_execution_logs("urn:li:executionRequest:test")

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

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            lines_from_end=100,
            offset_from_end=150,
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

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            grep_phrase="ERROR",
            lines_after_match=1,
            lines_before_match=1,
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

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            grep_phrase="ERROR",
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

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            grep_phrase="ERROR",
            max_matches_returned=3,
            lines_after_match=1,
            lines_before_match=0,
        )

        # Should find all matches but only return limited number
        assert result["matches_found"] > result["matches_returned"]
        assert result["matches_returned"] == 3
        assert result["truncated"] is True
        assert "max_matches_returned" in result["truncation_reason"]

    @patch("datahub_integrations.chat.agents.tools.ingestion.get_datahub_client")
    @patch("datahub_integrations.chat.agents.tools.ingestion.execute_graphql")
    def test_grep_mode_truncates_by_max_total_lines(
        self, mock_execute_graphql: Mock, mock_get_client: Mock
    ) -> None:
        """Test grep mode truncates when max_total_lines would be exceeded."""
        log_lines = "\n".join([f"ERROR line {i}" for i in range(1, 11)])
        mock_execute_graphql.return_value = self._create_mock_graphql_result(log_lines)

        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test",
            grep_phrase="ERROR",
            max_matches_returned=10,
            lines_after_match=100,
            max_total_lines=50,
        )

        assert result["matches_found"] >= result["matches_returned"]
        assert result["truncated"] is True
        assert "max_total_lines" in result["truncation_reason"]

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
        result = get_ingestion_execution_logs(
            "urn:li:executionRequest:test", grep_phrase="ERROR"
        )

        # Defaults should limit output
        assert result["matches_returned"] <= 3  # max_matches_returned default
        assert result["lines_returned"] <= 500  # max_total_lines default

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
