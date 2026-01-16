"""
Unit tests for Snowflake connection retry logic.

Tests the retry behavior for ACCOUNT_USAGE intermittent unavailability.
"""

from unittest.mock import MagicMock, patch

from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    _is_retryable_account_usage_error,
)


class MockSnowflakeError(Exception):
    """Mock Snowflake error for testing."""

    pass


class TestRetryLogic:
    """Test suite for ACCOUNT_USAGE retry logic."""

    def test_is_retryable_account_usage_error_with_valid_conditions(self):
        """Test that ACCOUNT_USAGE queries with 002003 error are retryable."""
        query = "SELECT * FROM snowflake.account_usage.tag_references"
        error = MockSnowflakeError(
            "002003 (42S02): SQL compilation error: "
            "Object 'SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES' does not exist or not authorized."
        )

        result = _is_retryable_account_usage_error(error, query)

        assert result is True

    def test_is_retryable_account_usage_error_without_account_usage_in_query(self):
        """Test that non-ACCOUNT_USAGE queries are not retryable."""
        query = "SELECT * FROM my_database.my_schema.my_table"
        error = MockSnowflakeError(
            "002003 (42S02): SQL compilation error: "
            "Object 'MY_TABLE' does not exist or not authorized."
        )

        result = _is_retryable_account_usage_error(error, query)

        assert result is False

    def test_is_retryable_account_usage_error_with_different_error_code(self):
        """Test that ACCOUNT_USAGE queries with different errors are not retryable."""
        query = "SELECT * FROM snowflake.account_usage.query_history"
        error = MockSnowflakeError(
            "001003: SQL compilation error: syntax error line 1 at position 15"
        )

        result = _is_retryable_account_usage_error(error, query)

        assert result is False


class TestConnectionRetry:
    """Integration tests for connection retry behavior."""

    @patch("datahub.ingestion.source.snowflake.snowflake_connection.snowflake")
    def test_successful_retry_after_transient_error(self, mock_snowflake):
        """Test that query succeeds after transient ACCOUNT_USAGE error."""
        mock_cursor = MagicMock()
        mock_native_conn = MagicMock()
        mock_native_conn.cursor.return_value = mock_cursor

        call_count = 0

        def mock_execute(query):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise MockSnowflakeError(
                    "002003: Object 'SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES' does not exist or not authorized"
                )
            result = MagicMock()
            result.rowcount = 100
            return result

        mock_cursor.execute = mock_execute

        conn = SnowflakeConnection(mock_native_conn)

        query = "SELECT * FROM snowflake.account_usage.tag_references"
        result = conn.query(query)

        assert result is not None
        assert call_count == 3
