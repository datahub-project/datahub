"""
Unit tests for Snowflake connection retry logic.

Tests the retry behavior for ACCOUNT_USAGE intermittent unavailability.
"""

import threading
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakePermissionError,
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
    """Unit tests for connection retry behavior."""

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

    @patch("datahub.ingestion.source.snowflake.snowflake_connection.snowflake")
    def test_all_retries_exhausted_raises_original_snowflake_error(
        self, mock_snowflake
    ):
        """Test that when all retries fail, the original Snowflake error is raised."""
        mock_cursor = MagicMock()
        mock_native_conn = MagicMock()
        mock_native_conn.cursor.return_value = mock_cursor

        call_count = 0

        def mock_execute(query):
            nonlocal call_count
            call_count += 1
            # All attempts fail with the same ACCOUNT_USAGE error
            raise MockSnowflakeError(
                "002003: Object 'SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES' does not exist or not authorized"
            )

        mock_cursor.execute = mock_execute
        conn = SnowflakeConnection(mock_native_conn)

        query = "SELECT * FROM snowflake.account_usage.tag_references"

        # Should raise SnowflakePermissionError (which wraps the original error)
        # This is expected because _is_permission_error() catches "not authorized"
        with pytest.raises(SnowflakePermissionError) as exc_info:
            conn.query(query)

        # Verify the error message contains the real Snowflake error details
        assert "002003" in str(exc_info.value)
        assert "TAG_REFERENCES" in str(exc_info.value)

        # Should have tried 3 times (initial + 2 retries)
        assert call_count == 3

    @patch("datahub.ingestion.source.snowflake.snowflake_connection.snowflake")
    def test_real_permission_error_on_user_table_fails_immediately(
        self, mock_snowflake
    ):
        """Test that 002003 errors on non-ACCOUNT_USAGE tables fail without retry."""
        mock_cursor = MagicMock()
        mock_native_conn = MagicMock()
        mock_native_conn.cursor.return_value = mock_cursor

        call_count = 0

        def mock_execute(query):
            nonlocal call_count
            call_count += 1
            # Real permission error on a user table (not ACCOUNT_USAGE)
            raise MockSnowflakeError(
                "002003: Database 'MY_DATABASE' does not exist or not authorized"
            )

        mock_cursor.execute = mock_execute
        conn = SnowflakeConnection(mock_native_conn)

        # Query to a user table, NOT ACCOUNT_USAGE
        query = "SELECT * FROM my_database.my_schema.my_table"

        # Should raise SnowflakePermissionError immediately
        with pytest.raises(SnowflakePermissionError):
            conn.query(query)

        # CRITICAL: Should only try ONCE, not 3 times
        assert call_count == 1

    @patch("datahub.ingestion.source.snowflake.snowflake_connection.snowflake")
    def test_concurrent_queries_with_retries_are_thread_safe(self, mock_snowflake):
        """Test that retry logic works correctly with concurrent queries."""
        # Track call counts globally across all cursor instances
        call_counts = {}
        lock = threading.Lock()

        def mock_cursor_factory(cursor_class=None):
            """Create a cursor that tracks calls per thread."""
            mock_cursor = MagicMock()

            def mock_execute(query):
                thread_name = threading.current_thread().name
                with lock:
                    if thread_name not in call_counts:
                        call_counts[thread_name] = 0
                    call_counts[thread_name] += 1
                    current_count = call_counts[thread_name]

                # Fail first 2 attempts, succeed on 3rd
                if current_count <= 2:
                    raise MockSnowflakeError(
                        "002003: Object 'SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES' does not exist or not authorized"
                    )
                # On 3rd attempt, return success
                result = MagicMock()
                result.rowcount = 100
                return result

            mock_cursor.execute = mock_execute
            return mock_cursor

        mock_native_conn = MagicMock()
        mock_native_conn.cursor.side_effect = mock_cursor_factory

        conn = SnowflakeConnection(mock_native_conn)

        query = "SELECT * FROM snowflake.account_usage.tag_references"
        results = []
        errors = []

        def run_query(thread_name):
            threading.current_thread().name = thread_name
            try:
                result = conn.query(query)
                with lock:
                    results.append((thread_name, result))
            except Exception as e:
                with lock:
                    errors.append((thread_name, e))

        # Run 2 queries concurrently
        thread1 = threading.Thread(
            target=run_query, args=("thread_1",), name="thread_1"
        )
        thread2 = threading.Thread(
            target=run_query, args=("thread_2",), name="thread_2"
        )

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        # Debug: print errors if any
        if errors:
            for thread_name, error in errors:
                print(f"{thread_name} error: {error}")

        # Both threads should succeed after retries
        assert len(results) == 2, (
            f"Expected 2 results, got {len(results)}. Errors: {errors}"
        )
        assert len(errors) == 0, f"Expected no errors, got {len(errors)}: {errors}"
        assert call_counts["thread_1"] == 3
        assert call_counts["thread_2"] == 3
