"""
Tests for cursor context manager usage across database sources.
These tests verify that cursors are properly managed using context managers
to prevent memory leaks.
"""

from unittest.mock import MagicMock, Mock, call, patch

import pytest
from datahub._version import __version__

from datahub_executor.common.connection.databricks.databricks_connection import (
    DatabricksConnection,
)
from datahub_executor.common.connection.redshift.redshift_connection import (
    RedshiftConnection,
)
from datahub_executor.common.connection.snowflake.snowflake_connection import (
    SnowflakeConnection,
)
from datahub_executor.common.exceptions import SourceQueryFailedException
from datahub_executor.common.source.databricks.databricks import DatabricksSource
from datahub_executor.common.source.redshift.redshift import RedshiftSource
from datahub_executor.common.source.snowflake.snowflake import SnowflakeSource


class TestSnowflakeCursorManagement:
    """Test cursor context manager usage in SnowflakeSource"""

    def setup_method(self) -> None:
        """Setup test fixtures"""
        self.mock_connection = Mock(spec=SnowflakeConnection)
        self.mock_client = Mock()
        self.mock_cursor = Mock()

        # Setup the mock chain: connection -> client -> cursor
        self.mock_connection.get_client.return_value = self.mock_client
        self.mock_client.cursor.return_value.__enter__ = Mock(
            return_value=self.mock_cursor
        )
        self.mock_client.cursor.return_value.__exit__ = Mock(return_value=None)

        # Mock cursor context manager
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = self.mock_cursor
        cursor_context.__exit__.return_value = None
        self.mock_client.cursor.return_value = cursor_context

    @patch(
        "datahub_executor.common.source.snowflake.snowflake.DATAHUB_EXECUTOR_SNOWFLAKE_TIMEOUT",
        300,
    )
    def test_snowflake_source_init_uses_cursor_context_manager(self) -> None:
        """Test that SnowflakeSource.__init__ uses cursor context manager"""
        self.mock_cursor.execute = Mock()

        # Create source (this will trigger the cursor usage in __init__)
        _ = SnowflakeSource(self.mock_connection)

        # Verify cursor context manager was used
        self.mock_client.cursor.assert_called_once()
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called_once()
        cursor_context.__exit__.assert_called_once()

        # Verify the SQL was executed
        expected_query = (
            "ALTER SESSION SET TIMEZONE = 'UTC', STATEMENT_TIMEOUT_IN_SECONDS = 300;"
        )
        self.mock_cursor.execute.assert_called_once_with(expected_query)

    def test_execute_fetchall_query_uses_cursor_context_manager(self) -> None:
        """Test that _execute_fetchall_query uses cursor context manager"""
        source = SnowflakeSource(self.mock_connection)
        test_query = "SELECT * FROM test_table"
        expected_result = [("row1",), ("row2",)]

        self.mock_cursor.execute = Mock()
        self.mock_cursor.fetchall.return_value = expected_result

        # Execute query
        result = source._execute_fetchall_query(test_query)

        # Verify cursor context manager was used
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()

        # Verify query execution
        self.mock_cursor.execute.assert_called_with(test_query)
        self.mock_cursor.fetchall.assert_called_once()
        assert result == expected_result

    def test_execute_fetchone_query_uses_cursor_context_manager(self) -> None:
        """Test that _execute_fetchone_query uses cursor context manager"""
        source = SnowflakeSource(self.mock_connection)
        test_query = "SELECT COUNT(*) FROM test_table"
        expected_result = ("42",)

        self.mock_cursor.execute = Mock()
        self.mock_cursor.fetchone.return_value = expected_result

        # Execute query
        result = source._execute_fetchone_query(test_query)

        # Verify cursor context manager was used
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()

        # Verify query execution
        self.mock_cursor.execute.assert_called_with(test_query)
        self.mock_cursor.fetchone.assert_called_once()
        assert result == list(expected_result)

    def test_cursor_closed_on_exception_in_fetchall(self) -> None:
        """Test that cursor is properly closed even when exception occurs in fetchall"""
        source = SnowflakeSource(self.mock_connection)
        test_query = "SELECT * FROM non_existent_table"

        self.mock_cursor.execute = Mock()
        self.mock_cursor.fetchall.side_effect = Exception("Table not found")

        # Execute query and expect exception
        with pytest.raises(SourceQueryFailedException, match="Table not found"):
            source._execute_fetchall_query(test_query)

        # Verify cursor context manager was used (including __exit__)
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()

    def test_cursor_closed_on_exception_in_fetchone(self) -> None:
        """Test that cursor is properly closed even when exception occurs in fetchone"""
        source = SnowflakeSource(self.mock_connection)
        test_query = "SELECT COUNT(*) FROM non_existent_table"

        self.mock_cursor.execute = Mock()
        self.mock_cursor.fetchone.side_effect = Exception("Table not found")

        # Execute query and expect exception
        with pytest.raises(SourceQueryFailedException, match="Table not found"):
            source._execute_fetchone_query(test_query)

        # Verify cursor context manager was used (including __exit__)
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()


class TestRedshiftCursorManagement:
    """Test cursor context manager usage in RedshiftSource"""

    def setup_method(self) -> None:
        """Setup test fixtures"""
        self.mock_connection = Mock(spec=RedshiftConnection)
        self.mock_client = Mock()
        self.mock_cursor = Mock()

        # Setup the mock chain: connection -> client -> cursor
        self.mock_connection.get_client.return_value = self.mock_client

        # Mock cursor context manager
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = self.mock_cursor
        cursor_context.__exit__.return_value = None
        self.mock_client.cursor.return_value = cursor_context

    def test_execute_fetchall_query_uses_cursor_context_manager(self) -> None:
        """Test that _execute_fetchall_query uses cursor context manager"""
        source = RedshiftSource(self.mock_connection)
        test_query = "SELECT * FROM test_table"
        expected_result = [("row1",), ("row2",)]

        self.mock_cursor.execute = Mock()
        self.mock_cursor.fetchall.return_value = expected_result

        # Execute query
        result = source._execute_fetchall_query(test_query)

        # Verify cursor context manager was used
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()

        # Verify query execution with query tagging
        expected_tagged_query = f"-- partner: DataHub -v {__version__}\n{test_query}"
        self.mock_cursor.execute.assert_called_with(expected_tagged_query)
        self.mock_cursor.fetchall.assert_called_once()
        assert result == expected_result

    def test_execute_fetchone_query_uses_cursor_context_manager(self) -> None:
        """Test that _execute_fetchone_query uses cursor context manager"""
        source = RedshiftSource(self.mock_connection)
        test_query = "SELECT COUNT(*) FROM test_table"
        expected_result = ("42",)

        self.mock_cursor.execute = Mock()
        self.mock_cursor.fetchone.return_value = expected_result

        # Execute query
        result = source._execute_fetchone_query(test_query)

        # Verify cursor context manager was used
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()

        # Verify query execution with query tagging
        expected_tagged_query = f"-- partner: DataHub -v {__version__}\n{test_query}"
        self.mock_cursor.execute.assert_called_with(expected_tagged_query)
        self.mock_cursor.fetchone.assert_called_once()
        assert result == expected_result

    def test_rollback_handling_in_fetchall_exception(self) -> None:
        """Test that rollback is properly handled in fetchall exception with cursor context manager"""
        source = RedshiftSource(self.mock_connection)
        test_query = "SELECT * FROM non_existent_table"

        self.mock_cursor.execute = Mock()
        # First execute call for main query fails, second for rollback succeeds
        self.mock_cursor.execute.side_effect = [Exception("Table not found"), None]

        # Execute query and expect exception
        with pytest.raises(SourceQueryFailedException, match="Table not found"):
            source._execute_fetchall_query(test_query)

        # Verify cursor context manager was used
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()

        # Verify both main query (with tagging) and rollback were attempted
        expected_tagged_query = f"-- partner: DataHub -v {__version__}\n{test_query}"
        assert self.mock_cursor.execute.call_count == 2
        self.mock_cursor.execute.assert_has_calls(
            [call(expected_tagged_query), call("rollback")]
        )

    def test_rollback_exception_handled_gracefully(self) -> None:
        """Test that rollback exceptions are handled gracefully"""
        source = RedshiftSource(self.mock_connection)
        test_query = "SELECT * FROM non_existent_table"

        self.mock_cursor.execute = Mock()
        # Both main query and rollback fail
        self.mock_cursor.execute.side_effect = [
            Exception("Table not found"),
            Exception("Rollback failed"),
        ]

        # Execute query and expect original exception (not rollback exception)
        with pytest.raises(SourceQueryFailedException, match="Table not found"):
            source._execute_fetchall_query(test_query)

        # Verify cursor context manager was used
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()


class TestDatabricksCursorManagement:
    """Test cursor context manager usage in DatabricksSource"""

    def setup_method(self) -> None:
        """Setup test fixtures"""
        self.mock_connection = Mock(spec=DatabricksConnection)
        self.mock_client = Mock()
        self.mock_cursor = Mock()

        # Setup the mock chain: connection -> client -> cursor
        self.mock_connection.get_client.return_value = self.mock_client

        # Mock cursor context manager
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = self.mock_cursor
        cursor_context.__exit__.return_value = None
        self.mock_client.cursor.return_value = cursor_context

    def test_execute_fetchall_query_uses_cursor_context_manager(self) -> None:
        """Test that _execute_fetchall_query uses cursor context manager"""
        source = DatabricksSource(self.mock_connection)
        test_query = "SELECT * FROM test_table"
        expected_result = [("row1",), ("row2",)]

        # Mock the cursor.execute().fetchall() chain
        execute_result = Mock()
        execute_result.fetchall.return_value = expected_result
        self.mock_cursor.execute.return_value = execute_result

        # Execute query
        result = source._execute_fetchall_query(test_query)

        # Verify cursor context manager was used
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()

        # Verify query execution
        self.mock_cursor.execute.assert_called_with(test_query)
        execute_result.fetchall.assert_called_once()
        assert result == expected_result

    def test_execute_fetchone_query_uses_cursor_context_manager(self) -> None:
        """Test that _execute_fetchone_query uses cursor context manager"""
        source = DatabricksSource(self.mock_connection)
        test_query = "SELECT COUNT(*) FROM test_table"
        expected_result = ("42",)

        # Mock the cursor.execute().fetchone() chain
        execute_result = Mock()
        execute_result.fetchone.return_value = expected_result
        self.mock_cursor.execute.return_value = execute_result

        # Execute query
        result = source._execute_fetchone_query(test_query)

        # Verify cursor context manager was used
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()

        # Verify query execution
        self.mock_cursor.execute.assert_called_with(test_query)
        execute_result.fetchone.assert_called_once()
        assert result == expected_result

    def test_cursor_closed_on_exception_in_databricks_fetchall(self) -> None:
        """Test that cursor is properly closed even when exception occurs in Databricks fetchall"""
        source = DatabricksSource(self.mock_connection)
        test_query = "SELECT * FROM non_existent_table"

        self.mock_cursor.execute.side_effect = Exception("Table not found")

        # Execute query and expect exception
        with pytest.raises(SourceQueryFailedException, match="Table not found"):
            source._execute_fetchall_query(test_query)

        # Verify cursor context manager was used (including __exit__)
        cursor_context = self.mock_client.cursor.return_value
        cursor_context.__enter__.assert_called()
        cursor_context.__exit__.assert_called()


class TestCursorContextManagerIntegration:
    """Integration tests for cursor context manager usage across all sources"""

    def test_all_sources_use_context_managers(self) -> None:
        """Test that all database sources consistently use cursor context managers"""

        # Test data
        sources_and_connections = [
            (SnowflakeSource, SnowflakeConnection),
            (RedshiftSource, RedshiftConnection),
            (DatabricksSource, DatabricksConnection),
        ]

        for source_class, connection_class in sources_and_connections:
            # Setup mock connection
            mock_connection = Mock(spec=connection_class)
            mock_client = Mock()
            mock_cursor = Mock()

            mock_connection.get_client.return_value = mock_client

            # Mock cursor context manager
            cursor_context = MagicMock()
            cursor_context.__enter__.return_value = mock_cursor
            cursor_context.__exit__.return_value = None
            mock_client.cursor.return_value = cursor_context

            # Create source
            if source_class == SnowflakeSource:
                # Snowflake source uses cursor in __init__
                mock_cursor.execute = Mock()
                source = source_class(mock_connection)
            else:
                source = source_class(mock_connection)

            # Test _execute_fetchall_query
            test_query = "SELECT * FROM test"
            if source_class == DatabricksSource:
                # Databricks has different return pattern
                execute_result = Mock()
                execute_result.fetchall.return_value = [("test",)]
                mock_cursor.execute.return_value = execute_result
            else:
                mock_cursor.execute = Mock()
                mock_cursor.fetchall.return_value = [("test",)]

            # Reset mocks for clean test
            cursor_context.reset_mock()

            # Execute query
            _ = source._execute_fetchall_query(test_query)

            # Verify context manager was used
            cursor_context.__enter__.assert_called()
            cursor_context.__exit__.assert_called()

            print(f"✅ {source_class.__name__} properly uses cursor context managers")
