import gc
import weakref
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)

from datahub_executor.common.connection.snowflake.snowflake_connection import (
    SnowflakeConnection,
)


class TestSnowflakeConnection:
    """Test SnowflakeConnection lifecycle management and memory leak prevention"""

    def setup_method(self) -> None:
        """Setup test fixtures"""
        self.test_urn = "urn:li:dataPlatform:snowflake"
        self.mock_config = Mock(spec=SnowflakeConnectionConfig)
        self.mock_graph = Mock(spec=DataHubGraph)
        self.mock_native_connection = Mock()
        self.mock_native_connection.is_closed.return_value = False
        self.mock_native_connection.close = Mock()

        # Setup config to return our mock connection
        self.mock_config.get_native_connection.return_value = (
            self.mock_native_connection
        )

    def test_connection_creation(self) -> None:
        """Test basic connection creation"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        assert connection.urn == self.test_urn
        assert connection.config == self.mock_config
        assert connection.graph == self.mock_graph
        assert connection.connection is None
        assert connection._finalizer is not None

    def test_get_client_creates_connection_when_none(self) -> None:
        """Test that get_client creates a new connection when none exists"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        client = connection.get_client()

        assert client == self.mock_native_connection
        assert connection.connection == self.mock_native_connection
        self.mock_config.get_native_connection.assert_called_once()

    def test_get_client_reuses_existing_connection(self) -> None:
        """Test that get_client reuses existing open connection"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # First call creates connection
        client1 = connection.get_client()
        # Second call should reuse
        client2 = connection.get_client()

        assert client1 == client2 == self.mock_native_connection
        # Should only call get_native_connection once
        self.mock_config.get_native_connection.assert_called_once()

    def test_get_client_reconnects_when_closed(self) -> None:
        """Test that get_client creates new connection when existing one is closed"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # First call creates connection
        client1 = connection.get_client()

        # Simulate connection being closed
        self.mock_native_connection.is_closed.return_value = True

        # Create a new mock for the reconnected connection
        new_mock_connection = Mock()
        new_mock_connection.is_closed.return_value = False
        self.mock_config.get_native_connection.return_value = new_mock_connection

        # Second call should create new connection
        client2 = connection.get_client()

        assert client1 != client2
        assert client2 == new_mock_connection
        # Should call get_native_connection twice (initial + reconnect)
        assert self.mock_config.get_native_connection.call_count == 2

    def test_explicit_close(self) -> None:
        """Test explicit connection close"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Create connection
        connection.get_client()
        assert connection.connection is not None

        # Close it
        connection.close()

        assert connection.connection is None
        self.mock_native_connection.close.assert_called_once()

    def test_context_manager(self) -> None:
        """Test connection as context manager"""
        with SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        ) as connection:
            client = connection.get_client()
            assert client == self.mock_native_connection

        # Connection should be closed after exiting context
        self.mock_native_connection.close.assert_called_once()

    def test_finalizer_cleanup(self) -> None:
        """Test that finalizer cleans up connection when object is garbage collected"""
        # Create connection and get client to establish native connection
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )
        connection.get_client()

        # Keep a reference to the native connection to verify cleanup
        native_conn = connection.connection
        assert native_conn is not None  # Ensure connection was established

        # Create a weak reference to track when connection object is deleted
        weak_ref = weakref.ref(connection)

        # Delete the connection object
        del connection

        # Force garbage collection
        gc.collect()

        # Verify the connection object was deleted
        assert weak_ref() is None

        # The finalizer should have called close on the native connection
        cast(Mock, native_conn.close).assert_called_once()

    def test_close_handles_exceptions(self) -> None:
        """Test that close method handles exceptions gracefully"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Create connection
        connection.get_client()

        # Make close raise an exception
        self.mock_native_connection.close.side_effect = Exception("Close failed")

        # Should not raise exception
        connection.close()

        # Connection should still be set to None despite exception
        assert connection.connection is None

    def test_get_client_handles_connection_creation_failure(self) -> None:
        """Test that get_client handles connection creation failures"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Make connection creation fail
        self.mock_config.get_native_connection.side_effect = Exception(
            "Connection failed"
        )

        # Should raise the exception
        with pytest.raises(Exception, match="Connection failed"):
            connection.get_client()

    @patch("datahub_executor.common.connection.snowflake.snowflake_connection.logger")
    def test_cleanup_logs_errors(self, mock_logger: Any) -> None:
        """Test that cleanup method logs errors appropriately"""
        # Test successful cleanup
        mock_conn = Mock()
        SnowflakeConnection._cleanup_connection(mock_conn)
        mock_conn.close.assert_called_once()
        mock_logger.debug.assert_called_with(
            "Snowflake connection closed during cleanup"
        )

        # Test cleanup with exception
        mock_logger.reset_mock()
        mock_conn.close.side_effect = Exception("Cleanup failed")
        SnowflakeConnection._cleanup_connection(mock_conn)
        mock_logger.warning.assert_called_with(
            "Error closing Snowflake connection during cleanup: Cleanup failed"
        )

    def test_cleanup_handles_none_connection(self) -> None:
        """Test that cleanup handles None connection gracefully"""
        # Should not raise any exception
        SnowflakeConnection._cleanup_connection(None)

    def test_multiple_finalizers_updated_correctly(self) -> None:
        """Test that finalizer is updated correctly when reconnecting"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Create initial connection
        client1 = connection.get_client()
        initial_finalizer = connection._finalizer

        # Simulate connection being closed and force reconnection
        self.mock_native_connection.is_closed.return_value = True
        new_mock_connection = Mock()
        new_mock_connection.is_closed.return_value = False
        self.mock_config.get_native_connection.return_value = new_mock_connection

        # Get client again (should reconnect)
        client2 = connection.get_client()
        new_finalizer = connection._finalizer

        # Finalizer should be different (updated)
        assert initial_finalizer != new_finalizer
        assert client1 != client2

    def test_connection_state_after_explicit_close(self) -> None:
        """Test connection state after explicit close"""
        connection = SnowflakeConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Create connection
        connection.get_client()
        assert connection.connection is not None

        # Close explicitly
        connection.close()
        assert connection.connection is None

        # Getting client again should create new connection
        new_mock_connection = Mock()
        new_mock_connection.is_closed.return_value = False
        self.mock_config.get_native_connection.return_value = new_mock_connection

        client = connection.get_client()
        assert client == new_mock_connection
        assert connection.connection == new_mock_connection
