import gc
import weakref
from typing import Any
from unittest.mock import Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig

from datahub_executor.common.connection.databricks.databricks_connection import (
    DATABRICKS_USER_AGENT_ENTRY,
    DatabricksConnection,
)
from datahub_executor.common.exceptions import SourceConnectionErrorException


class TestDatabricksConnection:
    """Test DatabricksConnection lifecycle management and memory leak prevention"""

    def setup_method(self) -> None:
        """Setup test fixtures"""
        self.test_urn = "urn:li:dataPlatform:databricks"
        self.mock_config = Mock(spec=UnityCatalogSourceConfig)
        self.mock_graph = Mock(spec=DataHubGraph)
        self.mock_native_connection = Mock()
        self.mock_native_connection._closed = False
        self.mock_native_connection.close = Mock()

        # Setup config attributes
        self.mock_config.workspace_url = "https://test-workspace.databricks.com/"
        self.mock_config.warehouse_id = "test-warehouse-id"
        self.mock_config.token = "test-token"

    def test_connection_creation(self) -> None:
        """Test basic connection creation"""
        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        assert connection.urn == self.test_urn
        assert connection.config == self.mock_config
        assert connection.graph == self.mock_graph
        assert connection.connection is None
        assert connection._finalizer is not None

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_get_client_creates_connection_when_none(self, mock_sql: Any) -> None:
        """Test that get_client creates a new connection when none exists"""
        mock_sql.connect.return_value = self.mock_native_connection

        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        client = connection.get_client()

        assert client == self.mock_native_connection
        assert connection.connection == self.mock_native_connection

        # Verify connection parameters
        mock_sql.connect.assert_called_once_with(
            server_hostname="test-workspace.databricks.com",
            http_path="/sql/1.0/warehouses/test-warehouse-id",
            access_token="test-token",
            user_agent_entry=DATABRICKS_USER_AGENT_ENTRY,
        )

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_get_client_reuses_existing_connection(self, mock_sql: Any) -> None:
        """Test that get_client reuses existing open connection"""
        mock_sql.connect.return_value = self.mock_native_connection

        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # First call creates connection
        client1 = connection.get_client()
        # Second call should reuse
        client2 = connection.get_client()

        assert client1 == client2 == self.mock_native_connection
        # Should only call connect once
        mock_sql.connect.assert_called_once()

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_get_client_reconnects_when_closed(self, mock_sql: Any) -> None:
        """Test that get_client creates new connection when existing one is closed"""
        mock_sql.connect.return_value = self.mock_native_connection

        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # First call creates connection
        client1 = connection.get_client()

        # Simulate connection being closed
        self.mock_native_connection._closed = True

        # Create a new mock for the reconnected connection
        new_mock_connection = Mock()
        new_mock_connection._closed = False
        mock_sql.connect.return_value = new_mock_connection

        # Second call should create new connection
        client2 = connection.get_client()

        assert client1 != client2
        assert client2 == new_mock_connection
        # Should call connect twice (initial + reconnect)
        assert mock_sql.connect.call_count == 2

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_explicit_close(self, mock_sql: Any) -> None:
        """Test explicit connection close"""
        mock_sql.connect.return_value = self.mock_native_connection

        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Create connection
        connection.get_client()
        assert connection.connection is not None

        # Close it
        connection.close()

        assert connection.connection is None
        self.mock_native_connection.close.assert_called_once()

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_context_manager(self, mock_sql: Any) -> None:
        """Test connection as context manager"""
        mock_sql.connect.return_value = self.mock_native_connection

        with DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        ) as connection:
            client = connection.get_client()
            assert client == self.mock_native_connection

        # Connection should be closed after exiting context
        self.mock_native_connection.close.assert_called_once()

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_finalizer_cleanup(self, mock_sql: Any) -> None:
        """Test that finalizer cleans up connection when object is garbage collected"""
        mock_sql.connect.return_value = self.mock_native_connection

        # Create connection and get client to establish native connection
        connection = DatabricksConnection(
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
        native_conn.close.assert_called_once()

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_close_handles_exceptions(self, mock_sql: Any) -> None:
        """Test that close method handles exceptions gracefully"""
        mock_sql.connect.return_value = self.mock_native_connection

        connection = DatabricksConnection(
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

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_get_client_handles_connection_creation_failure(
        self, mock_sql: Any
    ) -> None:
        """Test that get_client handles connection creation failures"""
        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Make connection creation fail
        mock_sql.connect.side_effect = Exception("Connection failed")

        # Should raise SourceConnectionErrorException
        with pytest.raises(
            SourceConnectionErrorException,
            match="Unable to connect to databricks instance",
        ):
            connection.get_client()

    @patch("datahub_executor.common.connection.databricks.databricks_connection.logger")
    def test_cleanup_logs_errors(self, mock_logger: Any) -> None:
        """Test that cleanup method logs errors appropriately"""
        # Test successful cleanup
        mock_conn = Mock()
        DatabricksConnection._cleanup_connection(mock_conn)
        mock_conn.close.assert_called_once()
        mock_logger.debug.assert_called_with(
            "Databricks connection closed during cleanup"
        )

        # Test cleanup with exception
        mock_logger.reset_mock()
        mock_conn.close.side_effect = Exception("Cleanup failed")
        DatabricksConnection._cleanup_connection(mock_conn)
        mock_logger.warning.assert_called_with(
            "Error closing Databricks connection during cleanup: Cleanup failed"
        )

    def test_cleanup_handles_none_connection(self) -> None:
        """Test that cleanup handles None connection gracefully"""
        # Should not raise any exception
        DatabricksConnection._cleanup_connection(None)

    def test_get_server_hostname(self) -> None:
        """Test server hostname extraction from workspace URL"""
        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Test with trailing slash
        self.mock_config.workspace_url = "https://test-workspace.databricks.com/"
        hostname = connection._get_server_hostname()
        assert hostname == "test-workspace.databricks.com"

        # Test without trailing slash
        self.mock_config.workspace_url = "https://test-workspace.databricks.com"
        hostname = connection._get_server_hostname()
        assert hostname == "test-workspace.databricks.com"

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_connection_without_closed_attribute(self, mock_sql: Any) -> None:
        """Test connection handling when native connection doesn't have _closed attribute"""
        # Create a mock connection without _closed attribute
        mock_connection_without_closed = Mock(spec=[])  # Empty spec means no attributes
        mock_sql.connect.return_value = mock_connection_without_closed

        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # First call creates connection
        client1 = connection.get_client()
        # Second call should reuse (since hasattr check will return False)
        client2 = connection.get_client()

        assert client1 == client2 == mock_connection_without_closed
        # Should only call connect once
        mock_sql.connect.assert_called_once()

    @patch("datahub_executor.common.connection.databricks.databricks_connection.sql")
    def test_multiple_finalizers_updated_correctly(self, mock_sql: Any) -> None:
        """Test that finalizer is updated correctly when reconnecting"""
        mock_sql.connect.return_value = self.mock_native_connection

        connection = DatabricksConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Create initial connection
        client1 = connection.get_client()
        initial_finalizer = connection._finalizer

        # Simulate connection being closed and force reconnection
        self.mock_native_connection._closed = True
        new_mock_connection = Mock()
        new_mock_connection._closed = False
        mock_sql.connect.return_value = new_mock_connection

        # Get client again (should reconnect)
        client2 = connection.get_client()
        new_finalizer = connection._finalizer

        # Finalizer should be different (updated)
        assert initial_finalizer != new_finalizer
        assert client1 != client2
