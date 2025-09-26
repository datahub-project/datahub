import gc
import weakref
from typing import Any
from unittest.mock import Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.redshift.config import RedshiftConfig

from datahub_executor.common.connection.redshift.redshift_connection import (
    RedshiftConnection,
)


class TestRedshiftConnection:
    """Test RedshiftConnection lifecycle management and memory leak prevention"""

    def setup_method(self) -> None:
        """Setup test fixtures"""
        self.test_urn = "urn:li:dataPlatform:redshift"
        self.mock_config = Mock(spec=RedshiftConfig)
        self.mock_graph = Mock(spec=DataHubGraph)
        self.mock_native_connection = Mock()
        self.mock_native_connection.closed = False
        self.mock_native_connection.close = Mock()

        # Setup config attributes
        self.mock_config.extra_client_options = {}
        self.mock_config.host_port = "test-host:5439"
        self.mock_config.username = "test_user"
        self.mock_config.database = "test_db"
        self.mock_config.password = Mock()
        self.mock_config.password.get_secret_value.return_value = "test_password"

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_connection_creation(self, mock_redshift_connector: Any) -> None:
        """Test basic connection creation"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection

        connection = RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        assert connection.urn == self.test_urn
        assert connection.config == self.mock_config
        assert connection.graph == self.mock_graph
        assert connection.connection is None
        assert connection._finalizer is not None

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_get_client_creates_connection_when_none(
        self, mock_redshift_connector: Any
    ) -> None:
        """Test that get_client creates a new connection when none exists"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection

        connection = RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        client = connection.get_client()

        assert client == self.mock_native_connection
        assert connection.connection == self.mock_native_connection

        # Verify connection parameters
        mock_redshift_connector.connect.assert_called_once_with(
            application_name="datahub-executor.Undefined",
            host="test-host",
            port=5439,
            user="test_user",
            database="test_db",
            password="test_password",
        )

        # Verify autocommit is set
        assert self.mock_native_connection.autocommit is True

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_get_client_reuses_existing_connection(
        self, mock_redshift_connector: Any
    ) -> None:
        """Test that get_client reuses existing open connection"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection

        connection = RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # First call creates connection
        client1 = connection.get_client()
        # Second call should reuse
        client2 = connection.get_client()

        assert client1 == client2 == self.mock_native_connection
        # Should only call connect once
        mock_redshift_connector.connect.assert_called_once()

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_get_client_reconnects_when_closed(
        self, mock_redshift_connector: Any
    ) -> None:
        """Test that get_client creates new connection when existing one is closed"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection

        connection = RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # First call creates connection
        client1 = connection.get_client()

        # Simulate connection being closed
        self.mock_native_connection.closed = True

        # Create a new mock for the reconnected connection
        new_mock_connection = Mock()
        new_mock_connection.closed = False
        mock_redshift_connector.connect.return_value = new_mock_connection

        # Second call should create new connection
        client2 = connection.get_client()

        assert client1 != client2
        assert client2 == new_mock_connection
        # Should call connect twice (initial + reconnect)
        assert mock_redshift_connector.connect.call_count == 2

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_explicit_close(self, mock_redshift_connector: Any) -> None:
        """Test explicit connection close"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection

        connection = RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Create connection
        connection.get_client()
        assert connection.connection is not None

        # Close it
        connection.close()

        assert connection.connection is None
        self.mock_native_connection.close.assert_called_once()

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_context_manager(self, mock_redshift_connector: Any) -> None:
        """Test connection as context manager"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection

        with RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        ) as connection:
            client = connection.get_client()
            assert client == self.mock_native_connection

        # Connection should be closed after exiting context
        self.mock_native_connection.close.assert_called_once()

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_finalizer_cleanup(self, mock_redshift_connector: Any) -> None:
        """Test that finalizer cleans up connection when object is garbage collected"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection

        # Create connection and get client to establish native connection
        connection = RedshiftConnection(
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

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_close_handles_exceptions(self, mock_redshift_connector: Any) -> None:
        """Test that close method handles exceptions gracefully"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection

        connection = RedshiftConnection(
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

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_get_client_handles_connection_creation_failure(
        self, mock_redshift_connector: Any
    ) -> None:
        """Test that get_client handles connection creation failures"""
        connection = RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        # Make connection creation fail
        mock_redshift_connector.connect.side_effect = Exception("Connection failed")

        # Should raise the exception
        with pytest.raises(Exception, match="Connection failed"):
            connection.get_client()

    @patch("datahub_executor.common.connection.redshift.redshift_connection.logger")
    def test_cleanup_logs_errors(self, mock_logger: Any) -> None:
        """Test that cleanup method logs errors appropriately"""
        # Test successful cleanup
        mock_conn = Mock()
        RedshiftConnection._cleanup_connection(mock_conn)
        mock_conn.close.assert_called_once()
        mock_logger.debug.assert_called_with(
            "Redshift connection closed during cleanup"
        )

        # Test cleanup with exception
        mock_logger.reset_mock()
        mock_conn.close.side_effect = Exception("Cleanup failed")
        RedshiftConnection._cleanup_connection(mock_conn)
        mock_logger.warning.assert_called_with(
            "Error closing Redshift connection during cleanup: Cleanup failed"
        )

    def test_cleanup_handles_none_connection(self) -> None:
        """Test that cleanup handles None connection gracefully"""
        # Should not raise any exception
        RedshiftConnection._cleanup_connection(None)

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_connection_with_none_password(self, mock_redshift_connector: Any) -> None:
        """Test connection creation when password is None"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection
        self.mock_config.password = None

        connection = RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        connection.get_client()

        # Verify connection was called with None password
        mock_redshift_connector.connect.assert_called_once_with(
            application_name="datahub-executor.Undefined",
            host="test-host",
            port=5439,
            user="test_user",
            database="test_db",
            password=None,
        )

    @patch(
        "datahub_executor.common.connection.redshift.redshift_connection.redshift_connector"
    )
    def test_connection_with_extra_client_options(
        self, mock_redshift_connector: Any
    ) -> None:
        """Test connection creation with extra client options"""
        mock_redshift_connector.connect.return_value = self.mock_native_connection
        self.mock_config.extra_client_options = {"ssl": True, "timeout": 30}

        connection = RedshiftConnection(
            self.test_urn, self.mock_config, self.mock_graph
        )

        connection.get_client()

        # Verify connection was called with extra options
        mock_redshift_connector.connect.assert_called_once_with(
            application_name="datahub-executor.Undefined",
            host="test-host",
            port=5439,
            user="test_user",
            database="test_db",
            password="test_password",
            ssl=True,
            timeout=30,
        )
