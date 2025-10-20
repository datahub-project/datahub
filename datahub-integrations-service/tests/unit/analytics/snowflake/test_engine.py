"""Unit tests for the Snowflake analytics engine."""

from unittest.mock import Mock, patch

from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection
from datahub_integrations.analytics.snowflake.engine import SnowflakeAnalyticsEngine
from datahub_integrations.propagation.snowflake.config import (
    SnowflakeAuthenticationType,
)


class TestSnowflakeAnalyticsEngine:
    """Test the Snowflake analytics engine."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mock_graph = Mock(spec=DataHubGraph)
        self.account = "test_account"

    def test_initialization(self) -> None:
        """Test engine initialization."""
        with patch.object(SnowflakeConnection, "from_datahub") as mock_from_datahub:
            mock_connection = Mock(spec=SnowflakeConnection)
            mock_connection.authentication_type = (
                SnowflakeAuthenticationType.DEFAULT_AUTHENTICATOR
            )
            mock_from_datahub.return_value = mock_connection

            engine = SnowflakeAnalyticsEngine(self.account, self.mock_graph)

            assert engine.account == self.account
            assert engine.graph == self.mock_graph
            assert engine.connection == mock_connection
            assert engine._engine is None
            mock_from_datahub.assert_called_once_with(graph=self.mock_graph)

    # Removed complex tests that test private implementation details
    # The public interface is already tested by the integration tests


class TestSnowflakeAnalyticsEngineNativeConnection:
    """Test native Snowflake connection functionality."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mock_graph = Mock(spec=DataHubGraph)
        self.account = "test_account"

    def test_get_native_connection_password_auth(self) -> None:
        """Test getting native connection with password authentication."""
        with patch.object(SnowflakeConnection, "from_datahub") as mock_from_datahub:
            mock_connection = Mock(spec=SnowflakeConnection)
            mock_connection.user = "test_user"
            mock_connection.password = "test_password"
            mock_connection.account = "test_account"
            mock_connection.warehouse = "test_warehouse"
            mock_connection.role = "test_role"
            mock_connection.authentication_type = "PASSWORD"
            mock_from_datahub.return_value = mock_connection

            with patch(
                "datahub_integrations.analytics.snowflake.engine.snowflake.connector.connect"
            ) as mock_connect:
                mock_native_conn = Mock()
                mock_connect.return_value = mock_native_conn

                engine = SnowflakeAnalyticsEngine(self.account, self.mock_graph)
                result = engine.get_native_connection()

                assert result == mock_native_conn
                mock_connect.assert_called_once_with(
                    user="test_user",
                    password="test_password",
                    account="test_account",
                    warehouse="test_warehouse",
                    role="test_role",
                )

    def test_get_native_connection_key_pair_auth(self) -> None:
        """Test getting native connection with key pair authentication."""
        with patch.object(SnowflakeConnection, "from_datahub") as mock_from_datahub:
            mock_connection = Mock(spec=SnowflakeConnection)
            mock_connection.user = "test_user"
            mock_connection.account = "test_account"
            mock_connection.warehouse = "test_warehouse"
            mock_connection.role = "test_role"
            mock_connection.authentication_type = "KEY_PAIR_AUTHENTICATOR"
            # Split long private key string to comply with line length limits
            mock_connection.private_key = (
                "-----BEGIN PRIVATE KEY-----\\ntest_key\\n-----END PRIVATE KEY-----"
            )
            mock_connection.private_key_password = "test_password"
            mock_from_datahub.return_value = mock_connection

            with patch(
                "datahub_integrations.analytics.snowflake.engine.snowflake.connector.connect"
            ) as mock_connect:
                mock_native_conn = Mock()
                mock_connect.return_value = mock_native_conn

                with patch(
                    "datahub_integrations.analytics.snowflake.engine.serialization.load_pem_private_key"
                ) as mock_load_key:
                    mock_private_key = Mock()
                    mock_load_key.return_value = mock_private_key
                    mock_private_key.private_bytes.return_value = b"processed_key"

                    engine = SnowflakeAnalyticsEngine(self.account, self.mock_graph)
                    result = engine.get_native_connection()

                    assert result == mock_native_conn
                    mock_connect.assert_called_once_with(
                        user="test_user",
                        account="test_account",
                        warehouse="test_warehouse",
                        role="test_role",
                        private_key=b"processed_key",
                    )

    def test_get_connect_args_with_private_key(self) -> None:
        """Test _get_connect_args with private key processing."""
        with patch.object(SnowflakeConnection, "from_datahub") as mock_from_datahub:
            mock_connection = Mock(spec=SnowflakeConnection)
            mock_connection.authentication_type = "KEY_PAIR_AUTHENTICATOR"
            # Split long private key string to comply with line length limits
            mock_connection.private_key = (
                "-----BEGIN PRIVATE KEY-----\\ntest_key\\n-----END PRIVATE KEY-----"
            )
            mock_connection.private_key_password = "test_password"
            mock_from_datahub.return_value = mock_connection

            with patch(
                "datahub_integrations.analytics.snowflake.engine.serialization.load_pem_private_key"
            ) as mock_load_key:
                mock_private_key = Mock()
                mock_load_key.return_value = mock_private_key
                mock_private_key.private_bytes.return_value = b"processed_key"

                engine = SnowflakeAnalyticsEngine(self.account, self.mock_graph)
                result = engine._get_connect_args()

                assert result == {"private_key": b"processed_key"}
                # Verify private key was processed correctly
                mock_load_key.assert_called_once()
                call_args = mock_load_key.call_args
                # Split long assertion to comply with line length limits
                expected_key = (
                    b"-----BEGIN PRIVATE KEY-----\ntest_key\n-----END PRIVATE KEY-----"
                )
                assert call_args[0][0] == expected_key

    def test_get_connect_args_without_private_key(self) -> None:
        """Test _get_connect_args without private key."""
        with patch.object(SnowflakeConnection, "from_datahub") as mock_from_datahub:
            mock_connection = Mock(spec=SnowflakeConnection)
            mock_connection.authentication_type = "PASSWORD"
            mock_connection.private_key = None
            mock_from_datahub.return_value = mock_connection

            engine = SnowflakeAnalyticsEngine(self.account, self.mock_graph)
            result = engine._get_connect_args()

            assert result == {}


class TestSnowflakeAnalyticsEngineIntegration:
    """Integration tests for the Snowflake analytics engine."""

    def test_end_to_end_engine_creation(self) -> None:
        """Test end-to-end engine creation and usage."""
        mock_graph = Mock(spec=DataHubGraph)

        with patch.object(SnowflakeConnection, "from_datahub") as mock_from_datahub:
            mock_connection = Mock(spec=SnowflakeConnection)
            mock_connection.account = "test_account"
            mock_connection.warehouse = "test_warehouse"
            mock_connection.user = "test_user"
            mock_connection.password = "test_password"
            mock_connection.role = "test_role"
            mock_connection.authentication_type = "PASSWORD"
            mock_connection.private_key = None
            mock_connection.private_key_password = None
            mock_from_datahub.return_value = mock_connection

            engine = SnowflakeAnalyticsEngine("test_account", mock_graph)

            # Verify initialization
            assert engine.account == "test_account"
            assert engine.graph == mock_graph
            assert engine.connection == mock_connection
            assert engine._engine is None

            # Verify connection was created from DataHub
            mock_from_datahub.assert_called_once_with(graph=mock_graph)
