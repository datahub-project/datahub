"""Unit tests for the Snowflake analytics engine."""

from unittest.mock import Mock, patch

from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection
from datahub_integrations.analytics.snowflake.engine import SnowflakeAnalyticsEngine
from datahub_integrations.propagation.snowflake.constants import AUTH_TYPE_DEFAULT


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
            mock_connection.authentication_type = AUTH_TYPE_DEFAULT
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

    def test_get_native_connection_delegates_to_config(self) -> None:
        """Test that get_native_connection delegates to SnowflakeConnectionConfigPermissive."""
        with patch.object(SnowflakeConnection, "from_datahub") as mock_from_datahub:
            mock_connection = Mock(spec=SnowflakeConnection)
            mock_connection.user = "test_user"
            mock_connection.password = "test_password"
            mock_connection.account = "test_account"
            mock_connection.warehouse = "test_warehouse"
            mock_connection.role = "test_role"
            mock_connection.authentication_type = AUTH_TYPE_DEFAULT
            mock_connection.private_key = None
            mock_connection.private_key_password = None
            mock_from_datahub.return_value = mock_connection

            engine = SnowflakeAnalyticsEngine(self.account, self.mock_graph)

            # Mock the config's create_native_connection method
            with patch.object(
                engine._get_config(), "create_native_connection"
            ) as mock_create_conn:
                mock_native_conn = Mock()
                mock_create_conn.return_value = mock_native_conn

                result = engine.get_native_connection()

                assert result == mock_native_conn
                mock_create_conn.assert_called_once_with(
                    application="datahub_analytics"
                )

    def test_get_config_creates_and_caches_config(self) -> None:
        """Test that _get_config creates and caches SnowflakeConnectionConfigPermissive."""
        with patch.object(SnowflakeConnection, "from_datahub") as mock_from_datahub:
            mock_connection = Mock(spec=SnowflakeConnection)
            mock_connection.user = "test_user"
            mock_connection.password = "test_password"
            mock_connection.account = "test_account"
            mock_connection.warehouse = "test_warehouse"
            mock_connection.role = "test_role"
            mock_connection.authentication_type = AUTH_TYPE_DEFAULT
            mock_connection.private_key = None
            mock_connection.private_key_password = None
            mock_from_datahub.return_value = mock_connection

            engine = SnowflakeAnalyticsEngine(self.account, self.mock_graph)

            # First call should create config
            config1 = engine._get_config()
            assert config1 is not None

            # Second call should return cached config
            config2 = engine._get_config()
            assert config1 is config2


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
