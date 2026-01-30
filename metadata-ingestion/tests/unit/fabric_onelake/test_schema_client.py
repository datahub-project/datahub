"""Unit tests for SQL Analytics Endpoint schema client."""

import struct
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.onelake.config import SqlEndpointConfig
from datahub.ingestion.source.fabric.onelake.schema_client import (
    SqlAnalyticsEndpointClient,
)
from datahub.ingestion.source.fabric.onelake.schema_report import (
    SqlAnalyticsEndpointReport,
)


@pytest.fixture
def mock_auth_helper():
    """Create a mock authentication helper."""
    helper = MagicMock(spec=FabricAuthHelper)
    helper.get_bearer_token.return_value = "mock-token-12345"
    return helper


@pytest.fixture
def sql_endpoint_config():
    """Create a SQL endpoint configuration."""
    return SqlEndpointConfig(
        enabled=True,
        query_timeout=30,
    )


@pytest.fixture
def mock_report():
    """Create a mock report."""
    return MagicMock(spec=SqlAnalyticsEndpointReport)


class TestSqlAnalyticsEndpointClient:
    """Test cases for SqlAnalyticsEndpointClient."""

    def test_init(self, mock_auth_helper, sql_endpoint_config, mock_report):
        """Test client initialization."""
        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            report=mock_report,
            item_display_name="TestLakehouse",
        )
        assert client.auth_helper == mock_auth_helper
        assert client.config == sql_endpoint_config
        assert client.report == mock_report
        assert client._engines == {}

    def test_get_connection_string_default(self, mock_auth_helper, sql_endpoint_config):
        """Test default connection string generation (uses display name for Database=)."""
        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            item_display_name="TestLakehouse",
        )
        connection_string = client._get_connection_string(
            workspace_id="ws-123",
            item_id="item-456",
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
        )
        assert "TestLakehouse" in connection_string
        assert "ODBC Driver 18 for SQL Server" in connection_string
        assert "test-endpoint.datawarehouse.fabric.microsoft.com" in connection_string
        assert "Encrypt=yes" in connection_string
        assert "TrustServerCertificate=no" in connection_string

    def test_get_connection_string_custom_options(
        self, mock_auth_helper, sql_endpoint_config
    ):
        """Test custom connection options."""
        sql_endpoint_config.odbc_driver = "Custom Driver"
        sql_endpoint_config.encrypt = "no"
        sql_endpoint_config.trust_server_certificate = "yes"
        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            item_display_name="TestLakehouse",
        )
        connection_string = client._get_connection_string(
            workspace_id="ws-123",
            item_id="item-456",
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
        )
        assert "TestLakehouse" in connection_string
        assert "Custom Driver" in connection_string
        assert "Encrypt=no" in connection_string
        assert "TrustServerCertificate=yes" in connection_string

    @patch("datahub.ingestion.source.fabric.onelake.schema_client.create_engine")
    def test_create_engine_token_injection(
        self, mock_create_engine, mock_auth_helper, sql_endpoint_config
    ):
        """Test that token is injected into SQLAlchemy engine."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            item_display_name="TestLakehouse",
        )

        client._create_engine(
            workspace_id="ws-123",
            item_id="item-456",
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
        )

        # Verify engine was created with correct connection string
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args
        assert "mssql+pyodbc" in call_args[0][0]

        # Verify creator parameter is provided (for token injection)
        assert "creator" in call_args[1] or call_args.kwargs.get("creator")
        # Verify pool settings
        assert call_args[1].get("pool_pre_ping") is True
        assert call_args[1].get("pool_recycle") == 3600

    @patch("datahub.ingestion.source.fabric.onelake.schema_client.create_engine")
    def test_get_engine_caching(
        self, mock_create_engine, mock_auth_helper, sql_endpoint_config
    ):
        """Test that engines are cached per workspace/item combination."""
        # Create different mock engines for different calls
        mock_engine1 = MagicMock()
        mock_engine2 = MagicMock()
        mock_create_engine.side_effect = [mock_engine1, mock_engine2]

        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            item_display_name="TestLakehouse",
        )

        engine1 = client._get_engine(
            workspace_id="ws-123",
            item_id="item-456",
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
        )
        engine2 = client._get_engine(
            workspace_id="ws-123",
            item_id="item-456",
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
        )
        engine3 = client._get_engine(
            workspace_id="ws-789",
            item_id="item-456",
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
        )

        # Same workspace/item should return same engine
        assert engine1 is engine2
        # Different workspace should return different engine
        assert engine1 is not engine3
        # Should only create 2 engines
        assert mock_create_engine.call_count == 2

    @patch("datahub.ingestion.source.fabric.onelake.schema_client.create_engine")
    def test_get_table_columns_success(
        self, mock_create_engine, mock_auth_helper, sql_endpoint_config, mock_report
    ):
        """Test successful column extraction."""
        # Mock engine and connection
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()

        # Mock query result rows
        mock_row1 = MagicMock()
        mock_row1.COLUMN_NAME = "id"
        mock_row1.DATA_TYPE = "int"
        mock_row1.IS_NULLABLE = "NO"
        mock_row1.ORDINAL_POSITION = 1

        mock_row2 = MagicMock()
        mock_row2.COLUMN_NAME = "name"
        mock_row2.DATA_TYPE = "varchar"
        mock_row2.IS_NULLABLE = "YES"
        mock_row2.ORDINAL_POSITION = 2

        mock_result.__iter__ = Mock(return_value=iter([mock_row1, mock_row2]))
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            report=mock_report,
            item_display_name="TestLakehouse",
        )

        columns = client.get_table_columns(
            workspace_id="ws-123",
            item_id="item-456",
            schema_name="dbo",
            table_name="test_table",
        )

        assert len(columns) == 2
        assert columns[0].name == "id"
        assert columns[0].data_type == "int"
        assert columns[0].is_nullable is False
        assert columns[0].ordinal_position == 1

        assert columns[1].name == "name"
        assert columns[1].data_type == "varchar"
        assert columns[1].is_nullable is True
        assert columns[1].ordinal_position == 2

    @patch("datahub.ingestion.source.fabric.onelake.schema_client.create_engine")
    def test_get_table_columns_error_handling(
        self, mock_create_engine, mock_auth_helper, sql_endpoint_config, mock_report
    ):
        """Test error handling in column extraction."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = SQLAlchemyError("Connection failed")
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        # Set up mock_report.failures as an integer attribute
        mock_report.failures = 0

        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            report=mock_report,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            item_display_name="TestLakehouse",
        )

        columns = client.get_table_columns(
            workspace_id="ws-123",
            item_id="item-456",
            schema_name="dbo",
            table_name="test_table",
        )

        # Should return empty list on error
        assert columns == []
        # Should report failure
        assert mock_report.failures > 0

    @patch("datahub.ingestion.source.fabric.onelake.schema_client.create_engine")
    def test_get_all_table_columns(
        self, mock_create_engine, mock_auth_helper, sql_endpoint_config, mock_report
    ):
        """Test fetching all table columns for an item."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()

        # Mock query result rows for multiple tables
        mock_row1 = MagicMock()
        mock_row1.TABLE_SCHEMA = "dbo"
        mock_row1.TABLE_NAME = "table1"
        mock_row1.COLUMN_NAME = "id"
        mock_row1.DATA_TYPE = "INT"
        mock_row1.IS_NULLABLE = "NO"
        mock_row1.ORDINAL_POSITION = 1

        mock_row2 = MagicMock()
        mock_row2.TABLE_SCHEMA = "dbo"
        mock_row2.TABLE_NAME = "table2"
        mock_row2.COLUMN_NAME = "name"
        mock_row2.DATA_TYPE = "VARCHAR"
        mock_row2.IS_NULLABLE = "YES"
        mock_row2.ORDINAL_POSITION = 1

        mock_result.__iter__ = Mock(return_value=iter([mock_row1, mock_row2]))
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            report=mock_report,
            item_display_name="TestLakehouse",
        )

        columns_by_table = client.get_all_table_columns(
            workspace_id="ws-123",
            item_id="item-456",
        )

        assert len(columns_by_table) == 2
        assert ("dbo", "table1") in columns_by_table
        assert ("dbo", "table2") in columns_by_table
        assert len(columns_by_table[("dbo", "table1")]) == 1
        assert len(columns_by_table[("dbo", "table2")]) == 1
        # Verify uppercase types are preserved
        assert columns_by_table[("dbo", "table1")][0].data_type == "INT"
        assert columns_by_table[("dbo", "table2")][0].data_type == "VARCHAR"

    def test_close(self, mock_auth_helper, sql_endpoint_config):
        """Test closing all engine connections."""
        client = SqlAnalyticsEndpointClient(
            auth_helper=mock_auth_helper,
            config=sql_endpoint_config,
            endpoint_url="test-endpoint.datawarehouse.fabric.microsoft.com",
            item_display_name="TestLakehouse",
        )

        # Create mock engines
        mock_engine1 = MagicMock()
        mock_engine2 = MagicMock()
        endpoint_url = "test-endpoint.datawarehouse.fabric.microsoft.com"
        client._engines = {
            ("ws-1", "item-1", endpoint_url): mock_engine1,
            ("ws-2", "item-2", endpoint_url): mock_engine2,
        }

        client.close()

        # Verify all engines were disposed
        mock_engine1.dispose.assert_called_once()
        mock_engine2.dispose.assert_called_once()
        assert client._engines == {}

    def test_token_struct_encoding(self):
        """Test that token is properly encoded for pyodbc."""
        token = "test-token-12345"
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(
            f"<I{len(token_bytes)}s", len(token_bytes), token_bytes
        )

        # Verify struct format: 4-byte length prefix + token bytes
        assert len(token_struct) == 4 + len(token_bytes)
        # Verify length prefix
        length = struct.unpack("<I", token_struct[:4])[0]
        assert length == len(token_bytes)
