"""
Comprehensive unit tests for the Snowflake analytics engine module.

Tests the SnowflakeAnalyticsEngine class and its functionality.
"""

from unittest.mock import Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from sqlalchemy.engine import Engine

from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection
from datahub_integrations.analytics.snowflake.engine import SnowflakeAnalyticsEngine
from datahub_integrations.propagation.snowflake.config import (
    SnowflakeConnectionConfigPermissive,
)


class TestSnowflakeAnalyticsEngine:
    """Test the SnowflakeAnalyticsEngine class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_graph = Mock(spec=DataHubGraph)
        self.account = "test_account"

        # Mock the SnowflakeConnection.from_datahub method
        self.mock_connection = Mock(spec=SnowflakeConnection)
        self.mock_connection.account = "test_account"
        self.mock_connection.warehouse = "test_warehouse"
        self.mock_connection.user = "test_user"
        self.mock_connection.password = "test_password"
        self.mock_connection.role = "test_role"
        self.mock_connection.authentication_type = "DEFAULT_AUTHENTICATOR"
        self.mock_connection.private_key = None
        self.mock_connection.private_key_password = None

        with patch.object(
            SnowflakeConnection, "from_datahub", return_value=self.mock_connection
        ):
            self.engine = SnowflakeAnalyticsEngine(self.account, self.mock_graph)

    def test_initialization(self):
        """Test SnowflakeAnalyticsEngine initialization."""
        assert self.engine.account == self.account
        assert self.engine.graph == self.mock_graph
        assert self.engine.connection == self.mock_connection
        assert self.engine._engine is None

    @patch("datahub_integrations.analytics.snowflake.engine.create_engine")
    def test_get_sqlalchemy_engine_lazy_loading(self, mock_create_engine):
        """Test that SQLAlchemy engine is created lazily."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        # Mock the config and its methods
        with (
            patch.object(
                SnowflakeConnectionConfigPermissive, "parse_obj"
            ) as mock_parse,
            patch.object(
                SnowflakeConnectionConfigPermissive, "get_sql_alchemy_url"
            ) as mock_get_url,
            patch.object(
                SnowflakeConnectionConfigPermissive, "get_options"
            ) as mock_get_options,
        ):
            mock_config = Mock()
            mock_parse.return_value = mock_config
            mock_get_url.return_value = "snowflake://test_url"
            mock_get_options.return_value = {"pool_size": 5}

            # First access should create engine
            engine = self.engine._get_sqlalchemy_engine()

            assert engine == mock_engine
            mock_create_engine.assert_called_once_with(
                "snowflake://test_url", pool_size=5
            )

            # Second access should return same engine
            engine2 = self.engine._get_sqlalchemy_engine()
            assert engine2 == mock_engine
            assert mock_create_engine.call_count == 1  # Should not be called again

    @patch("datahub_integrations.analytics.snowflake.engine.create_engine")
    def test_get_sqlalchemy_engine_config_conversion(self, mock_create_engine):
        """Test that connection is properly converted to config format."""
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        with patch.object(
            SnowflakeConnectionConfigPermissive, "parse_obj"
        ) as mock_parse:
            mock_config = Mock()
            mock_config.get_sql_alchemy_url.return_value = "snowflake://test_url"
            mock_config.get_options.return_value = {}
            mock_parse.return_value = mock_config

            self.engine._get_sqlalchemy_engine()

            # Verify the config dict passed to parse_obj
            call_args = mock_parse.call_args[0][0]
            expected_config = {
                "account_id": "test_account",
                "warehouse": "test_warehouse",
                "username": "test_user",
                "password": "test_password",
                "role": "test_role",
                "authentication_type": "DEFAULT_AUTHENTICATOR",
                "private_key": None,
                "private_key_password": None,
            }
            assert call_args == expected_config

    @patch("datahub_integrations.analytics.snowflake.engine.create_engine")
    def test_get_sqlalchemy_engine_with_private_key_auth(self, mock_create_engine):
        """Test SQLAlchemy engine creation with private key authentication."""
        # Update connection to use private key auth
        self.mock_connection.authentication_type = "KEY_PAIR_AUTHENTICATOR"
        self.mock_connection.private_key = "test_private_key"
        self.mock_connection.private_key_password = "test_key_password"

        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        with patch.object(
            SnowflakeConnectionConfigPermissive, "parse_obj"
        ) as mock_parse:
            mock_config = Mock()
            mock_config.get_sql_alchemy_url.return_value = "snowflake://test_url"
            mock_config.get_options.return_value = {}
            mock_parse.return_value = mock_config

            self.engine._get_sqlalchemy_engine()

            # Verify the config includes private key details
            call_args = mock_parse.call_args[0][0]
            assert call_args["authentication_type"] == "KEY_PAIR_AUTHENTICATOR"
            assert call_args["private_key"] == "test_private_key"
            assert call_args["private_key_password"] == "test_key_password"

    def test_execute_query_success(self):
        """Test successful query execution."""
        mock_engine = Mock(spec=Engine)
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [("result1",), ("result2",)]
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        with patch.object(
            self.engine, "_get_sqlalchemy_engine", return_value=mock_engine
        ):
            result = self.engine.execute_query("SELECT * FROM test_table")

            assert result == [("result1",), ("result2",)]
            mock_connection.execute.assert_called_once_with("SELECT * FROM test_table")

    def test_execute_query_exception_handling(self):
        """Test query execution exception handling."""
        mock_engine = Mock(spec=Engine)
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Database error")
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        with patch.object(
            self.engine, "_get_sqlalchemy_engine", return_value=mock_engine
        ):
            with pytest.raises(Exception, match="Database error"):
                self.engine.execute_query("SELECT * FROM test_table")

    def test_get_tables_success(self):
        """Test successful table listing."""
        mock_tables = [("table1",), ("table2",), ("table3",)]

        with patch.object(self.engine, "execute_query", return_value=mock_tables):
            result = self.engine.get_tables("test_database", "test_schema")

            assert result == ["table1", "table2", "table3"]
            # Verify the query was called with correct parameters
            self.engine.execute_query.assert_called_once()
            call_args = self.engine.execute_query.call_args[0][0]
            assert "SHOW TABLES" in call_args
            assert "test_database" in call_args
            assert "test_schema" in call_args

    def test_get_tables_empty_result(self):
        """Test table listing with empty result."""
        with patch.object(self.engine, "execute_query", return_value=[]):
            result = self.engine.get_tables("test_database", "test_schema")

            assert result == []

    def test_get_columns_success(self):
        """Test successful column listing."""
        mock_columns = [("col1", "VARCHAR"), ("col2", "INTEGER"), ("col3", "TIMESTAMP")]

        with patch.object(self.engine, "execute_query", return_value=mock_columns):
            result = self.engine.get_columns(
                "test_database", "test_schema", "test_table"
            )

            expected = [
                {"name": "col1", "type": "VARCHAR"},
                {"name": "col2", "type": "INTEGER"},
                {"name": "col3", "type": "TIMESTAMP"},
            ]
            assert result == expected

            # Verify the query was called with correct parameters
            self.engine.execute_query.assert_called_once()
            call_args = self.engine.execute_query.call_args[0][0]
            assert "DESCRIBE TABLE" in call_args
            assert "test_database" in call_args
            assert "test_schema" in call_args
            assert "test_table" in call_args

    def test_get_columns_empty_result(self):
        """Test column listing with empty result."""
        with patch.object(self.engine, "execute_query", return_value=[]):
            result = self.engine.get_columns(
                "test_database", "test_schema", "test_table"
            )

            assert result == []

    def test_test_connection_success(self):
        """Test successful connection testing."""
        with patch.object(self.engine, "execute_query", return_value=[("1",)]):
            result = self.engine.test_connection()

            assert result is True
            self.engine.execute_query.assert_called_once_with("SELECT 1")

    def test_test_connection_failure(self):
        """Test connection testing failure."""
        with patch.object(
            self.engine, "execute_query", side_effect=Exception("Connection failed")
        ):
            result = self.engine.test_connection()

            assert result is False

    def test_get_database_names_success(self):
        """Test successful database name retrieval."""
        mock_databases = [("db1",), ("db2",), ("db3",)]

        with patch.object(self.engine, "execute_query", return_value=mock_databases):
            result = self.engine.get_database_names()

            assert result == ["db1", "db2", "db3"]
            self.engine.execute_query.assert_called_once_with("SHOW DATABASES")

    def test_get_schema_names_success(self):
        """Test successful schema name retrieval."""
        mock_schemas = [("schema1",), ("schema2",), ("schema3",)]

        with patch.object(self.engine, "execute_query", return_value=mock_schemas):
            result = self.engine.get_schema_names("test_database")

            assert result == ["schema1", "schema2", "schema3"]
            # Verify the query includes the database name
            call_args = self.engine.execute_query.call_args[0][0]
            assert "SHOW SCHEMAS" in call_args
            assert "test_database" in call_args

    def test_execute_ddl_success(self):
        """Test successful DDL execution."""
        ddl_statement = "CREATE TABLE test_table (id INTEGER, name VARCHAR(50))"

        with patch.object(self.engine, "execute_query", return_value=None):
            result = self.engine.execute_ddl(ddl_statement)

            assert result is True
            self.engine.execute_query.assert_called_once_with(ddl_statement)

    def test_execute_ddl_failure(self):
        """Test DDL execution failure."""
        ddl_statement = "CREATE TABLE invalid_syntax"

        with patch.object(
            self.engine, "execute_query", side_effect=Exception("SQL syntax error")
        ):
            result = self.engine.execute_ddl(ddl_statement)

            assert result is False

    def test_get_table_row_count_success(self):
        """Test successful table row count retrieval."""
        with patch.object(self.engine, "execute_query", return_value=[(42,)]):
            result = self.engine.get_table_row_count(
                "test_database", "test_schema", "test_table"
            )

            assert result == 42
            # Verify the query structure
            call_args = self.engine.execute_query.call_args[0][0]
            assert "SELECT COUNT(*)" in call_args
            assert "test_database" in call_args
            assert "test_schema" in call_args
            assert "test_table" in call_args

    def test_get_table_row_count_failure(self):
        """Test table row count retrieval failure."""
        with patch.object(
            self.engine, "execute_query", side_effect=Exception("Table not found")
        ):
            result = self.engine.get_table_row_count(
                "test_database", "test_schema", "nonexistent_table"
            )

            assert result is None

    def test_close_engine(self):
        """Test engine cleanup."""
        mock_engine = Mock(spec=Engine)
        self.engine._engine = mock_engine

        self.engine.close()

        mock_engine.dispose.assert_called_once()

    def test_close_engine_no_engine(self):
        """Test engine cleanup when no engine exists."""
        # Should not raise any exception
        self.engine.close()

    def test_context_manager_usage(self):
        """Test using the engine as a context manager."""
        with patch.object(self.engine, "close") as mock_close:
            with self.engine as engine:
                assert engine == self.engine

            mock_close.assert_called_once()

    def test_repr(self):
        """Test string representation of the engine."""
        result = repr(self.engine)

        assert "SnowflakeAnalyticsEngine" in result
        assert "test_account" in result

    def test_str(self):
        """Test string conversion of the engine."""
        result = str(self.engine)

        assert "SnowflakeAnalyticsEngine" in result
        assert "test_account" in result


class TestSnowflakeAnalyticsEngineIntegration:
    """Integration tests for SnowflakeAnalyticsEngine."""

    def test_end_to_end_query_execution(self):
        """Test end-to-end query execution flow."""
        mock_graph = Mock(spec=DataHubGraph)

        # Mock the connection creation chain
        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.account = "test_account"
        mock_connection.warehouse = "test_warehouse"
        mock_connection.user = "test_user"
        mock_connection.password = "test_password"
        mock_connection.role = "test_role"
        mock_connection.authentication_type = "DEFAULT_AUTHENTICATOR"
        mock_connection.private_key = None
        mock_connection.private_key_password = None

        with (
            patch.object(
                SnowflakeConnection, "from_datahub", return_value=mock_connection
            ),
            patch(
                "datahub_integrations.analytics.snowflake.engine.create_engine"
            ) as mock_create_engine,
        ):
            # Mock SQLAlchemy engine and connection
            mock_sqlalchemy_engine = Mock(spec=Engine)
            mock_sqlalchemy_connection = Mock()
            mock_result = Mock()
            mock_result.fetchall.return_value = [("test_result",)]
            mock_sqlalchemy_connection.execute.return_value = mock_result
            mock_sqlalchemy_engine.connect.return_value.__enter__.return_value = (
                mock_sqlalchemy_connection
            )
            mock_create_engine.return_value = mock_sqlalchemy_engine

            # Mock config creation
            with patch.object(
                SnowflakeConnectionConfigPermissive, "parse_obj"
            ) as mock_parse:
                mock_config = Mock()
                mock_config.get_sql_alchemy_url.return_value = "snowflake://test_url"
                mock_config.get_options.return_value = {}
                mock_parse.return_value = mock_config

                # Create engine and execute query
                engine = SnowflakeAnalyticsEngine("test_account", mock_graph)
                result = engine.execute_query("SELECT 1")

                # Verify the full flow
                assert result == [("test_result",)]
                mock_sqlalchemy_connection.execute.assert_called_once_with("SELECT 1")

    def test_connection_failure_handling(self):
        """Test handling of connection failures."""
        mock_graph = Mock(spec=DataHubGraph)

        # Mock connection creation to fail
        with patch.object(
            SnowflakeConnection,
            "from_datahub",
            side_effect=Exception("Connection failed"),
        ):
            with pytest.raises(Exception, match="Connection failed"):
                SnowflakeAnalyticsEngine("test_account", mock_graph)

    def test_multiple_query_execution(self):
        """Test executing multiple queries with the same engine."""
        mock_graph = Mock(spec=DataHubGraph)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.account = "test_account"
        mock_connection.warehouse = "test_warehouse"
        mock_connection.user = "test_user"
        mock_connection.password = "test_password"
        mock_connection.role = "test_role"
        mock_connection.authentication_type = "DEFAULT_AUTHENTICATOR"
        mock_connection.private_key = None
        mock_connection.private_key_password = None

        with patch.object(
            SnowflakeConnection, "from_datahub", return_value=mock_connection
        ):
            engine = SnowflakeAnalyticsEngine("test_account", mock_graph)

            # Mock the SQLAlchemy engine to return different results
            with patch.object(
                engine,
                "execute_query",
                side_effect=[[("result1",)], [("result2",)], [("result3",)]],
            ):
                result1 = engine.execute_query("SELECT 1")
                result2 = engine.execute_query("SELECT 2")
                result3 = engine.execute_query("SELECT 3")

                assert result1 == [("result1",)]
                assert result2 == [("result2",)]
                assert result3 == [("result3",)]

                # Verify engine was reused (SQLAlchemy engine should be created
                # only once)
                assert (
                    engine._get_sqlalchemy_engine() is engine._get_sqlalchemy_engine()
                )

    def test_resource_cleanup_on_exception(self):
        """Test that resources are properly cleaned up on exceptions."""
        mock_graph = Mock(spec=DataHubGraph)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.account = "test_account"
        mock_connection.warehouse = "test_warehouse"
        mock_connection.user = "test_user"
        mock_connection.password = "test_password"
        mock_connection.role = "test_role"
        mock_connection.authentication_type = "DEFAULT_AUTHENTICATOR"
        mock_connection.private_key = None
        mock_connection.private_key_password = None

        with patch.object(
            SnowflakeConnection, "from_datahub", return_value=mock_connection
        ):
            engine = SnowflakeAnalyticsEngine("test_account", mock_graph)

            # Mock SQLAlchemy engine to raise exception during query
            mock_sqlalchemy_engine = Mock(spec=Engine)
            mock_sqlalchemy_connection = Mock()
            mock_sqlalchemy_connection.execute.side_effect = Exception("Query failed")
            mock_sqlalchemy_engine.connect.return_value.__enter__.return_value = (
                mock_sqlalchemy_connection
            )

            with patch.object(
                engine, "_get_sqlalchemy_engine", return_value=mock_sqlalchemy_engine
            ):
                with pytest.raises(Exception, match="Query failed"):
                    engine.execute_query("SELECT 1")

                # Verify connection context manager was properly used
                mock_sqlalchemy_engine.connect.assert_called_once()
                mock_sqlalchemy_engine.connect.return_value.__enter__.assert_called_once()
                mock_sqlalchemy_engine.connect.return_value.__exit__.assert_called_once()
