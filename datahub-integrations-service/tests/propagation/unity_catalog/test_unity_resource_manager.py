"""
Comprehensive tests for Unity Catalog Tag Management Script
Tests cover functionality, error handling, SQL injection protection, and connection management.
"""

from typing import Dict, List, Optional, Union
from unittest.mock import MagicMock, call, patch

import pytest

from datahub_integrations.propagation.unity_catalog.unity_resource_manager import (
    UnityResourceManager,
)

# Assuming the class is imported from the main module
# from unity_catalog_manager import UnityResourceManager


class TestUnityResourceManager:
    """Test suite for UnityResourceManager class."""

    @pytest.fixture
    def mock_connection(self) -> MagicMock:
        """Create a mock database connection."""
        conn = MagicMock()
        cursor = MagicMock()
        # Mock the context manager behavior for cursor
        cursor_context_manager = MagicMock()
        cursor_context_manager.__enter__.return_value = cursor
        cursor_context_manager.__exit__.return_value = None
        conn.cursor.return_value = cursor_context_manager
        # Mock the context manager behavior for connection
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = None
        return conn

    @pytest.fixture
    def mock_connection_params(self) -> Dict[str, str]:
        """Standard connection parameters for testing."""
        return {
            "server_hostname": "test.databricks.com",
            "http_path": "/sql/protocolv1/o/123/456",
            "access_token": "test_token",
        }

    @pytest.fixture
    def manager_with_params(
        self, mock_connection_params: Dict[str, str]
    ) -> UnityResourceManager:
        """Create manager with connection parameters."""
        return UnityResourceManager(connection_params=mock_connection_params)

    @pytest.fixture
    def manager_with_conn(self, mock_connection: MagicMock) -> UnityResourceManager:
        """Create manager with pre-configured connection."""
        return UnityResourceManager(conn=mock_connection)

    def test_init_with_connection_string(self) -> None:
        """Test initialization with connection string."""
        conn_str = "databricks://token:test_token@test.databricks.com:443/default?http_path=/sql/protocolv1/o/123/456"
        manager = UnityResourceManager(connection_string=conn_str)

        assert manager.connection_string == conn_str
        assert manager.connection_params is not None
        assert manager.connection_params["server_hostname"] == "test.databricks.com"
        assert manager.connection_params["access_token"] == "test_token"

    def test_init_with_connection_params(
        self, mock_connection_params: Dict[str, str]
    ) -> None:
        """Test initialization with connection parameters."""
        manager = UnityResourceManager(connection_params=mock_connection_params)

        assert manager.connection_params == mock_connection_params
        assert manager.conn is None

    def test_init_with_existing_connection(self, mock_connection: MagicMock) -> None:
        """Test initialization with existing connection."""
        manager = UnityResourceManager(conn=mock_connection)

        assert manager.conn == mock_connection

    def test_init_without_params_raises_error(self) -> None:
        """Test that initialization without parameters raises ValueError."""
        with pytest.raises(
            ValueError,
            match="Either connection_string, conn, or connection_params must be provided",
        ):
            UnityResourceManager()

    def test_test_connection_success(
        self, manager_with_conn: UnityResourceManager, mock_connection: MagicMock
    ) -> None:
        """Test successful connection test."""
        cursor = MagicMock()
        mock_connection.cursor.return_value = cursor
        cursor.__enter__.return_value = cursor
        cursor.__exit__.return_value = None

        result = manager_with_conn._test_connection(mock_connection)

        assert result is True
        cursor.execute.assert_called_once_with("SELECT 1")
        cursor.fetchone.assert_called_once()

    def test_test_connection_failure(
        self, manager_with_conn: UnityResourceManager, mock_connection: MagicMock
    ) -> None:
        """Test connection test failure."""
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Connection failed")
        mock_connection.cursor.return_value = cursor
        cursor.__enter__.return_value = cursor
        cursor.__exit__.return_value = None

        result = manager_with_conn._test_connection(mock_connection)

        assert result is False

    @patch(
        "datahub_integrations.propagation.unity_catalog.unity_resource_manager.connect"
    )
    def test_execute_sql_success(
        self, mock_connect: MagicMock, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful SQL execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [("val1", "val2")]

        # Mock context managers
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.__exit__.return_value = None
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        result = manager_with_params._execute_sql("SELECT * FROM test")

        expected = [{"col1": "val1", "col2": "val2"}]
        assert result == expected
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test")

    @patch(
        "datahub_integrations.propagation.unity_catalog.unity_resource_manager.connect"
    )
    def test_execute_sql_failure(
        self, mock_connect: MagicMock, manager_with_params: UnityResourceManager
    ) -> None:
        """Test SQL execution failure."""
        mock_connect.side_effect = ValueError("SQL Error")

        with pytest.raises(ValueError):
            manager_with_params._execute_sql("INVALID SQL")

    @patch(
        "datahub_integrations.propagation.unity_catalog.unity_resource_manager.connect"
    )
    def test_execute_multiple_sql_success(
        self, mock_connect: MagicMock, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful multiple SQL execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()

        # Mock context managers
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.__exit__.return_value = None
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        sql_commands: List[str] = ["CREATE TABLE test1", "CREATE TABLE test2"]

        result = manager_with_params._execute_multiple_sql(sql_commands)

        assert result is True
        assert mock_cursor.execute.call_count == 2
        mock_cursor.execute.assert_has_calls(
            [call("CREATE TABLE test1"), call("CREATE TABLE test2")]
        )

    def test_add_table_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful table tag addition."""
        tags: Dict[str, str] = {"env": "prod", "team": "data"}

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.add_table_tags(
                "catalog1", "schema1", "table1", tags
            )

        assert result is True
        expected_sql = "ALTER TABLE `catalog1`.`schema1`.`table1` SET TAGS ('env' = 'prod', 'team' = 'data')"
        mock_exec.assert_called_once_with(expected_sql)

    def test_add_table_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table tag addition failure."""
        tags: Dict[str, str] = {"env": "prod"}

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result = manager_with_params.add_table_tags(
                "catalog1", "schema1", "table1", tags
            )

        assert result is False

    def test_remove_table_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful table tag removal."""
        tag_keys: List[str] = ["env", "team"]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.remove_table_tags(
                "catalog1", "schema1", "table1", tag_keys
            )

        assert result is True
        expected_sql = (
            "ALTER TABLE `catalog1`.`schema1`.`table1` UNSET TAGS ('env', 'team')"
        )
        mock_exec.assert_called_once_with(expected_sql)

    def test_update_table_tags_merge_mode(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table tag update in merge mode."""
        tags: Dict[str, str] = {"new_tag": "new_value"}

        with patch.object(
            manager_with_params, "_execute_multiple_sql", return_value=True
        ) as mock_exec:
            result = manager_with_params.update_table_tags(
                "catalog1", "schema1", "table1", tags
            )

        assert result is True
        expected_sql: List[str] = [
            "ALTER TABLE `catalog1`.`schema1`.`table1` SET TAGS ('new_tag' = 'new_value')"
        ]
        mock_exec.assert_called_once_with(expected_sql)

    def test_update_table_tags_replace_mode(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table tag update in replace mode."""
        existing_tags: Dict[str, str] = {"old_tag": "old_value"}
        new_tags: Dict[str, str] = {"new_tag": "new_value"}

        with patch.object(
            manager_with_params, "get_table_tags", return_value=existing_tags
        ):
            with patch.object(
                manager_with_params, "_execute_multiple_sql", return_value=True
            ) as mock_exec:
                result = manager_with_params.update_table_tags(
                    "cat", "sch", "tbl", new_tags, replace_all=True
                )

        assert result is True
        expected_sql: List[str] = [
            "ALTER TABLE `cat`.`sch`.`tbl` UNSET TAGS ('old_tag')",
            "ALTER TABLE `cat`.`sch`.`tbl` SET TAGS ('new_tag' = 'new_value')",
        ]
        mock_exec.assert_called_once_with(expected_sql)

    def test_add_column_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful column tag addition."""
        tags: Dict[str, str] = {"pii": "true", "sensitive": "high"}

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.add_column_tags(
                "cat", "sch", "tbl", "col", tags
            )

        assert result is True
        expected_sql = "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `col` SET TAGS ('pii' = 'true', 'sensitive' = 'high')"
        mock_exec.assert_called_once_with(expected_sql)

    def test_remove_column_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful column tag removal."""
        tag_keys: List[str] = ["pii", "sensitive"]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.remove_column_tags(
                "cat", "sch", "tbl", "col", tag_keys
            )

        assert result is True
        expected_sql = "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `col` UNSET TAGS ('pii', 'sensitive')"
        mock_exec.assert_called_once_with(expected_sql)

    def test_get_table_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful table tag retrieval."""
        query_result: List[Dict[str, str]] = [
            {"tag_name": "env", "tag_value": "prod"},
            {"tag_name": "team", "tag_value": "data"},
        ]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result = manager_with_params.get_table_tags("cat", "sch", "tbl")

        expected: Dict[str, str] = {"env": "prod", "team": "data"}
        assert result == expected

    def test_get_table_tags_empty_result(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table tag retrieval with no tags."""
        with patch.object(manager_with_params, "_execute_sql", return_value=[]):
            result = manager_with_params.get_table_tags("cat", "sch", "tbl")

        assert result == {}

    def test_get_table_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table tag retrieval failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result = manager_with_params.get_table_tags("cat", "sch", "tbl")

        assert result is None

    def test_get_column_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful column tag retrieval."""
        query_result: List[Dict[str, str]] = [
            {"tag_name": "pii", "tag_value": "true"},
            {"tag_name": "format", "tag_value": "email"},
        ]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result = manager_with_params.get_column_tags("cat", "sch", "tbl", "col")

        expected: Dict[str, str] = {"pii": "true", "format": "email"}
        assert result == expected

    def test_set_table_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful table description setting."""
        description: str = "This is a test table"

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.set_table_description(
                "cat", "sch", "tbl", description
            )

        assert result is True
        expected_sql = "ALTER TABLE `cat`.`sch`.`tbl` SET TBLPROPERTIES ('comment' = 'This is a test table')"
        mock_exec.assert_called_once_with(expected_sql)

    def test_set_table_description_with_quotes(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table description setting with quotes (potential SQL injection)."""
        description: str = "Table with 'single quotes' and \"double quotes\""

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.set_table_description(
                "cat", "sch", "tbl", description
            )

        assert result is True
        # Check that single quotes are escaped
        call_args = mock_exec.call_args[0][0]
        assert "\\'" in call_args

    def test_get_table_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful table description retrieval."""
        query_result: List[Dict[str, Optional[str]]] = [
            {"comment": "This is a test table"}
        ]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result = manager_with_params.get_table_description("cat", "sch", "tbl")

        assert result == "This is a test table"

    def test_get_table_description_no_comment(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table description retrieval with no comment."""
        query_result: List[Dict[str, Optional[str]]] = [{"comment": None}]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result = manager_with_params.get_table_description("cat", "sch", "tbl")

        assert result is None

    def test_set_column_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful column description setting."""
        description: str = "User email address"

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.set_column_description(
                "cat", "sch", "tbl", "email", description
            )

        assert result is True
        expected_sql = "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `email` COMMENT 'User email address'"
        mock_exec.assert_called_once_with(expected_sql)

    def test_add_catalog_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful catalog tag addition."""
        tags: Dict[str, str] = {"env": "production", "cost_center": "engineering"}

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.add_catalog_tags("test_catalog", tags)

        assert result is True
        expected_sql = "ALTER CATALOG `test_catalog` SET TAGS ('env' = 'production', 'cost_center' = 'engineering')"
        mock_exec.assert_called_once_with(expected_sql)

    def test_remove_catalog_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful catalog tag removal."""
        tag_keys: List[str] = ["env", "cost_center"]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.remove_catalog_tags("test_catalog", tag_keys)

        assert result is True
        expected_sql = "ALTER CATALOG `test_catalog` UNSET TAGS ('env', 'cost_center')"
        mock_exec.assert_called_once_with(expected_sql)

    def test_add_schema_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful schema tag addition."""
        tags: Dict[str, str] = {"owner": "data_team", "purpose": "analytics"}

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result = manager_with_params.add_schema_tags("catalog", "schema", tags)

        assert result is True
        expected_sql = "ALTER SCHEMA `catalog`.`schema` SET TAGS ('owner' = 'data_team', 'purpose' = 'analytics')"
        mock_exec.assert_called_once_with(expected_sql)

    def test_close_persistent_connection(
        self, manager_with_conn: UnityResourceManager, mock_connection: MagicMock
    ) -> None:
        """Test closing persistent connection."""
        manager_with_conn.conn = mock_connection

        manager_with_conn.close_persistent_connection()

        mock_connection.close.assert_called_once()
        assert manager_with_conn.conn is None

    def test_close_persistent_connection_with_error(
        self, manager_with_conn: UnityResourceManager, mock_connection: MagicMock
    ) -> None:
        """Test closing persistent connection with error."""
        mock_connection.close.side_effect = Exception("Close error")
        manager_with_conn.conn = mock_connection

        # Should not raise exception
        manager_with_conn.close_persistent_connection()

        assert manager_with_conn.conn is None

    def test_list_all_table_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful listing of all table tags."""
        table_query_result: List[Dict[str, str]] = [
            {
                "table_catalog": "cat1",
                "table_schema": "sch1",
                "table_name": "tbl1",
                "table_type": "TABLE",
                "owner": "user1",
                "comment": "Test table",
            }
        ]

        table_tags: Dict[str, str] = {"env": "prod", "team": "data"}

        with patch.object(
            manager_with_params, "_execute_sql", return_value=table_query_result
        ):
            with patch.object(
                manager_with_params, "get_table_tags", return_value=table_tags
            ):
                result = manager_with_params.list_all_table_tags("cat1", "sch1")

        expected: List[Dict[str, Union[str, Dict[str, str]]]] = [
            {
                "table_catalog": "cat1",
                "table_schema": "sch1",
                "table_name": "tbl1",
                "table_type": "TABLE",
                "owner": "user1",
                "comment": "Test table",
                "tags": {"env": "prod", "team": "data"},
            }
        ]
        assert result == expected

    # Test for the bug in get_schema_description method signature
    def test_get_schema_description_wrong_signature(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test that get_schema_description has incorrect method signature."""
        # This test demonstrates the bug - the method takes 'table' parameter but doesn't use it
        query_result: List[Dict[str, str]] = [{"comment": "Test schema comment"}]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            # The method signature is wrong - it shouldn't need 'table' parameter
            result = manager_with_params.get_schema_description(
                "cat", "sch", "unused_table_param"
            )

        assert result == "Test schema comment"

    # Test for inconsistent quoting in update_column_tags
    def test_update_column_tags_inconsistent_quoting(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test the inconsistent quoting bug in update_column_tags."""
        tags: Dict[str, str] = {"tag": "value"}

        with patch.object(
            manager_with_params, "_execute_multiple_sql", return_value=True
        ) as mock_exec:
            result = manager_with_params.update_column_tags(
                "cat", "sch", "tbl", "col", tags
            )

        assert result is True
        # Check that it uses double quotes instead of backticks (the bug)
        call_args = str(mock_exec.call_args[0][0])
        assert '"cat"."sch"."tbl"' in call_args

    # SQL Injection Tests
    def test_sql_injection_in_table_tags(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test potential SQL injection in table tag operations."""
        malicious_catalog: str = "cat'; DROP TABLE users; --"
        tags: Dict[str, str] = {"test": "value"}

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            manager_with_params.add_table_tags(malicious_catalog, "sch", "tbl", tags)

        # The current implementation is vulnerable - it directly interpolates the catalog name
        call_args = mock_exec.call_args[0][0]
        assert "DROP TABLE" in call_args  # This demonstrates the vulnerability

    def test_sql_injection_in_tag_values(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test potential SQL injection in tag values."""
        malicious_tags: Dict[str, str] = {"test": "value'; DROP TABLE users; --"}

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            manager_with_params.add_table_tags("cat", "sch", "tbl", malicious_tags)

        call_args = mock_exec.call_args[0][0]
        assert "DROP TABLE" in call_args  # This demonstrates the vulnerability

    # Edge Cases
    def test_empty_tags_dict(self, manager_with_params: UnityResourceManager) -> None:
        """Test handling of empty tags dictionary."""
        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ):
            result = manager_with_params.add_table_tags("cat", "sch", "tbl", {})

        # Should handle empty tags gracefully
        assert result is True

    def test_special_characters_in_names(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test handling of special characters in catalog/schema/table names."""
        special_names: Dict[str, str] = {
            "catalog": "cat-with-dashes",
            "schema": "schema_with_underscores",
            "table": "table.with.dots",
        }

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            manager_with_params.add_table_tags(
                special_names["catalog"],
                special_names["schema"],
                special_names["table"],
                {"test": "value"},
            )

        call_args = mock_exec.call_args[0][0]
        # Should properly quote identifiers
        assert "`cat-with-dashes`" in call_args
        assert "`schema_with_underscores`" in call_args
        assert "`table.with.dots`" in call_args

    @patch(
        "datahub_integrations.propagation.unity_catalog.unity_resource_manager.connect"
    )
    def test_connection_context_manager_usage(self, mock_connect: MagicMock) -> None:
        """Test that connections are properly used as context managers."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",)]
        mock_cursor.fetchall.return_value = [("value1",)]

        # Mock context managers
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.__exit__.return_value = None
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        manager = UnityResourceManager(
            connection_params={
                "server_hostname": "test.com",
                "http_path": "/path",
                "access_token": "token",
            }
        )

        result = manager._execute_sql("SELECT 1")

        # Verify both connection and cursor context managers were used
        mock_connection.__enter__.assert_called()
        mock_connection.__exit__.assert_called()
        mock_cursor.__enter__.assert_called()
        mock_cursor.__exit__.assert_called()

        assert result == [{"col1": "value1"}]

    def test_set_catalog_description_with_quotes(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test catalog description setting with quotes (SQL injection protection)."""
        description: str = "Catalog with 'single quotes' and special chars"

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.set_catalog_description(
                "test_catalog", description
            )

        assert result is True
        # Check that single quotes are escaped
        call_args: str = mock_exec.call_args[0][0]
        assert "\\'" in call_args

    def test_set_catalog_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test catalog description setting failure."""
        description: str = "Test description"

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.set_catalog_description(
                "test_catalog", description
            )

        assert result is False

    def test_remove_catalog_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful catalog description removal."""
        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.remove_catalog_description(
                "test_catalog"
            )

        assert result is True
        expected_sql = "COMMENT ON CATALOG test_catalog IS ''"
        mock_exec.assert_called_once_with(expected_sql)

    def test_remove_catalog_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test catalog description removal failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.remove_catalog_description(
                "test_catalog"
            )

        assert result is False

    def test_get_catalog_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful catalog description retrieval."""
        query_result: List[Dict[str, Optional[str]]] = [
            {"comment": "Test catalog description"}
        ]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result: Optional[str] = manager_with_params.get_catalog_description(
                "test_catalog"
            )

        assert result == "Test catalog description"

    def test_get_catalog_description_no_comment(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test catalog description retrieval with no comment."""
        query_result: List[Dict[str, Optional[str]]] = [{"comment": None}]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result: Optional[str] = manager_with_params.get_catalog_description(
                "test_catalog"
            )

        assert result is None

    def test_get_catalog_description_empty_result(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test catalog description retrieval with empty result."""
        with patch.object(manager_with_params, "_execute_sql", return_value=[]):
            result: Optional[str] = manager_with_params.get_catalog_description(
                "test_catalog"
            )

        assert result is None

    def test_get_catalog_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test catalog description retrieval failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: Optional[str] = manager_with_params.get_catalog_description(
                "test_catalog"
            )

        assert result is None

    # Tests for schema description methods
    def test_set_schema_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful schema description setting."""
        description: str = "This is a test schema for user data"

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.set_schema_description(
                "test_catalog", "test_schema", description
            )

        assert result is True
        expected_sql = "COMMENT ON SCHEMA `test_catalog`.`test_schema` is 'This is a test schema for user data'"
        mock_exec.assert_called_once_with(expected_sql)

    def test_set_schema_description_with_quotes(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test schema description setting with quotes."""
        description: str = "Schema with 'quotes' and special characters"

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.set_schema_description(
                "catalog", "schema", description
            )

        assert result is True
        call_args: str = mock_exec.call_args[0][0]
        assert "\\'" in call_args

    def test_set_schema_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test schema description setting failure."""
        description: str = "Test description"

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.set_schema_description(
                "catalog", "schema", description
            )

        assert result is False

    def test_remove_schema_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful schema description removal."""
        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.remove_schema_description(
                "catalog", "schema"
            )

        assert result is True
        expected_sql = "COMMENT ON SCHEMA `catalog`.`schema` IS ''"
        mock_exec.assert_called_once_with(expected_sql)

    def test_remove_schema_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test schema description removal failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.remove_schema_description(
                "catalog", "schema"
            )

        assert result is False

    def test_get_schema_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful schema description retrieval."""
        query_result: List[Dict[str, Optional[str]]] = [
            {"comment": "Test schema description"}
        ]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            # Note: This method has a bug - it takes a 'table' parameter that's not used
            result: Optional[str] = manager_with_params.get_schema_description(
                "catalog", "schema", "unused_table_param"
            )

        assert result == "Test schema description"

    def test_get_schema_description_no_comment(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test schema description retrieval with no comment."""
        query_result: List[Dict[str, Optional[str]]] = [{"comment": None}]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result: Optional[str] = manager_with_params.get_schema_description(
                "catalog", "schema", "unused"
            )

        assert result is None

    def test_get_schema_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test schema description retrieval failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: Optional[str] = manager_with_params.get_schema_description(
                "catalog", "schema", "unused"
            )

        assert result is None

    # Tests for table description methods (additional edge cases)
    def test_remove_table_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful table description removal."""
        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.remove_table_description(
                "catalog", "schema", "table"
            )

        assert result is True
        expected_sql = (
            "ALTER TABLE `catalog`.`schema`.`table` SET TBLPROPERTIES ('comment' = '')"
        )
        mock_exec.assert_called_once_with(expected_sql)

    def test_remove_table_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table description removal failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.remove_table_description(
                "catalog", "schema", "table"
            )

        assert result is False

    def test_get_table_description_empty_result(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table description retrieval with empty result."""
        with patch.object(manager_with_params, "_execute_sql", return_value=[]):
            result: Optional[str] = manager_with_params.get_table_description(
                "catalog", "schema", "table"
            )

        assert result is None

    def test_get_table_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table description retrieval failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: Optional[str] = manager_with_params.get_table_description(
                "catalog", "schema", "table"
            )

        assert result is None

    # Tests for column description methods (additional edge cases)
    def test_set_column_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column description setting failure."""
        description: str = "Test column description"

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.set_column_description(
                "catalog", "schema", "table", "column", description
            )

        assert result is False

    def test_set_column_description_with_quotes(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column description setting with quotes."""
        description: str = "Column with 'quotes' and special chars"

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.set_column_description(
                "cat", "sch", "tbl", "col", description
            )

        assert result is True
        call_args: str = mock_exec.call_args[0][0]
        assert "\\'" in call_args

    def test_remove_column_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful column description removal."""
        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.remove_column_description(
                "catalog", "schema", "table", "column"
            )

        assert result is True
        expected_sql = (
            "ALTER TABLE `catalog`.`schema`.`table` ALTER COLUMN `column` COMMENT ''"
        )
        mock_exec.assert_called_once_with(expected_sql)

    def test_remove_column_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column description removal failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.remove_column_description(
                "catalog", "schema", "table", "column"
            )

        assert result is False

    def test_get_column_description_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful column description retrieval."""
        query_result: List[Dict[str, Optional[str]]] = [
            {"comment": "User's email address"}
        ]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result: Optional[str] = manager_with_params.get_column_description(
                "catalog", "schema", "table", "email"
            )

        assert result == "User's email address"

    def test_get_column_description_no_comment(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column description retrieval with no comment."""
        query_result: List[Dict[str, Optional[str]]] = [{"comment": None}]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=query_result
        ):
            result: Optional[str] = manager_with_params.get_column_description(
                "catalog", "schema", "table", "column"
            )

        assert result is None

    def test_get_column_description_empty_result(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column description retrieval with empty result."""
        with patch.object(manager_with_params, "_execute_sql", return_value=[]):
            result: Optional[str] = manager_with_params.get_column_description(
                "catalog", "schema", "table", "column"
            )

        assert result is None

    def test_get_column_description_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column description retrieval failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: Optional[str] = manager_with_params.get_column_description(
                "catalog", "schema", "table", "column"
            )

        assert result is None

    # Tests for column tag methods (additional edge cases not covered)
    def test_add_column_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column tag addition failure."""
        tags: Dict[str, str] = {"pii": "true"}

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.add_column_tags(
                "catalog", "schema", "table", "column", tags
            )

        assert result is False

    def test_remove_column_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column tag removal failure."""
        tag_keys: List[str] = ["pii", "sensitive"]

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.remove_column_tags(
                "catalog", "schema", "table", "column", tag_keys
            )

        assert result is False

    def test_update_column_tags_success_merge_mode(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful column tag update in merge mode."""
        tags: Dict[str, str] = {"new_tag": "new_value"}

        with patch.object(
            manager_with_params, "_execute_multiple_sql", return_value=True
        ) as mock_exec:
            result: bool = manager_with_params.update_column_tags(
                "cat", "sch", "tbl", "col", tags
            )

        assert result is True
        expected_sql: List[str] = [
            'ALTER TABLE "cat"."sch"."tbl" ALTER COLUMN `col` SET TAGS (\'new_tag\' = \'new_value\')'
        ]
        mock_exec.assert_called_once_with(expected_sql)

    def test_update_column_tags_replace_mode(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column tag update in replace mode."""
        existing_tags: Dict[str, str] = {"old_tag": "old_value"}
        new_tags: Dict[str, str] = {"new_tag": "new_value"}

        with patch.object(
            manager_with_params, "get_column_tags", return_value=existing_tags
        ):
            with patch.object(
                manager_with_params, "_execute_multiple_sql", return_value=True
            ) as mock_exec:
                result: bool = manager_with_params.update_column_tags(
                    "cat", "sch", "tbl", "col", new_tags, replace_all=True
                )

        assert result is True
        # Check that both commands were included
        actual_sql: List[str] = mock_exec.call_args[0][0]
        assert len(actual_sql) == 2
        assert "UNSET TAGS" in actual_sql[0]
        assert "SET TAGS" in actual_sql[1]

    def test_update_column_tags_no_operations_needed(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column tag update when no operations are needed."""
        with patch.object(manager_with_params, "get_column_tags", return_value={}):
            result: bool = manager_with_params.update_column_tags(
                "cat", "sch", "tbl", "col", {}, replace_all=True
            )

        assert result is True

    def test_update_column_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column tag update failure."""
        tags: Dict[str, str] = {"tag": "value"}

        with patch.object(
            manager_with_params,
            "_execute_multiple_sql",
            side_effect=Exception("SQL Error"),
        ):
            result: bool = manager_with_params.update_column_tags(
                "cat", "sch", "tbl", "col", tags
            )

        assert result is False

    def test_get_column_tags_empty_result(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column tag retrieval with empty result."""
        with patch.object(manager_with_params, "_execute_sql", return_value=[]):
            result: Optional[Dict[str, str]] = manager_with_params.get_column_tags(
                "catalog", "schema", "table", "column"
            )

        assert result == {}

    def test_get_column_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test column tag retrieval failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: Optional[Dict[str, str]] = manager_with_params.get_column_tags(
                "catalog", "schema", "table", "column"
            )

        assert result is None

    # Tests for catalog and schema tag methods (additional edge cases)
    def test_add_catalog_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test catalog tag addition failure."""
        tags: Dict[str, str] = {"env": "prod"}

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.add_catalog_tags("catalog", tags)

        assert result is False

    def test_remove_catalog_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test catalog tag removal failure."""
        tag_keys: List[str] = ["env", "team"]

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.remove_catalog_tags("catalog", tag_keys)

        assert result is False

    def test_add_schema_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test schema tag addition failure."""
        tags: Dict[str, str] = {"owner": "team"}

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.add_schema_tags(
                "catalog", "schema", tags
            )

        assert result is False

    def test_remove_schema_tags_success(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test successful schema tag removal."""
        tag_keys: List[str] = ["owner", "purpose"]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=[{"result": "success"}]
        ) as mock_exec:
            result: bool = manager_with_params.remove_schema_tags(
                "catalog", "schema", tag_keys
            )

        assert result is True
        expected_sql = "ALTER SCHEMA `catalog`.`schema` UNSET TAGS ('owner', 'purpose')"
        mock_exec.assert_called_once_with(expected_sql)

    def test_remove_schema_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test schema tag removal failure."""
        tag_keys: List[str] = ["owner"]

        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: bool = manager_with_params.remove_schema_tags(
                "catalog", "schema", tag_keys
            )

        assert result is False

    # Tests for list_all_table_tags method edge cases
    def test_list_all_table_tags_no_filters(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test listing all table tags without filters."""
        table_query_result: List[Dict[str, str]] = [
            {
                "table_catalog": "cat1",
                "table_schema": "sch1",
                "table_name": "tbl1",
                "table_type": "TABLE",
                "owner": "user1",
                "comment": "Test table",
            }
        ]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=table_query_result
        ) as mock_exec:
            with patch.object(manager_with_params, "get_table_tags", return_value={}):
                result: Optional[List[Dict]] = manager_with_params.list_all_table_tags()

        assert result is not None
        assert len(result) == 1
        # Check that the WHERE clause uses "1=1" when no filters
        call_args: str = mock_exec.call_args[0][0]
        assert "WHERE 1=1" in call_args

    def test_list_all_table_tags_with_catalog_filter(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test listing table tags with catalog filter."""
        table_query_result: List[Dict[str, str]] = []

        with patch.object(
            manager_with_params, "_execute_sql", return_value=table_query_result
        ) as mock_exec:
            result: Optional[List[Dict]] = manager_with_params.list_all_table_tags(
                catalog="test_catalog"
            )

        assert result == []
        call_args: str = mock_exec.call_args[0][0]
        assert "table_catalog = 'test_catalog'" in call_args

    def test_list_all_table_tags_with_both_filters(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test listing table tags with both catalog and schema filters."""
        table_query_result: List[Dict[str, str]] = []

        with patch.object(
            manager_with_params, "_execute_sql", return_value=table_query_result
        ) as mock_exec:
            result: Optional[List[Dict]] = manager_with_params.list_all_table_tags(
                catalog="test_catalog", schema="test_schema"
            )

        assert result == []
        call_args: str = mock_exec.call_args[0][0]
        assert "table_catalog = 'test_catalog'" in call_args
        assert "table_schema = 'test_schema'" in call_args
        assert " AND " in call_args

    def test_list_all_table_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test listing table tags failure."""
        with patch.object(
            manager_with_params, "_execute_sql", side_effect=Exception("SQL Error")
        ):
            result: Optional[List[Dict]] = manager_with_params.list_all_table_tags()

        assert result is None

    def test_list_all_table_tags_get_tags_failure(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test listing table tags when get_table_tags fails."""
        table_query_result: List[Dict[str, str]] = [
            {
                "table_catalog": "cat1",
                "table_schema": "sch1",
                "table_name": "tbl1",
                "table_type": "TABLE",
                "owner": "user1",
                "comment": "Test table",
            }
        ]

        with patch.object(
            manager_with_params, "_execute_sql", return_value=table_query_result
        ):
            with patch.object(manager_with_params, "get_table_tags", return_value=None):
                result: Optional[List[Dict]] = manager_with_params.list_all_table_tags()

        assert result is not None
        assert len(result) == 1
        assert result[0]["tags"] == {}  # Empty dict when get_table_tags returns None

    # Edge case tests for connection string parsing
    def test_parse_connection_string_with_no_string(self) -> None:
        """Test connection string parsing when no string is provided."""
        manager = UnityResourceManager(connection_params={"test": "value"})
        manager.connection_string = None

        # Should not raise error
        manager._parse_connection_string()

        # connection_params should remain unchanged
        assert manager.connection_params == {"test": "value"}

    # Tests for update_table_tags edge cases
    def test_update_table_tags_no_existing_tags_replace_mode(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table tag update in replace mode when no existing tags."""
        new_tags: Dict[str, str] = {"new_tag": "new_value"}

        with patch.object(manager_with_params, "get_table_tags", return_value={}):
            with patch.object(
                manager_with_params, "_execute_multiple_sql", return_value=True
            ) as mock_exec:
                result: bool = manager_with_params.update_table_tags(
                    "cat", "sch", "tbl", new_tags, replace_all=True
                )

        assert result is True
        # Should only have SET command, no UNSET since no existing tags
        expected_sql: List[str] = [
            "ALTER TABLE `cat`.`sch`.`tbl` SET TAGS ('new_tag' = 'new_value')"
        ]
        mock_exec.assert_called_once_with(expected_sql)

    def test_update_table_tags_empty_new_tags(
        self, manager_with_params: UnityResourceManager
    ) -> None:
        """Test table tag update with empty new tags."""
        with patch.object(
            manager_with_params, "get_table_tags", return_value={"old": "value"}
        ):
            with patch.object(
                manager_with_params, "_execute_multiple_sql", return_value=True
            ) as mock_exec:
                result: bool = manager_with_params.update_table_tags(
                    "cat", "sch", "tbl", {}, replace_all=True
                )

        assert result is True
        # Should only have UNSET command for existing tags
        expected_sql: List[str] = ["ALTER TABLE `cat`.`sch`.`tbl` UNSET TAGS ('old')"]
        mock_exec.assert_called_once_with(expected_sql)
