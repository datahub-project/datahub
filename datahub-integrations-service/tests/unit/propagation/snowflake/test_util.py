"""
Comprehensive unit tests for the Snowflake util module.

Tests the is_snowflake_urn function and SnowflakeTagHelper class.
"""

from collections import deque
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from datahub.metadata.schema_classes import GlossaryTermInfoClass
from datahub_actions.api.action_graph import AcrylDataHubGraph
from sqlalchemy.exc import ProgrammingError

from datahub_integrations.propagation.snowflake.config import (
    SnowflakeConnectionConfigPermissive,
)
from datahub_integrations.propagation.snowflake.util import (
    MAX_ERRORS_PER_HOUR,
    SnowflakeTagHelper,
    is_snowflake_urn,
)


class TestIsSnowflakeUrn:
    """Test the is_snowflake_urn utility function."""

    def test_is_snowflake_urn_dataset_true(self):
        """Test that Snowflake dataset URN returns True."""
        # Split URN string to comply with line length limits
        urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )

        result = is_snowflake_urn(urn)

        assert result is True

    def test_is_snowflake_urn_schema_field_true(self):
        """Test that Snowflake schema field URN returns True."""
        # Split URN string to comply with line length limits
        urn = (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD),test_column)"
        )

        result = is_snowflake_urn(urn)

        assert result is True

    def test_is_snowflake_urn_non_snowflake_false(self):
        """Test that non-Snowflake URN returns False."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"

        result = is_snowflake_urn(urn)

        assert result is False

    def test_is_snowflake_urn_invalid_urn_false(self):
        """Test that invalid URN returns False."""
        urn = "urn:li:corpuser:test_user"

        result = is_snowflake_urn(urn)

        assert result is False


class TestSnowflakeTagHelper:
    """Test the SnowflakeTagHelper class."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create a mock config
        self.mock_config = Mock(spec=SnowflakeConnectionConfigPermissive)
        self.mock_config.account_id = "test_account"
        self.mock_config.username = "test_user"
        self.mock_config.password = Mock()
        self.mock_config.password.get_secret_value.return_value = "test_password"
        self.mock_config.warehouse = "test_warehouse"
        self.mock_config.role = "test_role"
        self.mock_config.authentication_type = "DEFAULT_AUTHENTICATOR"
        self.mock_config.get_connect_args.return_value = {}

        # Create helper instance
        self.helper = SnowflakeTagHelper(self.mock_config)

    def test_initialization(self):
        """Test SnowflakeTagHelper initialization."""
        assert self.helper.config == self.mock_config
        assert self.helper._connection is None
        assert self.helper._comment_manager is None
        assert isinstance(self.helper.error_timestamps, deque)
        assert self.helper.error_threshold == MAX_ERRORS_PER_HOUR

    @patch(
        "datahub_integrations.propagation.snowflake.util.snowflake.connector.connect"
    )
    def test_connection_property_lazy_loading(self, mock_connect):
        """Test that connection is created lazily."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        # First access should create connection
        connection = self.helper.connection

        assert connection == mock_connection
        mock_connect.assert_called_once()

        # Second access should return same connection
        connection2 = self.helper.connection
        assert connection2 == mock_connection
        assert mock_connect.call_count == 1  # Should not be called again

    @patch("datahub_integrations.propagation.snowflake.util.SnowflakeCommentManager")
    def test_comment_manager_property_lazy_loading(self, mock_manager_class):
        """Test that comment manager is created lazily."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager

        with patch.object(self.helper, "connection") as mock_connection:
            # First access should create manager
            manager = self.helper.comment_manager

            assert manager == mock_manager
            mock_manager_class.assert_called_once_with(
                mock_connection, self.helper._run_query_direct
            )

            # Second access should return same manager
            manager2 = self.helper.comment_manager
            assert manager2 == mock_manager
            assert mock_manager_class.call_count == 1  # Should not be called again

    @patch(
        "datahub_integrations.propagation.snowflake.util.snowflake.connector.connect"
    )
    def test_get_native_connection_default_auth(self, mock_connect):
        """Test getting native connection with default authentication."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        connection = self.helper._get_native_connection()

        assert connection == mock_connection
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[1]
        assert call_args["user"] == "test_user"
        assert call_args["password"] == "test_password"
        assert call_args["account"] == "test_account"
        assert call_args["warehouse"] == "test_warehouse"
        assert call_args["role"] == "test_role"
        assert call_args["application"] == "acryl_datahub"

    @patch(
        "datahub_integrations.propagation.snowflake.util.snowflake.connector.connect"
    )
    def test_get_native_connection_key_pair_auth(self, mock_connect):
        """Test getting native connection with key pair authentication."""
        self.mock_config.authentication_type = "KEY_PAIR_AUTHENTICATOR"
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        connection = self.helper._get_native_connection()

        assert connection == mock_connection
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[1]
        assert call_args["authenticator"] == "KEY_PAIR_AUTHENTICATOR"
        assert "password" not in call_args

    def test_get_term_name_from_id_with_name(self):
        """Test getting term name when term info has name."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        mock_term_info = Mock(spec=GlossaryTermInfoClass)
        mock_term_info.name = "Test Term Name"
        mock_graph.graph.get_aspect.return_value = mock_term_info

        term_urn = "urn:li:glossaryTerm:test_term_id"

        result = SnowflakeTagHelper.get_term_name_from_id(term_urn, mock_graph)

        assert result == "Test Term Name"

    def test_get_term_name_from_id_without_name(self):
        """Test getting term name when term info has no name."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        mock_graph.graph.get_aspect.return_value = None

        term_urn = "urn:li:glossaryTerm:test_term_id"

        result = SnowflakeTagHelper.get_term_name_from_id(term_urn, mock_graph)

        assert result == "test_term_id"

    def test_get_label_urn_to_tag_with_tag_urn(self):
        """Test getting label from tag URN."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        tag_urn = "urn:li:tag:TestTag"

        result = SnowflakeTagHelper.get_label_urn_to_tag(tag_urn, mock_graph)

        assert result == "TestTag"

    def test_get_label_urn_to_tag_with_term_urn(self):
        """Test getting label from glossary term URN."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        term_urn = "urn:li:glossaryTerm:test_term_id"

        with patch.object(
            SnowflakeTagHelper, "get_term_name_from_id", return_value="Test Term"
        ):
            result = SnowflakeTagHelper.get_label_urn_to_tag(term_urn, mock_graph)

            assert result == "Test Term"

    def test_get_label_urn_to_tag_invalid_urn(self):
        """Test getting label from invalid URN type."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        invalid_urn = "urn:li:corpuser:test_user"

        with pytest.raises(Exception, match="Unexpected label type"):
            SnowflakeTagHelper.get_label_urn_to_tag(invalid_urn, mock_graph)

    def test_has_special_chars_true(self):
        """Test has_special_chars returns True for strings with special characters."""
        assert self.helper.has_special_chars("test-table") is True
        assert self.helper.has_special_chars("test.table") is True
        assert self.helper.has_special_chars("test table") is True
        assert self.helper.has_special_chars("test@table") is True

    def test_has_special_chars_false(self):
        """Test has_special_chars returns False for alphanumeric strings."""
        assert self.helper.has_special_chars("test_table") is False
        assert self.helper.has_special_chars("TestTable123") is False
        assert self.helper.has_special_chars("test123") is False

    def test_should_quote_identifier_special_chars(self):
        """Test identifier quoting for special characters."""
        assert self.helper._should_quote_identifier("test-table") is True
        assert self.helper._should_quote_identifier("test.table") is True
        assert self.helper._should_quote_identifier("test table") is True

    def test_should_quote_identifier_reserved_words(self):
        """Test identifier quoting for reserved words."""
        assert self.helper._should_quote_identifier("SELECT") is True
        assert self.helper._should_quote_identifier("table") is True
        assert self.helper._should_quote_identifier("FROM") is True

    def test_should_quote_identifier_starts_with_number(self):
        """Test identifier quoting for identifiers starting with numbers."""
        assert self.helper._should_quote_identifier("123table") is True
        assert self.helper._should_quote_identifier("9test") is True

    def test_should_quote_identifier_normal_identifier(self):
        """Test identifier quoting for normal identifiers."""
        assert self.helper._should_quote_identifier("test_table") is False
        assert self.helper._should_quote_identifier("TestTable") is False
        assert self.helper._should_quote_identifier("my_column") is False

    def test_format_identifier_needs_quoting(self):
        """Test identifier formatting when quoting is needed."""
        result = self.helper._format_identifier("test-table")
        assert result == '"test-table"'

        result = self.helper._format_identifier("SELECT")
        assert result == '"SELECT"'

    def test_format_identifier_no_quoting_needed(self):
        """Test identifier formatting when no quoting is needed."""
        result = self.helper._format_identifier("test_table")
        assert result == "test_table"

        result = self.helper._format_identifier("MyTable")
        assert result == "MyTable"

    def test_format_identifier_already_quoted(self):
        """Test identifier formatting when already quoted."""
        result = self.helper._format_identifier('"test-table"')
        assert result == '"test-table"'

    @patch.object(SnowflakeTagHelper, "get_label_urn_to_tag", return_value="TestTag")
    def test_apply_tag_or_term_non_snowflake_urn(self, mock_get_label):
        """Test applying tag to non-Snowflake URN (should return early)."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        entity_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
        )
        tag_urn = "urn:li:tag:TestTag"

        # Should return early without doing anything
        self.helper.apply_tag_or_term(entity_urn, tag_urn, mock_graph)

        mock_get_label.assert_not_called()

    @patch.object(SnowflakeTagHelper, "get_label_urn_to_tag", return_value="TestTag")
    @patch.object(SnowflakeTagHelper, "_create_tag")
    @patch.object(SnowflakeTagHelper, "_run_query")
    def test_apply_tag_or_term_dataset_urn(
        self, mock_run_query, mock_create_tag, mock_get_label
    ):
        """Test applying tag to dataset URN."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        # Split URN string to comply with line length limits
        entity_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )
        tag_urn = "urn:li:tag:TestTag"

        self.helper.apply_tag_or_term(entity_urn, tag_urn, mock_graph)

        mock_get_label.assert_called_once_with(tag_urn, mock_graph)
        mock_create_tag.assert_called_once_with(
            "test_db", "test_schema", "TestTag", tag_urn
        )
        mock_run_query.assert_called_once()

    @patch.object(SnowflakeTagHelper, "get_label_urn_to_tag", return_value="TestTag")
    @patch.object(SnowflakeTagHelper, "_create_tag")
    @patch.object(SnowflakeTagHelper, "find_table_name", return_value="test_table")
    @patch.object(SnowflakeTagHelper, "find_column_name", return_value="test_column")
    @patch.object(SnowflakeTagHelper, "_run_query")
    def test_apply_tag_or_term_schema_field_urn(
        self,
        mock_run_query,
        mock_find_column,
        mock_find_table,
        mock_create_tag,
        mock_get_label,
    ):
        """Test applying tag to schema field URN."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        # Split URN string to comply with line length limits
        entity_urn = (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD),test_column)"
        )
        tag_urn = "urn:li:tag:TestTag"

        self.helper.apply_tag_or_term(entity_urn, tag_urn, mock_graph)

        mock_get_label.assert_called_once_with(tag_urn, mock_graph)
        mock_create_tag.assert_called_once_with(
            "test_db", "test_schema", "TestTag", tag_urn
        )
        mock_run_query.assert_called_once()

    @patch.object(SnowflakeTagHelper, "get_label_urn_to_tag", return_value="TestTag")
    def test_apply_tag_or_term_invalid_urn(self, mock_get_label):
        """Test applying tag to invalid URN type."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        entity_urn = "urn:li:corpuser:test_user"
        tag_urn = "urn:li:tag:TestTag"

        with pytest.raises(ValueError, match="Invalid entity urn"):
            self.helper.apply_tag_or_term(entity_urn, tag_urn, mock_graph)

    def test_find_table_name_found(self):
        """Test finding table name when it exists."""
        mock_columns = {"TEST_TABLE": ["col1", "col2"], "OTHER_TABLE": ["col3"]}

        with patch.object(self.helper, "_get_columns", return_value=mock_columns):
            result = self.helper.find_table_name("test_db", "test_schema", "test_table")

            assert result == "TEST_TABLE"

    def test_find_table_name_not_found(self):
        """Test finding table name when it doesn't exist."""
        mock_columns = {"OTHER_TABLE": ["col1", "col2"]}

        with patch.object(self.helper, "_get_columns", return_value=mock_columns):
            result = self.helper.find_table_name(
                "test_db", "test_schema", "nonexistent_table"
            )

            assert result == "nonexistent_table"

    def test_find_column_name_found(self):
        """Test finding column name when it exists."""
        mock_columns = {"test_table": ["TEST_COLUMN", "other_column"]}

        with patch.object(self.helper, "_get_columns", return_value=mock_columns):
            result = self.helper.find_column_name(
                "test_db", "test_schema", "test_table", "test_column"
            )

            assert result == "TEST_COLUMN"

    def test_find_column_name_not_found(self):
        """Test finding column name when it doesn't exist."""
        mock_columns = {"test_table": ["other_column"]}

        with patch.object(self.helper, "_get_columns", return_value=mock_columns):
            result = self.helper.find_column_name(
                "test_db", "test_schema", "test_table", "nonexistent_column"
            )

            assert result == "nonexistent_column"

    @patch.object(SnowflakeTagHelper, "get_label_urn_to_tag", return_value="TestTag")
    @patch.object(SnowflakeTagHelper, "find_table_name", return_value="test_table")
    @patch.object(SnowflakeTagHelper, "_run_query")
    def test_remove_tag_or_term_dataset_urn(
        self, mock_run_query, mock_find_table, mock_get_label
    ):
        """Test removing tag from dataset URN."""
        mock_graph = Mock(spec=AcrylDataHubGraph)
        # Split URN string to comply with line length limits
        entity_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )
        tag_urn = "urn:li:tag:TestTag"

        self.helper.remove_tag_or_term(entity_urn, tag_urn, mock_graph)

        mock_get_label.assert_called_once_with(tag_urn, mock_graph)
        mock_run_query.assert_called_once()
        # Verify the query contains UNSET TAG
        call_args = mock_run_query.call_args[0]
        assert 'UNSET TAG "TestTag"' in call_args[2]

    def test_create_tag(self):
        """Test creating a tag in Snowflake."""
        with patch.object(self.helper, "_run_query") as mock_run_query:
            self.helper._create_tag(
                "test_db", "test_schema", "TestTag", "urn:li:tag:TestTag"
            )

            mock_run_query.assert_called_once()
            call_args = mock_run_query.call_args[0]
            assert 'CREATE TAG IF NOT EXISTS "TestTag"' in call_args[2]
            assert "Replicated Tag urn:li:tag:TestTag from DataHub" in call_args[2]

    def test_get_columns_success(self):
        """Test successful column retrieval."""
        mock_cursor = Mock()
        mock_cursor.description = [("table_name",), ("column_name",)]
        mock_cursor.fetchmany.side_effect = [
            [("test_table", "col1"), ("test_table", "col2")],
            [],  # End of results
        ]

        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        with (
            patch.object(self.helper, "connection", mock_connection),
            patch.object(self.helper, "_too_many_errors", return_value=False),
        ):
            result = self.helper._get_columns("test_db", "test_schema", "SHOW COLUMNS;")

            expected = {"test_table": ["col1", "col2"]}
            assert result == expected

    def test_get_columns_too_many_errors(self):
        """Test column retrieval when too many errors occurred."""
        with patch.object(self.helper, "_too_many_errors", return_value=True):
            result = self.helper._get_columns("test_db", "test_schema", "SHOW COLUMNS;")

            assert result == {}

    def test_get_columns_exception_handling(self):
        """Test column retrieval exception handling."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database error")

        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        with (
            patch.object(self.helper, "connection", mock_connection),
            patch.object(self.helper, "_too_many_errors", return_value=False),
            patch.object(self.helper, "_log_error") as mock_log_error,
        ):
            result = self.helper._get_columns("test_db", "test_schema", "SHOW COLUMNS;")

            assert result == {}
            mock_log_error.assert_called_once()

    def test_run_query_success(self):
        """Test successful query execution."""
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        with (
            patch.object(self.helper, "connection", mock_connection),
            patch.object(self.helper, "_too_many_errors", return_value=False),
        ):
            self.helper._run_query("test_db", "test_schema", "SELECT 1;")

            # Verify USE statement and query execution
            assert mock_cursor.execute.call_count == 2
            calls = mock_cursor.execute.call_args_list
            assert "USE" in calls[0][0][0]
            assert "SELECT 1;" in calls[1][0][0]

    def test_run_query_too_many_errors(self):
        """Test query execution when too many errors occurred."""
        with patch.object(self.helper, "_too_many_errors", return_value=True):
            # Should return early without executing query
            self.helper._run_query("test_db", "test_schema", "SELECT 1;")
            # No assertions needed - just verify no exception is raised

    def test_run_query_programming_error(self):
        """Test query execution with ProgrammingError."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = [
            None,
            ProgrammingError("SQL error", None, None),
        ]

        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        with (
            patch.object(self.helper, "connection", mock_connection),
            patch.object(self.helper, "_too_many_errors", return_value=False),
            patch.object(self.helper, "_log_error") as mock_log_error,
        ):
            with pytest.raises(ValueError, match="Failed to execute snowflake query"):
                self.helper._run_query("test_db", "test_schema", "INVALID SQL;")

            mock_log_error.assert_called_once()

    def test_run_query_direct_success(self):
        """Test successful direct query execution."""
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor

        with (
            patch.object(self.helper, "connection", mock_connection),
            patch.object(self.helper, "_too_many_errors", return_value=False),
        ):
            self.helper._run_query_direct("SELECT 1;")

            # Verify only the query is executed (no USE statement)
            mock_cursor.execute.assert_called_once_with("SELECT 1;")

    def test_cleanup_old_errors(self):
        """Test cleanup of old error timestamps."""
        now = datetime.now()
        old_error = now - timedelta(hours=2)
        recent_error = now - timedelta(minutes=30)

        self.helper.error_timestamps.extend([old_error, recent_error])

        self.helper._cleanup_old_errors()

        # Only recent error should remain
        assert len(self.helper.error_timestamps) == 1
        assert self.helper.error_timestamps[0] == recent_error

    def test_log_error(self):
        """Test error logging."""
        initial_count = len(self.helper.error_timestamps)

        with patch.object(self.helper, "_cleanup_old_errors") as mock_cleanup:
            self.helper._log_error()

            assert len(self.helper.error_timestamps) == initial_count + 1
            mock_cleanup.assert_called_once()

    def test_too_many_errors_false(self):
        """Test too_many_errors returns False when under threshold."""
        # Add fewer errors than threshold
        for _ in range(self.helper.error_threshold - 1):
            self.helper.error_timestamps.append(datetime.now())

        with patch.object(self.helper, "_cleanup_old_errors"):
            result = self.helper._too_many_errors()

            assert result is False

    def test_too_many_errors_true(self):
        """Test too_many_errors returns True when at threshold."""
        # Add errors equal to threshold
        for _ in range(self.helper.error_threshold):
            self.helper.error_timestamps.append(datetime.now())

        with patch.object(self.helper, "_cleanup_old_errors"):
            result = self.helper._too_many_errors()

            assert result is True

    def test_apply_description_delegation(self):
        """Test that apply_description delegates to comment manager."""
        # Split URN string to comply with line length limits
        entity_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )

        with patch.object(self.helper, "comment_manager") as mock_manager:
            self.helper.apply_description(entity_urn, "Test description", "TABLE")

            mock_manager.apply_description.assert_called_once_with(
                entity_urn, "Test description", "TABLE"
            )

    def test_close_with_open_connection(self):
        """Test closing helper with open connection."""
        mock_connection = Mock()
        mock_connection.is_closed.return_value = False
        self.helper._connection = mock_connection

        self.helper.close()

        mock_connection.close.assert_called_once()

    def test_close_with_closed_connection(self):
        """Test closing helper with already closed connection."""
        mock_connection = Mock()
        mock_connection.is_closed.return_value = True
        self.helper._connection = mock_connection

        self.helper.close()

        mock_connection.close.assert_not_called()

    def test_close_with_no_connection(self):
        """Test closing helper with no connection."""
        # Should not raise any exception
        self.helper.close()


class TestSnowflakeTagHelperIntegration:
    """Integration tests for SnowflakeTagHelper."""

    def test_end_to_end_tag_application(self):
        """Test end-to-end tag application flow."""
        mock_config = Mock(spec=SnowflakeConnectionConfigPermissive)
        helper = SnowflakeTagHelper(mock_config)

        # Mock all the dependencies
        with (
            patch.object(helper, "get_label_urn_to_tag", return_value="TestTag"),
            patch.object(helper, "_create_tag"),
            patch.object(helper, "_run_query"),
            patch.object(helper, "has_special_chars", return_value=False),
        ):
            mock_graph = Mock(spec=AcrylDataHubGraph)
            # Split URN string to comply with line length limits
            entity_urn = (
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
                "test_db.test_schema.test_table,PROD)"
            )
            tag_urn = "urn:li:tag:TestTag"

            helper.apply_tag_or_term(entity_urn, tag_urn, mock_graph)

            # Verify the flow executed without errors
            helper.get_label_urn_to_tag.assert_called_once()
            helper._create_tag.assert_called_once()
            helper._run_query.assert_called_once()

    def test_error_rate_limiting_integration(self):
        """Test that error rate limiting works correctly."""
        mock_config = Mock(spec=SnowflakeConnectionConfigPermissive)
        helper = SnowflakeTagHelper(mock_config)

        # Simulate hitting the error threshold
        for _ in range(MAX_ERRORS_PER_HOUR):
            helper._log_error()

        # Now queries should be skipped
        with patch.object(helper, "connection") as mock_connection:
            helper._run_query("test_db", "test_schema", "SELECT 1;")

            # Connection should not be accessed due to rate limiting
            mock_connection.assert_not_called()
