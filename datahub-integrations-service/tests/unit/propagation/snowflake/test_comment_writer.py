"""
Comprehensive unit tests for the Snowflake comment writer module.

Tests all classes: URNParser, ObjectTypeDetector, CommentUpdater, and
SnowflakeCommentManager.
"""

from unittest.mock import Mock, patch

import pytest
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn

from datahub_integrations.propagation.snowflake.comment_writer import (
    CommentUpdater,
    ObjectTypeDetector,
    SnowflakeCommentManager,
    URNParser,
)


class TestURNParser:
    """Test URN parsing functionality."""

    def test_parse_dataset_urn_success(self) -> None:
        """Test successful parsing of dataset URN."""
        dataset_urn = DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        )

        result = URNParser.parse_dataset_urn(dataset_urn)

        assert result == ("test_db", "test_schema", "test_table")

    def test_parse_dataset_urn_insufficient_parts(self) -> None:
        """Test parsing dataset URN with insufficient parts."""
        dataset_urn = DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema,PROD)"
        )

        result = URNParser.parse_dataset_urn(dataset_urn)

        assert result is None

    def test_parse_field_urn_success(self) -> None:
        """Test successful parsing of schema field URN."""
        field_urn = SchemaFieldUrn.create_from_string(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD),test_column)"
        )

        result = URNParser.parse_field_urn(field_urn)

        assert result == ("test_db", "test_schema", "test_table", "test_column")

    def test_parse_field_urn_invalid_parent(self) -> None:
        """Test parsing field URN with invalid parent."""
        # Create a mock field URN with invalid parent
        field_urn = Mock(spec=SchemaFieldUrn)
        field_urn.parent = "urn:li:invalid:parent"
        field_urn.field_path = "test_column"

        with patch(
            "datahub_integrations.propagation.snowflake.comment_writer.Urn.create_from_string"
        ) as mock_create:
            mock_create.return_value = Mock()  # Not a DatasetUrn

            result = URNParser.parse_field_urn(field_urn)

            assert result is None

    def test_parse_field_urn_dataset_parsing_fails(self) -> None:
        """Test parsing field URN when dataset parsing fails."""
        field_urn = SchemaFieldUrn.create_from_string(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,invalid,PROD),test_column)"
        )

        result = URNParser.parse_field_urn(field_urn)

        assert result is None


class TestObjectTypeDetector:
    """Test object type detection functionality."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.detector = ObjectTypeDetector(self.mock_connection)

    def test_detect_object_type_table(self) -> None:
        """Test detecting table object type."""
        # Mock cursor and results for table detection
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ("TABLE",)
        self.mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        result = self.detector.detect_object_type(
            "test_db", "test_schema", "test_table"
        )

        assert result == "TABLE"
        mock_cursor.execute.assert_called_once()

    def test_detect_object_type_view(self) -> None:
        """Test detecting view object type."""
        # Mock cursor and results for view detection
        mock_cursor = Mock()
        # First call returns None (not a table), second call returns VIEW
        mock_cursor.fetchone.side_effect = [None, ("VIEW",)]
        self.mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        result = self.detector.detect_object_type("test_db", "test_schema", "test_view")

        assert result == "VIEW"
        assert mock_cursor.execute.call_count == 2

    def test_detect_object_type_not_found(self) -> None:
        """Test detecting object type when object not found."""
        # Mock cursor that returns None for both queries
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        self.mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        result = self.detector.detect_object_type(
            "test_db", "test_schema", "nonexistent"
        )

        assert result is None
        assert mock_cursor.execute.call_count == 2

    def test_detect_object_type_exception_handling(self) -> None:
        """Test exception handling in object type detection."""
        # Mock cursor that raises an exception
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database error")
        self.mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        result = self.detector.detect_object_type(
            "test_db", "test_schema", "test_table"
        )

        assert result is None


class TestCommentUpdater:
    """Test comment update functionality."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.mock_query_executor = Mock()
        self.updater = CommentUpdater(self.mock_connection, self.mock_query_executor)

    def test_update_table_comment_success(self) -> None:
        """Test successful table comment update."""
        self.updater.update_table_comment(
            "test_db", "test_schema", "test_table", "Test description", "TABLE"
        )

        # Split SQL query to comply with line length limits
        # Use triple quotes for descriptions to handle multi-line content and quotes
        expected_query = (
            "ALTER TABLE test_db.test_schema.test_table "
            "SET COMMENT = '''Test description'''"
        )
        self.mock_query_executor.assert_called_once_with(expected_query)

    def test_update_table_comment_with_quotes_in_description(self) -> None:
        """Test table comment update with quotes in description."""
        description_with_quotes = "Test 'description' with \"quotes\""

        self.updater.update_table_comment(
            "test_db", "test_schema", "test_table", description_with_quotes, "TABLE"
        )

        # With triple quotes, no escaping needed for single/double quotes
        expected_description = "Test 'description' with \"quotes\""
        # Split SQL query to comply with line length limits
        # Use triple quotes for descriptions to handle multi-line content and quotes
        expected_query = (
            f"ALTER TABLE test_db.test_schema.test_table "
            f"SET COMMENT = '''{expected_description}'''"
        )
        self.mock_query_executor.assert_called_once_with(expected_query)

    def test_update_view_comment_success(self) -> None:
        """Test successful view comment update."""
        self.updater.update_table_comment(
            "test_db", "test_schema", "test_view", "Test view description", "VIEW"
        )

        # Split SQL query to comply with line length limits
        # Use triple quotes for descriptions to handle multi-line content and quotes
        expected_query = (
            "ALTER VIEW test_db.test_schema.test_view "
            "SET COMMENT = '''Test view description'''"
        )
        self.mock_query_executor.assert_called_once_with(expected_query)

    def test_update_column_comment_success(self) -> None:
        """Test successful column comment update."""
        self.updater.update_column_comment(
            "test_db",
            "test_schema",
            "test_table",
            "test_column",
            "Column description",
            "TABLE",
        )

        # Split SQL query to comply with line length limits
        # Use triple quotes for descriptions to handle multi-line content and quotes
        expected_query = (
            "ALTER TABLE test_db.test_schema.test_table "
            "MODIFY COLUMN test_column COMMENT '''Column description'''"
        )
        self.mock_query_executor.assert_called_once_with(expected_query)

    def test_try_table_then_view_comment_table_success(self) -> None:
        """Test trying table comment first (success case)."""
        # Mock successful table comment update
        self.mock_query_executor.side_effect = [None]  # First call succeeds

        self.updater.try_table_then_view_comment(
            "test_db", "test_schema", "test_object", "Test description"
        )

        # Should only call table comment update
        assert self.mock_query_executor.call_count == 1
        # Split SQL query to comply with line length limits
        # Use triple quotes for descriptions to handle multi-line content and quotes
        expected_query = (
            "ALTER TABLE test_db.test_schema.test_object "
            "SET COMMENT = '''Test description'''"
        )
        self.mock_query_executor.assert_called_with(expected_query)

    def test_try_table_then_view_comment_table_fails_view_succeeds(self) -> None:
        """Test trying table comment first (fails), then view comment (succeeds)."""
        # Mock table comment failure, view comment success
        table_error = Exception("Table not found")
        self.mock_query_executor.side_effect = [
            table_error,
            None,
        ]  # First fails, second succeeds

        self.updater.try_table_then_view_comment(
            "test_db", "test_schema", "test_object", "Test description"
        )

        # Should call both table and view comment updates
        assert self.mock_query_executor.call_count == 2

    def test_try_table_then_view_comment_both_fail(self) -> None:
        """Test trying table comment first (fails), then view comment (also fails)."""
        # Mock both table and view comment failures
        table_error = Exception("Table not found")
        view_error = Exception("View not found")
        self.mock_query_executor.side_effect = [table_error, view_error]

        with pytest.raises(Exception, match="View not found"):
            self.updater.try_table_then_view_comment(
                "test_db", "test_schema", "test_object", "Test description"
            )


class TestSnowflakeCommentManager:
    """Test the high-level comment manager."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.mock_query_executor = Mock()

        # Mock the component dependencies
        with (
            patch(
                "datahub_integrations.propagation.snowflake.comment_writer.ObjectTypeDetector"
            ) as mock_detector_class,
            patch(
                "datahub_integrations.propagation.snowflake.comment_writer.CommentUpdater"
            ) as mock_updater_class,
        ):
            self.mock_detector = Mock()
            self.mock_updater = Mock()
            mock_detector_class.return_value = self.mock_detector
            mock_updater_class.return_value = self.mock_updater

            self.manager = SnowflakeCommentManager(
                self.mock_connection, self.mock_query_executor
            )

    def test_apply_description_dataset_urn_with_subtype(self) -> None:
        """Test applying description to dataset URN with known subtype."""
        # Split URN string to comply with line length limits
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )

        self.manager.apply_description(dataset_urn, "Test description", subtype="TABLE")

        # Should call table comment update directly (no detection needed)
        self.mock_updater.update_table_comment.assert_called_once_with(
            "test_db", "test_schema", "test_table", "Test description", "TABLE"
        )
        self.mock_detector.detect_object_type.assert_not_called()

    def test_apply_description_dataset_urn_without_subtype(self) -> None:
        """Test applying description to dataset URN without subtype (auto-detect)."""
        # Split URN string to comply with line length limits
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )
        self.mock_detector.detect_object_type.return_value = "VIEW"

        self.manager.apply_description(dataset_urn, "Test description")

        # Should detect object type and call table comment update with VIEW type
        self.mock_detector.detect_object_type.assert_called_once_with(
            "test_db", "test_schema", "test_table"
        )
        self.mock_updater.update_table_comment.assert_called_once_with(
            "test_db", "test_schema", "test_table", "Test description", "VIEW"
        )

    def test_apply_description_dataset_urn_unknown_type(self) -> None:
        """Test applying description to dataset URN with unknown type (fallback)."""
        # Split URN string to comply with line length limits
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )
        self.mock_detector.detect_object_type.return_value = None

        self.manager.apply_description(dataset_urn, "Test description")

        # Should detect object type and fallback to try_table_then_view_comment
        self.mock_detector.detect_object_type.assert_called_once_with(
            "test_db", "test_schema", "test_table"
        )
        self.mock_updater.try_table_then_view_comment.assert_called_once_with(
            "test_db", "test_schema", "test_table", "Test description"
        )

    def test_apply_description_schema_field_urn(self) -> None:
        """Test applying description to schema field URN."""
        # Split URN string to comply with line length limits
        field_urn = (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD),test_column)"
        )

        self.manager.apply_description(field_urn, "Column description")

        # Should call column comment update
        self.mock_updater.update_column_comment.assert_called_once_with(
            "test_db",
            "test_schema",
            "test_table",
            "test_column",
            "Column description",
            "TABLE",
        )

    def test_apply_description_invalid_urn(self) -> None:
        """Test applying description to invalid URN type."""
        invalid_urn = "urn:li:corpuser:test_user"

        with pytest.raises(ValueError, match="Invalid entity urn"):
            self.manager.apply_description(invalid_urn, "Test description")

    def test_apply_description_exception_handling(self) -> None:
        """Test exception handling in apply_description."""
        # Split URN string to comply with line length limits
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )

        # Mock an exception during URN parsing
        with patch(
            "datahub_integrations.propagation.snowflake.comment_writer.Urn.create_from_string"
        ) as mock_create:
            mock_create.side_effect = Exception("URN parsing error")

            with pytest.raises(Exception, match="Failed to apply description"):
                self.manager.apply_description(dataset_urn, "Test description")

    def test_update_table_comment_private_method(self) -> None:
        """Test the private _update_table_comment method."""
        dataset_urn = DatasetUrn.create_from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)"
        )

        # Test with known subtype
        self.manager._update_table_comment(dataset_urn, "Test description", "TABLE")

        self.mock_updater.update_table_comment.assert_called_once_with(
            "test_db", "test_schema", "test_table", "Test description", "TABLE"
        )

    def test_update_column_comment_private_method(self) -> None:
        """Test the private _update_column_comment method."""
        field_urn = SchemaFieldUrn.create_from_string(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD),test_column)"
        )

        self.manager._update_column_comment(field_urn, "Column description")

        self.mock_updater.update_column_comment.assert_called_once_with(
            "test_db",
            "test_schema",
            "test_table",
            "test_column",
            "Column description",
            "TABLE",
        )


class TestCommentWriterIntegration:
    """Integration tests for the comment writer components."""

    def test_end_to_end_dataset_comment_update(self) -> None:
        """Test end-to-end dataset comment update flow."""
        mock_connection = Mock()
        mock_query_executor = Mock()

        # Create real manager with mocked dependencies
        manager = SnowflakeCommentManager(mock_connection, mock_query_executor)

        # Split URN string to comply with line length limits
        dataset_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD)"
        )

        # Mock object type detection to return TABLE
        with patch.object(
            manager.object_detector, "detect_object_type", return_value="TABLE"
        ):
            manager.apply_description(dataset_urn, "Integration test description")

        # Verify the query executor was called with the correct SQL
        mock_query_executor.assert_called_once()
        call_args = mock_query_executor.call_args[0][0]
        assert "COMMENT ON TABLE" in call_args
        assert "Integration test description" in call_args

    def test_end_to_end_column_comment_update(self) -> None:
        """Test end-to-end column comment update flow."""
        mock_connection = Mock()
        mock_query_executor = Mock()

        # Create real manager with mocked dependencies
        manager = SnowflakeCommentManager(mock_connection, mock_query_executor)

        # Split URN string to comply with line length limits
        field_urn = (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "test_db.test_schema.test_table,PROD),test_column)"
        )

        manager.apply_description(field_urn, "Integration test column description")

        # Verify the query executor was called with the correct SQL
        mock_query_executor.assert_called_once()
        call_args = mock_query_executor.call_args[0][0]
        assert "COMMENT ON COLUMN" in call_args
        assert "Integration test column description" in call_args
