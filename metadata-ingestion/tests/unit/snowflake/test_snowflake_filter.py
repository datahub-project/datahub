from unittest.mock import MagicMock, patch

from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
)


def test_is_dataset_pattern_allowed_for_dynamic_tables():
    """Test that dynamic tables are properly filtered by SnowflakeFilter.is_dataset_pattern_allowed"""

    # Create a mock source report
    mock_report = MagicMock()

    # Create mock patterns with mocked allowed methods
    mock_db_pattern = MagicMock()
    mock_db_pattern.allowed.return_value = True

    mock_schema_pattern = MagicMock()
    mock_schema_pattern.allowed.return_value = True

    mock_table_pattern = MagicMock()
    mock_table_pattern.allowed.return_value = True

    # Create filter configuration with mock patterns
    filter_config = MagicMock()
    filter_config.database_pattern = mock_db_pattern
    filter_config.schema_pattern = mock_schema_pattern
    filter_config.table_pattern = mock_table_pattern
    filter_config.match_fully_qualified_names = False

    # Create a patch for _cleanup_qualified_name to return expected values
    with patch(
        "datahub.ingestion.source.snowflake.snowflake_utils._cleanup_qualified_name"
    ) as mock_cleanup:
        # Return the same value as input for testing
        mock_cleanup.side_effect = lambda name, _: name

        snowflake_filter = SnowflakeFilter(
            filter_config=filter_config, structured_reporter=mock_report
        )

        # Test that a dynamic table matching the pattern is allowed
        assert snowflake_filter.is_dataset_pattern_allowed(
            dataset_name="test_db.test_schema.dynamic_table1",
            dataset_type=SnowflakeObjectDomain.DYNAMIC_TABLE,
        )

        # Verify table_pattern was called with the correct argument
        mock_table_pattern.allowed.assert_called_with(
            "test_db.test_schema.dynamic_table1"
        )

        # Test that a dynamic table not matching the pattern is denied
        mock_table_pattern.allowed.return_value = False
        assert not snowflake_filter.is_dataset_pattern_allowed(
            dataset_name="test_db.test_schema.regular_table",
            dataset_type=SnowflakeObjectDomain.DYNAMIC_TABLE,
        )

        # Reset table_pattern.allowed to return True
        mock_table_pattern.allowed.return_value = True

        # Ensure dynamic tables are treated as tables for filtering purposes
        assert snowflake_filter.is_dataset_pattern_allowed(
            dataset_name="test_db.test_schema.dynamic_table2",
            dataset_type=SnowflakeObjectDomain.TABLE,
        )

        # Test dynamic tables with fully qualified name
        mock_fully_qualified_filter_config = MagicMock()
        mock_fully_qualified_filter_config.database_pattern = MagicMock()
        mock_fully_qualified_filter_config.database_pattern.allowed.return_value = True
        mock_fully_qualified_filter_config.schema_pattern = MagicMock()
        mock_fully_qualified_filter_config.schema_pattern.allowed.return_value = True
        mock_fully_qualified_filter_config.table_pattern = MagicMock()
        mock_fully_qualified_filter_config.table_pattern.allowed.return_value = True
        mock_fully_qualified_filter_config.match_fully_qualified_names = True

        fully_qualified_filter = SnowflakeFilter(
            filter_config=mock_fully_qualified_filter_config,
            structured_reporter=mock_report,
        )

        # Should match the fully qualified pattern
        assert fully_qualified_filter.is_dataset_pattern_allowed(
            dataset_name="test_db.test_schema.dynamic_table3",
            dataset_type=SnowflakeObjectDomain.DYNAMIC_TABLE,
        )

        # Should not match with different schema
        mock_fully_qualified_filter_config.table_pattern.allowed.return_value = False
        assert not fully_qualified_filter.is_dataset_pattern_allowed(
            dataset_name="test_db.other_schema.dynamic_table3",
            dataset_type=SnowflakeObjectDomain.DYNAMIC_TABLE,
        )
