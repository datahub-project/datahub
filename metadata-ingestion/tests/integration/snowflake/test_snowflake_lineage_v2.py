import json
from unittest import mock

import pytest

from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
    UpstreamTableNode,
)

pytestmark = pytest.mark.integration_batch_2


def test_dynamic_table_moved_lineage_handling():
    """Test the handling of dynamic tables that have been moved between schemas or databases."""

    # Create a mock connection
    mock_connection = mock.MagicMock(spec=SnowflakeConnection)

    # Create a downstream lineage edge for a dynamic table in old location
    db_row = {
        "DOWNSTREAM_TABLE_NAME": "OLD_DB.OLD_SCHEMA.DYNAMIC_TABLE",
        "DOWNSTREAM_TABLE_DOMAIN": SnowflakeObjectDomain.DYNAMIC_TABLE.capitalize(),
        "UPSTREAM_TABLES": json.dumps(
            [
                {
                    "upstream_object_domain": SnowflakeObjectDomain.TABLE.capitalize(),
                    "upstream_object_name": "SOURCE_DB.SOURCE_SCHEMA.SOURCE_TABLE",
                    "query_id": "123456",
                }
            ]
        ),
        "UPSTREAM_COLUMNS": "[]",
        "QUERIES": json.dumps(
            [
                {
                    "query_id": "123456",
                    "query_text": "SELECT * FROM SOURCE_DB.SOURCE_SCHEMA.SOURCE_TABLE",
                    "start_time": "2023-01-01T00:00:00Z",
                }
            ]
        ),
    }

    # Mock the query results to simulate dynamic table not at original location but at new location
    def mock_query_side_effect(query_text, *args, **kwargs):
        mock_cursor = mock.MagicMock()

        if "DYNAMIC_TABLE" in query_text and "OLD_DB.OLD_SCHEMA" in query_text:
            # Return empty results to indicate table not found at original location
            mock_cursor.__iter__.return_value = []
        elif (
            "SHOW" in query_text
            and "DYNAMIC_TABLE" in query_text
            and "ACCOUNT" in query_text
        ):
            # Return found table at new location
            mock_cursor.__iter__.return_value = [
                {
                    "database_name": "NEW_DB",
                    "schema_name": "NEW_SCHEMA",
                    "name": "DYNAMIC_TABLE",
                }
            ]
        else:
            mock_cursor.__iter__.return_value = []

        return mock_cursor

    # Configure mock connection to use our side effect
    mock_connection.query.side_effect = mock_query_side_effect

    # Create necessary objects for lineage extractor
    config = SnowflakeV2Config(
        account_id="test_account",
        username="test_user",
        password="test_password",
    )

    # Create the lineage extractor with mocked components
    lineage_extractor = SnowflakeLineageExtractor(
        config=config,
        report=mock.MagicMock(),
        connection=mock_connection,
        filters=mock.MagicMock(),
        identifiers=mock.MagicMock(),
        redundant_run_skip_handler=None,
        sql_aggregator=mock.MagicMock(),
    )

    # Process the lineage row
    result = lineage_extractor._process_upstream_lineage_row(db_row)

    # Assert that the result has the correct updated table name
    assert result is not None
    assert result.DOWNSTREAM_TABLE_NAME == "NEW_DB.NEW_SCHEMA.DYNAMIC_TABLE"
    assert (
        SnowflakeObjectDomain.DYNAMIC_TABLE.capitalize()
        == result.DOWNSTREAM_TABLE_DOMAIN
    )

    # Verify the query calls
    expected_calls = [
        mock.call("SHOW DYNAMIC TABLES LIKE 'DYNAMIC_TABLE' IN OLD_DB.OLD_SCHEMA"),
        mock.call(
            "SELECT REFRESH_HISTORY FROM OLD_DB.INFORMATION_SCHEMA.DYNAMIC_TABLES WHERE TABLE_NAME = 'DYNAMIC_TABLE' AND TABLE_SCHEMA = 'OLD_SCHEMA'"
        ),
        mock.call("SHOW DYNAMIC TABLES LIKE 'DYNAMIC_TABLE' IN ACCOUNT"),
    ]
    mock_connection.query.assert_has_calls(expected_calls, any_order=False)


def test_upstream_dynamic_table_moved_lineage_handling():
    """Test that dynamic tables that moved are found when used as upstream tables in lineage"""

    # Create a mock connection
    mock_connection = mock.MagicMock(spec=SnowflakeConnection)

    # Mock the query results to handle table existence check and relocation search
    def query_side_effect(query, *args, **kwargs):
        mock_cursor = mock.MagicMock()

        if "SELECT 1 FROM" in query and "OLD_DB.OLD_SCHEMA.DYNAMIC_SOURCE" in query:
            # Table doesn't exist at original location
            mock_cursor.__iter__.return_value = []
        elif (
            "SHOW TABLES LIKE" in query
            and "DYNAMIC_SOURCE" in query
            and "ACCOUNT" in query
        ):
            # Table found in new location
            mock_cursor.__iter__.return_value = [
                {
                    "database_name": "NEW_DB",
                    "schema_name": "NEW_SCHEMA",
                    "name": "DYNAMIC_SOURCE",
                }
            ]
        else:
            mock_cursor.__iter__.return_value = []

        return mock_cursor

    mock_connection.query.side_effect = query_side_effect

    # Create upstream node with original location
    upstream_node = UpstreamTableNode(
        upstream_object_domain=SnowflakeObjectDomain.DYNAMIC_TABLE.capitalize(),
        upstream_object_name="OLD_DB.OLD_SCHEMA.DYNAMIC_SOURCE",
        query_id="123456",
    )

    # Create mocked identifiers
    mock_identifiers = mock.MagicMock()
    mock_identifiers.get_dataset_identifier_from_qualified_name.return_value = (
        "new_db.new_schema.dynamic_source"
    )
    mock_identifiers.gen_dataset_urn.return_value = "urn:li:dataset:(urn:li:dataPlatform:snowflake,new_db.new_schema.dynamic_source,PROD)"

    # Create mock filters that allow any dataset
    mock_filters = mock.MagicMock()
    mock_filters.is_dataset_pattern_allowed.return_value = True

    # Create a lineage extractor
    lineage_extractor = SnowflakeLineageExtractor(
        config=SnowflakeV2Config(
            account_id="test_account",
            username="test_user",
            password="test_password",
            validate_upstreams_against_patterns=False,  # Don't validate upstreams against patterns
        ),
        report=mock.MagicMock(),
        connection=mock_connection,
        filters=mock_filters,
        identifiers=mock_identifiers,
        redundant_run_skip_handler=None,
        sql_aggregator=mock.MagicMock(),
    )

    # Test the map_query_result_upstreams method
    upstreams = lineage_extractor.map_query_result_upstreams(
        upstream_tables=[upstream_node], query_id="123456"
    )

    # Verify the upstream dataset was identified and mapped correctly
    assert len(upstreams) == 1
    assert (
        upstreams[0]
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,new_db.new_schema.dynamic_source,PROD)"
    )

    # Verify the correct identifier was used
    mock_identifiers.get_dataset_identifier_from_qualified_name.assert_called_with(
        "OLD_DB.OLD_SCHEMA.DYNAMIC_SOURCE"
    )
    mock_identifiers.gen_dataset_urn.assert_called_with(
        "new_db.new_schema.dynamic_source"
    )
