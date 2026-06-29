from datetime import datetime
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeView,
)


class TestSnowflakeDataDictionary:
    @pytest.fixture
    def mock_snowflake_data_dictionary_information_schema(
        self,
    ) -> SnowflakeDataDictionary:
        connection = MagicMock(spec=SnowflakeConnection)
        report = MagicMock(spec=SnowflakeV2Report)
        data_dict = SnowflakeDataDictionary(
            connection, report, fetch_views_from_information_schema=True
        )
        return data_dict

    @pytest.fixture
    def mock_snowflake_data_dictionary_show_views(self) -> SnowflakeDataDictionary:
        connection = MagicMock(spec=SnowflakeConnection)
        report = MagicMock(spec=SnowflakeV2Report)
        data_dict = SnowflakeDataDictionary(
            connection, report, fetch_views_from_information_schema=False
        )
        return data_dict

    def test_fetch_views_from_information_schema_enabled(
        self, mock_snowflake_data_dictionary_information_schema
    ):
        """Test that fetch_views_from_information_schema=True uses information schema method"""
        # Mock the query response for information_schema.views
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [
            {
                "VIEW_SCHEMA": "PUBLIC",
                "VIEW_NAME": "TEST_VIEW1",
                "CREATED": "2024-01-01 00:00:00",
                "LAST_ALTERED": "2024-01-01 00:00:00",
                "COMMENT": "Test view comment",
                "VIEW_DEFINITION": "SELECT * FROM test_table",
                "IS_SECURE": "FALSE",
            },
            {
                "VIEW_SCHEMA": "PUBLIC",
                "VIEW_NAME": "TEST_VIEW2",
                "CREATED": "2024-01-01 00:00:00",
                "LAST_ALTERED": "2024-01-01 00:00:00",
                "COMMENT": None,
                "VIEW_DEFINITION": "",  # Empty definition to test hydration
                "IS_SECURE": "TRUE",
            },
        ]

        # Mock the SHOW VIEWS query for hydration
        hydration_cursor = MagicMock()
        hydration_cursor.__iter__.return_value = [
            {
                "name": "TEST_VIEW2",
                "text": "SELECT col1, col2 FROM source_table",
            }
        ]

        # Set up the query mock to return different responses
        def query_side_effect(query: str) -> Any:
            if "information_schema.views" in query:
                return mock_cursor
            elif "SHOW VIEWS" in query:
                return hydration_cursor
            else:
                return MagicMock()

        mock_snowflake_data_dictionary_information_schema.connection.query.side_effect = query_side_effect

        # Test getting views for database
        result = (
            mock_snowflake_data_dictionary_information_schema.get_views_for_database(
                "TEST_DB"
            )
        )

        # Verify the results
        assert result is not None
        assert "PUBLIC" in result
        assert len(result["PUBLIC"]) == 2

        # Check first view
        view1 = result["PUBLIC"][0]
        assert view1.name == "TEST_VIEW1"
        assert view1.view_definition == "SELECT * FROM test_table"
        assert not view1.is_secure

        # Check second view (should have hydrated definition)
        view2 = result["PUBLIC"][1]
        assert view2.name == "TEST_VIEW2"
        assert view2.view_definition == "SELECT col1, col2 FROM source_table"
        assert view2.is_secure

    def test_fetch_views_from_information_schema_disabled(
        self, mock_snowflake_data_dictionary_show_views
    ):
        """Test that fetch_views_from_information_schema=False uses SHOW VIEWS method"""
        # Mock the query response for SHOW VIEWS
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [
            {
                "name": "TEST_VIEW1",
                "schema_name": "PUBLIC",
                "created_on": "2024-01-01 00:00:00",
                "comment": "Test view comment",
                "text": "SELECT * FROM test_table",
                "is_secure": "false",
                "is_materialized": "false",
            }
        ]
        mock_snowflake_data_dictionary_show_views.connection.query.return_value = (
            mock_cursor
        )

        # Test getting views for database
        result = mock_snowflake_data_dictionary_show_views.get_views_for_database(
            "TEST_DB"
        )

        # Verify the results
        assert result is not None
        assert "PUBLIC" in result
        assert len(result["PUBLIC"]) == 1

        view = result["PUBLIC"][0]
        assert view.name == "TEST_VIEW1"
        assert view.view_definition == "SELECT * FROM test_table"
        assert not view.is_secure

    def test_information_schema_fallback_to_show_views(self):
        """Test fallback when information schema query fails"""
        connection = MagicMock(spec=SnowflakeConnection)
        report = MagicMock(spec=SnowflakeV2Report)
        data_dict = SnowflakeDataDictionary(
            connection, report, fetch_views_from_information_schema=True
        )

        # Mock information schema query to fail
        def query_side_effect(query: str) -> Any:
            if "information_schema.views" in query:
                raise Exception("Information schema query returned too much data")
            else:
                return MagicMock()

        cast(MagicMock, data_dict.connection.query).side_effect = query_side_effect

        # Test getting views for database - should return None (fallback handled by schema_gen)
        result = data_dict.get_views_for_database("TEST_DB")
        assert result is None

    def test_maybe_populate_empty_view_definitions(
        self, mock_snowflake_data_dictionary_information_schema
    ):
        """Test the view definition hydration functionality"""
        # Create views with empty definitions
        empty_views = [
            SnowflakeView(
                name="VIEW1",
                view_definition="",
                comment="Test view 1",
                created=datetime(2024, 1, 1),
                last_altered=datetime(2024, 1, 1),
            ),
            SnowflakeView(
                name="VIEW2",
                view_definition=None,
                comment="Test view 2",
                created=datetime(2024, 1, 1),
                last_altered=datetime(2024, 1, 1),
            ),
        ]

        # Mock SHOW VIEWS response for hydration
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [
            {
                "name": "VIEW1",
                "text": "SELECT * FROM table1",
            },
            {
                "name": "VIEW2",
                "text": "SELECT col1 FROM table2",
            },
        ]
        mock_snowflake_data_dictionary_information_schema.connection.query.return_value = mock_cursor

        # Test populating empty view definitions
        result = mock_snowflake_data_dictionary_information_schema._maybe_populate_empty_view_definitions(
            "TEST_DB", "PUBLIC", empty_views
        )

        # Verify definitions were populated
        assert len(result) == 2
        assert result[0].view_definition == "SELECT * FROM table1"
        assert result[1].view_definition == "SELECT col1 FROM table2"

    def test_maybe_populate_empty_view_definitions_covers_all_prefixes(
        self, mock_snowflake_data_dictionary_information_schema
    ):
        """Regression: every prefix group must get its own SHOW VIEWS query.

        The original code took `batch[0]` from each batch returned by
        build_prefix_batches, assuming `max_groups_in_batch=1` meant one
        group per batch. The packer's off-by-one silently let two groups
        land in one batch, so every other prefix range was dropped.
        Construct a workload that forces the packer to pair groups: one
        large group ("A") that occupies a batch alone, and several small
        follow-on groups that fit two-per-batch.
        """
        # 990 names under "A" plus 55 each under B and C. Under the old
        # packer, B and C landed in one batch [[A], [B, C]] and the old
        # batch[0]-only caller silently dropped C. Both fixes prevent that.
        view_names = (
            [f"A_VIEW_{i:04d}" for i in range(990)]
            + [f"B_VIEW_{i:02d}" for i in range(55)]
            + [f"C_VIEW_{i:02d}" for i in range(55)]
        )
        empty_views = [
            SnowflakeView(
                name=name,
                view_definition=None,
                comment=None,
                created=datetime(2024, 1, 1),
                last_altered=datetime(2024, 1, 1),
            )
            for name in view_names
        ]

        executed_queries: list[str] = []

        def query_side_effect(query: str) -> Any:
            executed_queries.append(query)
            # Echo back rows whose name matches the LIKE prefix in this query.
            # Extract the prefix between the single quotes around 'X%'.
            start = query.index("LIKE '") + len("LIKE '")
            end = query.index("%'", start)
            prefix = query[start:end]
            cursor = MagicMock()
            cursor.__iter__.return_value = [
                {"name": n, "text": f"SELECT * FROM src_{n}"}
                for n in view_names
                if n.startswith(prefix)
            ]
            return cursor

        cast(
            MagicMock,
            mock_snowflake_data_dictionary_information_schema.connection.query,
        ).side_effect = query_side_effect

        result = mock_snowflake_data_dictionary_information_schema._maybe_populate_empty_view_definitions(
            "TEST_DB", "PUBLIC", empty_views
        )

        # Every view, including those in the second-of-pair prefix group, must
        # have its definition populated.
        unpopulated = [v.name for v in result if not v.view_definition]
        assert unpopulated == [], (
            f"{len(unpopulated)} views missing definitions; sample={unpopulated[:5]}. "
            f"Executed queries: {executed_queries}"
        )

    def test_get_views_for_schema_using_information_schema(
        self, mock_snowflake_data_dictionary_information_schema
    ):
        """Test schema-level view fetching using information schema"""
        # Mock the query response
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = [
            {
                "VIEW_SCHEMA": "PUBLIC",
                "VIEW_NAME": "SCHEMA_VIEW1",
                "CREATED": "2024-01-01 00:00:00",
                "LAST_ALTERED": "2024-01-01 00:00:00",
                "COMMENT": "Schema view",
                "VIEW_DEFINITION": "SELECT * FROM schema_table",
                "IS_SECURE": "FALSE",
            }
        ]
        mock_snowflake_data_dictionary_information_schema.connection.query.return_value = mock_cursor

        # Test getting views for schema
        result = mock_snowflake_data_dictionary_information_schema.get_views_for_schema_using_information_schema(
            schema_name="PUBLIC", db_name="TEST_DB"
        )

        # Verify the results
        assert len(result) == 1
        assert result[0].name == "SCHEMA_VIEW1"
        assert result[0].view_definition == "SELECT * FROM schema_table"

    @patch(
        "datahub.ingestion.source.snowflake.snowflake_query.SnowflakeQuery.get_views_for_database"
    )
    def test_information_schema_query_construction(
        self, mock_get_views_query, mock_snowflake_data_dictionary_information_schema
    ):
        """Test that the correct information schema query is constructed"""
        mock_get_views_query.return_value = "SELECT * FROM information_schema.views"

        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = []
        mock_snowflake_data_dictionary_information_schema.connection.query.return_value = mock_cursor

        # Call the method
        mock_snowflake_data_dictionary_information_schema._get_views_for_database_using_information_schema(
            "TEST_DB"
        )

        # Verify the correct query method was called
        mock_get_views_query.assert_called_once_with("TEST_DB", "")

    @patch(
        "datahub.ingestion.source.snowflake.snowflake_query.SnowflakeQuery.get_views_for_schema"
    )
    def test_schema_information_schema_query_construction(
        self,
        mock_get_views_schema_query,
        mock_snowflake_data_dictionary_information_schema,
    ):
        """Test that the correct schema-level information schema query is constructed"""
        mock_get_views_schema_query.return_value = (
            "SELECT * FROM information_schema.views WHERE schema='PUBLIC'"
        )

        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = []
        mock_snowflake_data_dictionary_information_schema.connection.query.return_value = mock_cursor

        # Call the method
        mock_snowflake_data_dictionary_information_schema.get_views_for_schema_using_information_schema(
            schema_name="PUBLIC", db_name="TEST_DB"
        )

        # Verify the correct query method was called
        mock_get_views_schema_query.assert_called_once_with(
            db_name="TEST_DB", schema_name="PUBLIC", view_filter=""
        )
