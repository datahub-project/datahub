from datetime import datetime
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeDataDictionary,
    SnowflakeView,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
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

    def test_get_views_for_schema_using_information_schema_query_failure_raises_runtime_error(
        self, mock_snowflake_data_dictionary_information_schema
    ):
        """Query failure in get_views_for_schema_using_information_schema is wrapped in RuntimeError."""
        original_error = Exception("SQL compilation error")
        mock_snowflake_data_dictionary_information_schema.connection.query.side_effect = original_error

        with pytest.raises(RuntimeError) as exc_info:
            mock_snowflake_data_dictionary_information_schema.get_views_for_schema_using_information_schema(
                schema_name="MY_SCHEMA", db_name="MY_DB"
            )

        assert "MY_DB.MY_SCHEMA" in str(exc_info.value)
        assert exc_info.value.__cause__ is original_error

    def test_show_views_pagination_marker_is_qualified(self):
        """When SHOW VIEWS returns a full page, the pagination cursor uses schema_name.view_name."""
        connection = MagicMock(spec=SnowflakeConnection)
        report = MagicMock(spec=SnowflakeV2Report)
        data_dict = SnowflakeDataDictionary(
            connection, report, fetch_views_from_information_schema=False
        )

        page_size = 3
        # First page: exactly page_size rows across two schemas → triggers pagination
        first_page = [
            {
                "name": f"VIEW_{i}",
                "schema_name": "SCHEMA_A" if i < 2 else "SCHEMA_B",
                "created_on": datetime(2024, 1, 1),
                "comment": None,
                "text": "SELECT 1",
                "is_secure": "false",
                "is_materialized": "false",
            }
            for i in range(page_size)
        ]
        # Second page: empty → stops pagination
        second_page: list = []

        call_count = 0

        def query_side_effect(_: str) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return iter(first_page)
            return iter(second_page)

        connection.query.side_effect = query_side_effect

        with patch(
            "datahub.ingestion.source.snowflake.snowflake_schema.SHOW_COMMAND_MAX_PAGE_SIZE",
            page_size,
        ):
            result = data_dict._get_views_for_database_using_show("TEST_DB")

        assert call_count == 2
        # Second call must use the qualified marker "SCHEMA_B.VIEW_2"
        second_call_query: str = connection.query.call_args_list[1][0][0]
        assert "SCHEMA_B.VIEW_2" in second_call_query

        # All views from the first page are present in the result
        assert len(result["SCHEMA_A"]) == 2
        assert len(result["SCHEMA_B"]) == 1


class TestSnowflakeQueryStrings:
    def test_show_external_tables_without_db_name_queries_account(self):
        query = SnowflakeQuery.show_external_tables()
        assert query == "show external tables in account"

    def test_show_external_tables_with_db_name_scopes_to_database(self):
        query = SnowflakeQuery.show_external_tables(db_name="MY_DB")
        assert 'in database "MY_DB"' in query
        assert "account" not in query

    def test_show_external_tables_different_databases_produce_different_queries(self):
        q1 = SnowflakeQuery.show_external_tables(db_name="DB_ONE")
        q2 = SnowflakeQuery.show_external_tables(db_name="DB_TWO")
        assert q1 != q2
        assert "DB_ONE" in q1
        assert "DB_TWO" in q2


def _make_schema_gen() -> SnowflakeSchemaGenerator:
    config = SnowflakeV2Config.model_validate(
        {"account_id": "test_account", "username": "test_user", "password": "test_pass"}
    )
    report = SnowflakeV2Report()
    connection = MagicMock()
    identifiers = MagicMock()
    identifiers.get_dataset_identifier.side_effect = (
        lambda name, schema, db: f"{db}.{schema}.{name}".lower()
    )
    identifiers.gen_dataset_urn.side_effect = (
        lambda key: f"urn:li:dataset:(urn:li:dataPlatform:snowflake,{key},PROD)"
    )
    return SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=connection,
        filters=MagicMock(),
        identifiers=identifiers,
        domain_registry=None,
        profiler=None,
        aggregator=None,
        snowsight_url_builder=None,
    )


def _make_db(name: str) -> SnowflakeDatabase:
    return SnowflakeDatabase(name=name, created=None, comment=None)


class TestExternalTablesDdlLineage:
    def test_queries_per_database_not_account_wide(self):
        """_external_tables_ddl_lineage issues one SHOW EXTERNAL TABLES per database."""
        gen = _make_schema_gen()
        gen.databases = [_make_db("DB_A"), _make_db("DB_B")]
        mock_conn = cast(MagicMock, gen.connection)
        mock_conn.query.return_value = iter([])

        list(gen._external_tables_ddl_lineage(discovered_tables=[]))

        queried = [call.args[0] for call in mock_conn.query.call_args_list]
        assert SnowflakeQuery.show_external_tables(db_name="DB_A") in queried
        assert SnowflakeQuery.show_external_tables(db_name="DB_B") in queried
        assert SnowflakeQuery.show_external_tables() not in queried

    def test_failure_in_one_db_does_not_stop_other_dbs(self):
        """A query error for one database emits a warning but continues to the next."""
        gen = _make_schema_gen()
        gen.databases = [_make_db("DB_FAIL"), _make_db("DB_OK")]

        s3_row = {
            "name": "EXT_TBL",
            "schema_name": "MY_SCHEMA",
            "database_name": "DB_OK",
            "location": "s3://bucket/path/",
        }

        def query_side_effect(_: str) -> Any:
            if "DB_FAIL" in _:
                raise Exception("permission denied")
            return iter([s3_row])

        cast(MagicMock, gen.connection).query.side_effect = query_side_effect
        cast(MagicMock, gen.identifiers).get_dataset_identifier.side_effect = (
            lambda name, schema, db: f"{db}.{schema}.{name}".lower()
        )

        results = list(
            gen._external_tables_ddl_lineage(
                discovered_tables=["db_ok.my_schema.ext_tbl"]
            )
        )

        # DB_OK lineage is still emitted despite DB_FAIL erroring
        assert len(results) == 1
        assert "db_ok" in results[0].downstream_urn.lower()

        # A warning was reported for the failing database
        warnings = gen.report.warnings.as_obj()
        assert any("DB_FAIL" in str(w) for w in warnings)
