from datetime import datetime, timezone
from unittest import mock

import pytest
from freezegun import freeze_time
from pytest import fixture

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline, PipelineInitError
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake import snowflake_query
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from tests.integration.snowflake.common import FROZEN_TIME, default_query_results


def query_permission_error_override(fn, override_for_query, error_msg):
    def my_function(query):
        if query in override_for_query:
            raise Exception(error_msg)
        else:
            return fn(query)

    return my_function


def query_permission_response_override(fn, override_for_query, response):
    def my_function(query):
        if query in override_for_query:
            return response
        else:
            return fn(query)

    return my_function


@fixture(scope="function")
def snowflake_pipeline_config(tmp_path):
    output_file = tmp_path / "snowflake_test_events_permission_error.json"
    config = PipelineConfig(
        source=SourceConfig(
            type="snowflake",
            config=SnowflakeV2Config(
                account_id="ABC12345.ap-south-1.aws",
                username="TST_USR",
                password="TST_PWD",
                role="TEST_ROLE",
                warehouse="TEST_WAREHOUSE",
                include_technical_schema=True,
                match_fully_qualified_names=True,
                schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
                include_usage_stats=False,
                start_time=datetime(2022, 6, 6, 0, 0, 0, 0).replace(
                    tzinfo=timezone.utc
                ),
                end_time=datetime(2022, 6, 7, 7, 17, 0, 0).replace(tzinfo=timezone.utc),
            ),
        ),
        sink=DynamicTypedConfig(type="file", config={"filename": str(output_file)}),
    )
    return config


@freeze_time(FROZEN_TIME)
def test_snowflake_missing_role_access_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        # Snowflake connection fails role not granted error
        mock_connect.side_effect = Exception(
            "250001 (08001): Failed to connect to DB: abc12345.ap-south-1.snowflakecomputing.com:443. Role 'TEST_ROLE' specified in the connect string is not granted to this user. Contact your local system administrator, or attempt to login with another role, e.g. PUBLIC"
        )

        with pytest.raises(PipelineInitError, match="Permissions error"):
            pipeline = Pipeline(snowflake_pipeline_config)
            pipeline.run()
            pipeline.raise_from_status()


@freeze_time(FROZEN_TIME)
def test_snowflake_missing_warehouse_access_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Current warehouse query leads to blank result
        sf_cursor.execute.side_effect = query_permission_response_override(
            default_query_results,
            [SnowflakeQuery.current_warehouse()],
            [(None,)],
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()
        assert "permission-error" in [
            failure.message for failure in pipeline.source.get_report().failures
        ]


@freeze_time(FROZEN_TIME)
def test_snowflake_no_databases_with_access_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Error in listing databases
        sf_cursor.execute.side_effect = query_permission_error_override(
            default_query_results,
            [SnowflakeQuery.get_databases("TEST_DB")],
            "Database 'TEST_DB' does not exist or not authorized.",
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()
        assert "permission-error" in [
            failure.message for failure in pipeline.source.get_report().failures
        ]


@freeze_time(FROZEN_TIME)
def test_snowflake_no_tables_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Error in listing databases
        no_tables_fn = query_permission_response_override(
            default_query_results,
            [SnowflakeQuery.tables_for_schema("TEST_SCHEMA", "TEST_DB")],
            [],
        )
        no_views_fn = query_permission_response_override(
            no_tables_fn,
            [SnowflakeQuery.show_views_for_database("TEST_DB")],
            [],
        )
        sf_cursor.execute.side_effect = query_permission_response_override(
            no_views_fn,
            [SnowflakeQuery.streams_for_database("TEST_DB")],
            [],
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()
        assert "permission-error" in [
            failure.message for failure in pipeline.source.get_report().failures
        ]


@freeze_time(FROZEN_TIME)
def test_snowflake_list_columns_error_causes_pipeline_warning(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Error in listing columns
        sf_cursor.execute.side_effect = query_permission_error_override(
            default_query_results,
            [
                SnowflakeQuery.columns_for_schema("TEST_SCHEMA", "TEST_DB"),
            ],
            "Database 'TEST_DB' does not exist or not authorized.",
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()  # pipeline should not fail
        assert "Failed to get columns for table" in [
            warning.message for warning in pipeline.source.get_report().warnings
        ]


@freeze_time(FROZEN_TIME)
def test_snowflake_list_primary_keys_error_causes_pipeline_warning(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Error in listing keys leads to warning
        sf_cursor.execute.side_effect = query_permission_error_override(
            default_query_results,
            [SnowflakeQuery.show_primary_keys_for_schema("TEST_SCHEMA", "TEST_DB")],
            "Insufficient privileges to operate on TEST_DB",
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()  # pipeline should not fail
        assert "Failed to get primary key for table" in [
            warning.message for warning in pipeline.source.get_report().warnings
        ]


@freeze_time(FROZEN_TIME)
def test_snowflake_missing_snowflake_lineage_permission_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Error in getting lineage
        sf_cursor.execute.side_effect = query_permission_error_override(
            default_query_results,
            [
                snowflake_query.SnowflakeQuery.table_to_table_lineage_history_v2(
                    start_time_millis=1654473600000,
                    end_time_millis=1654586220000,
                    include_column_lineage=True,
                )
            ],
            "Database 'SNOWFLAKE' does not exist or not authorized.",
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()
        assert "lineage-permission-error" in [
            failure.message for failure in pipeline.source.get_report().failures
        ]


@freeze_time(FROZEN_TIME)
def test_snowflake_missing_snowflake_operations_permission_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Error in getting access history date range
        sf_cursor.execute.side_effect = query_permission_error_override(
            default_query_results,
            [snowflake_query.SnowflakeQuery.get_access_history_date_range()],
            "Database 'SNOWFLAKE' does not exist or not authorized.",
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()
        assert "usage-permission-error" in [
            failure.message for failure in pipeline.source.get_report().failures
        ]


@freeze_time(FROZEN_TIME)
def test_snowflake_missing_snowflake_secure_view_definitions_raises_pipeline_info(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Empty secure view definitions
        sf_cursor.execute.side_effect = query_permission_response_override(
            default_query_results,
            [snowflake_query.SnowflakeQuery.get_secure_view_definitions()],
            [],
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()

        pipeline.raise_from_status(raise_warnings=True)
        assert pipeline.source.get_report().infos.as_obj() == [
            {
                "title": "Secure view definition not found",
                "message": "Lineage will be missing for the view.",
                "context": ["TEST_DB.TEST_SCHEMA.VIEW_1"],
            }
        ]


@freeze_time(FROZEN_TIME)
def test_snowflake_failed_secure_view_definitions_query_raises_pipeline_warning(
    pytestconfig,
    snowflake_pipeline_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Error in getting secure view definitions
        sf_cursor.execute.side_effect = query_permission_error_override(
            default_query_results,
            [snowflake_query.SnowflakeQuery.get_secure_view_definitions()],
            "Database 'SNOWFLAKE' does not exist or not authorized.",
        )
        pipeline = Pipeline(snowflake_pipeline_config)
        pipeline.run()
        assert pipeline.source.get_report().warnings.as_obj() == [
            {
                "title": "Failed to get secure views definitions",
                "message": "Lineage will be missing for the view. Please check permissions.",
                "context": [
                    "TEST_DB.TEST_SCHEMA.VIEW_1 <class 'datahub.ingestion.source.snowflake.snowflake_connection.SnowflakePermissionError'>: Database 'SNOWFLAKE' does not exist or not authorized."
                ],
            }
        ]
