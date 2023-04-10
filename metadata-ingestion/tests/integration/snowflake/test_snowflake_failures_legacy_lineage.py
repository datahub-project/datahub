from datetime import datetime, timezone
from typing import cast
from unittest import mock

from freezegun import freeze_time
from pytest import fixture

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake import snowflake_query
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from tests.integration.snowflake.common import (
    FROZEN_TIME,
    NUM_TABLES,
    default_query_results,
)


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
def snowflake_pipeline_legacy_lineage_config(tmp_path):
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
                include_view_lineage=False,
                include_usage_stats=False,
                use_legacy_lineage_method=True,
                start_time=datetime(2022, 6, 6, 7, 17, 0, 0).replace(
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
    snowflake_pipeline_legacy_lineage_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        # Snowflake connection fails role not granted error
        mock_connect.side_effect = Exception(
            "250001 (08001): Failed to connect to DB: abc12345.ap-south-1.snowflakecomputing.com:443. Role 'TEST_ROLE' specified in the connect string is not granted to this user. Contact your local system administrator, or attempt to login with another role, e.g. PUBLIC"
        )

        pipeline = Pipeline(snowflake_pipeline_legacy_lineage_config)
        pipeline.run()
        assert "permission-error" in pipeline.source.get_report().failures.keys()


@freeze_time(FROZEN_TIME)
def test_snowflake_missing_warehouse_access_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_legacy_lineage_config,
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
        pipeline = Pipeline(snowflake_pipeline_legacy_lineage_config)
        pipeline.run()
        assert "permission-error" in pipeline.source.get_report().failures.keys()


@freeze_time(FROZEN_TIME)
def test_snowflake_no_databases_with_access_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_legacy_lineage_config,
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
        pipeline = Pipeline(snowflake_pipeline_legacy_lineage_config)
        pipeline.run()
        assert "permission-error" in pipeline.source.get_report().failures.keys()


@freeze_time(FROZEN_TIME)
def test_snowflake_no_tables_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_legacy_lineage_config,
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
        sf_cursor.execute.side_effect = query_permission_response_override(
            no_tables_fn,
            [SnowflakeQuery.show_views_for_schema("TEST_SCHEMA", "TEST_DB")],
            [],
        )

        pipeline = Pipeline(snowflake_pipeline_legacy_lineage_config)
        pipeline.run()
        assert "permission-error" in pipeline.source.get_report().failures.keys()


@freeze_time(FROZEN_TIME)
def test_snowflake_list_columns_error_causes_pipeline_warning(
    pytestconfig,
    snowflake_pipeline_legacy_lineage_config,
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
                SnowflakeQuery.columns_for_table(
                    "TABLE_{}".format(tbl_idx), "TEST_SCHEMA", "TEST_DB"
                )
                for tbl_idx in range(1, NUM_TABLES + 1)
            ],
            "Database 'TEST_DB' does not exist or not authorized.",
        )
        pipeline = Pipeline(snowflake_pipeline_legacy_lineage_config)
        pipeline.run()
        pipeline.raise_from_status()  # pipeline should not fail
        assert (
            "Failed to get columns for table"
            in pipeline.source.get_report().warnings.keys()
        )


@freeze_time(FROZEN_TIME)
def test_snowflake_list_primary_keys_error_causes_pipeline_warning(
    pytestconfig,
    snowflake_pipeline_legacy_lineage_config,
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
        pipeline = Pipeline(snowflake_pipeline_legacy_lineage_config)
        pipeline.run()
        pipeline.raise_from_status()  # pipeline should not fail
        assert (
            "Failed to get primary key for table"
            in pipeline.source.get_report().warnings.keys()
        )


@freeze_time(FROZEN_TIME)
def test_snowflake_missing_snowflake_lineage_permission_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_legacy_lineage_config,
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
                snowflake_query.SnowflakeQuery.table_to_table_lineage_history(
                    1654499820000, 1654586220000, True
                ),
            ],
            "Database 'SNOWFLAKE' does not exist or not authorized.",
        )
        pipeline = Pipeline(snowflake_pipeline_legacy_lineage_config)
        pipeline.run()
        assert (
            "lineage-permission-error" in pipeline.source.get_report().failures.keys()
        )


@freeze_time(FROZEN_TIME)
def test_snowflake_missing_snowflake_operations_permission_causes_pipeline_failure(
    pytestconfig,
    snowflake_pipeline_legacy_lineage_config,
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
        pipeline = Pipeline(snowflake_pipeline_legacy_lineage_config)
        pipeline.run()
        assert "usage-permission-error" in pipeline.source.get_report().failures.keys()


@freeze_time(FROZEN_TIME)
def test_snowflake_unexpected_snowflake_view_lineage_error_causes_pipeline_warning(
    pytestconfig,
    snowflake_pipeline_legacy_lineage_config,
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor

        # Error in getting view lineage
        sf_cursor.execute.side_effect = query_permission_error_override(
            default_query_results,
            [snowflake_query.SnowflakeQuery.view_dependencies()],
            "Unexpected Error",
        )

        snowflake_pipeline_config1 = snowflake_pipeline_legacy_lineage_config.copy()
        cast(
            SnowflakeV2Config,
            cast(PipelineConfig, snowflake_pipeline_config1).source.config,
        ).include_view_lineage = True
        pipeline = Pipeline(snowflake_pipeline_config1)
        pipeline.run()
        pipeline.raise_from_status()  # pipeline should not fail
        assert "view-upstream-lineage" in pipeline.source.get_report().warnings.keys()
