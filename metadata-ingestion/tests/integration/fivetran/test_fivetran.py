import datetime
from functools import partial
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryCredential
from datahub.ingestion.source.fivetran.config import (
    BigQueryDestinationConfig,
    FivetranSourceConfig,
    SnowflakeDestinationConfig,
)
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-06-07 17:00:00"

default_connector_query_results = [
    {
        "connector_id": "calendar_elected",
        "connecting_user_id": "reapply_phone",
        "connector_type_id": "postgres",
        "connector_name": "postgres",
        "paused": False,
        "sync_frequency": 1440,
        "destination_id": "interval_unconstitutional",
    },
]


def default_query_results(
    query, connector_query_results=default_connector_query_results
):
    fivetran_log_query = FivetranLogQuery()
    fivetran_log_query.set_db("test")
    if query == fivetran_log_query.use_database("test_database"):
        return []
    elif query == fivetran_log_query.get_connectors_query():
        return connector_query_results
    elif query == fivetran_log_query.get_table_lineage_query():
        return [
            {
                "connector_id": "calendar_elected",
                "source_table_id": "10040",
                "source_table_name": "employee",
                "source_schema_name": "public",
                "destination_table_id": "7779",
                "destination_table_name": "employee",
                "destination_schema_name": "postgres_public",
            },
            {
                "connector_id": "calendar_elected",
                "source_table_id": "10041",
                "source_table_name": "company",
                "source_schema_name": "public",
                "destination_table_id": "7780",
                "destination_table_name": "company",
                "destination_schema_name": "postgres_public",
            },
        ]
    elif query == fivetran_log_query.get_column_lineage_query():
        return [
            {
                "source_table_id": "10040",
                "destination_table_id": "7779",
                "source_column_name": "id",
                "destination_column_name": "id",
            },
            {
                "source_table_id": "10040",
                "destination_table_id": "7779",
                "source_column_name": "name",
                "destination_column_name": "name",
            },
            {
                "source_table_id": "10041",
                "destination_table_id": "7780",
                "source_column_name": "id",
                "destination_column_name": "id",
            },
            {
                "source_table_id": "10041",
                "destination_table_id": "7780",
                "source_column_name": "name",
                "destination_column_name": "name",
            },
        ]
    elif query == fivetran_log_query.get_users_query():
        return [
            {
                "user_id": "reapply_phone",
                "given_name": "Shubham",
                "family_name": "Jagtap",
                "email": "abc.xyz@email.com",
            }
        ]
    elif query == fivetran_log_query.get_sync_logs_query(
        syncs_interval=7,
        connector_ids=["calendar_elected"],
    ):
        return [
            {
                "connector_id": "calendar_elected",
                "sync_id": "4c9a03d6-eded-4422-a46a-163266e58243",
                "start_time": datetime.datetime(2023, 9, 20, 6, 37, 32, 606000),
                "end_time": datetime.datetime(2023, 9, 20, 6, 38, 5, 56000),
                "end_message_data": '"{\\"status\\":\\"SUCCESSFUL\\"}"',
            },
            {
                "connector_id": "calendar_elected",
                "sync_id": "f773d1e9-c791-48f4-894f-8cf9b3dfc834",
                "start_time": datetime.datetime(2023, 10, 3, 14, 35, 30, 345000),
                "end_time": datetime.datetime(2023, 10, 3, 14, 35, 31, 512000),
                "end_message_data": '"{\\"reason\\":\\"Sync has been cancelled because of a user action in the dashboard.Standard Config updated.\\",\\"status\\":\\"CANCELED\\"}"',
            },
            {
                "connector_id": "calendar_elected",
                "sync_id": "63c2fc85-600b-455f-9ba0-f576522465be",
                "start_time": datetime.datetime(2023, 10, 3, 14, 35, 55, 401000),
                "end_time": datetime.datetime(2023, 10, 3, 14, 36, 29, 678000),
                "end_message_data": '"{\\"reason\\":\\"java.lang.RuntimeException: FATAL: too many connections for role \\\\\\"hxwraqld\\\\\\"\\",\\"taskType\\":\\"reconnect\\",\\"status\\":\\"FAILURE_WITH_TASK\\"}"',
            },
        ]
    # Unreachable code
    raise Exception(f"Unknown query {query}")


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_fivetran_with_snowflake_dest(pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/fivetran"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "fivetran_test_events.json"
    golden_file = test_resources_dir / "fivetran_snowflake_golden.json"

    with mock.patch(
        "datahub.ingestion.source.fivetran.fivetran_log_api.create_engine"
    ) as mock_create_engine:
        connection_magic_mock = MagicMock()
        connection_magic_mock.execute.side_effect = default_query_results

        mock_create_engine.return_value = connection_magic_mock

        pipeline = Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "fivetran",
                    "config": {
                        "fivetran_log_config": {
                            "destination_platform": "snowflake",
                            "snowflake_destination_config": {
                                "account_id": "testid",
                                "warehouse": "test_wh",
                                "username": "test",
                                "password": "test@123",
                                "database": "test_database",
                                "role": "testrole",
                                "log_schema": "test",
                            },
                        },
                        "connector_patterns": {
                            "allow": [
                                "postgres",
                            ]
                        },
                        "destination_patterns": {
                            "allow": [
                                "interval_unconstitutional",
                            ]
                        },
                        "sources_to_database": {
                            "calendar_elected": "postgres_db",
                        },
                        "sources_to_platform_instance": {
                            "calendar_elected": {
                                "env": "DEV",
                            }
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{output_file}",
                    },
                },
            }
        )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{output_file}",
        golden_path=f"{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_fivetran_with_snowflake_dest_and_null_connector_user(pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/fivetran"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "fivetran_test_events.json"
    golden_file = (
        test_resources_dir / "fivetran_snowflake_empty_connection_user_golden.json"
    )

    with mock.patch(
        "datahub.ingestion.source.fivetran.fivetran_log_api.create_engine"
    ) as mock_create_engine:
        connection_magic_mock = MagicMock()

        connector_query_results = [
            {
                "connector_id": "calendar_elected",
                "connecting_user_id": None,
                "connector_type_id": "postgres",
                "connector_name": "postgres",
                "paused": False,
                "sync_frequency": 1440,
                "destination_id": "interval_unconstitutional",
            },
        ]

        connection_magic_mock.execute.side_effect = partial(
            default_query_results, connector_query_results=connector_query_results
        )

        mock_create_engine.return_value = connection_magic_mock

        pipeline = Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "fivetran",
                    "config": {
                        "fivetran_log_config": {
                            "destination_platform": "snowflake",
                            "snowflake_destination_config": {
                                "account_id": "testid",
                                "warehouse": "test_wh",
                                "username": "test",
                                "password": "test@123",
                                "database": "test_database",
                                "role": "testrole",
                                "log_schema": "test",
                            },
                        },
                        "connector_patterns": {
                            "allow": [
                                "postgres",
                            ]
                        },
                        "destination_patterns": {
                            "allow": [
                                "interval_unconstitutional",
                            ]
                        },
                        "sources_to_database": {
                            "calendar_elected": "postgres_db",
                        },
                        "sources_to_platform_instance": {
                            "calendar_elected": {
                                "env": "DEV",
                            }
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{output_file}",
                    },
                },
            }
        )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{output_file}",
        golden_path=f"{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_fivetran_bigquery_config():
    with mock.patch("datahub.ingestion.source.fivetran.fivetran_log_api.create_engine"):
        # Simply test that the config is parsed and the source is initialized without an error.
        assert FivetranSource.create(
            {
                "fivetran_log_config": {
                    "destination_platform": "bigquery",
                    "bigquery_destination_config": {
                        "credential": {
                            "private_key_id": "testprivatekey",
                            "project_id": "test-project",
                            "client_email": "fivetran-connector@test-project.iam.gserviceaccount.com",
                            "client_id": "1234567",
                            "private_key": "private-key",
                        },
                        "dataset": "test",
                    },
                },
            },
            ctx=PipelineContext(run_id="fivetran-bq-dummy"),
        )


@freeze_time(FROZEN_TIME)
def test_fivetran_snowflake_destination_config():
    snowflake_dest = SnowflakeDestinationConfig(
        account_id="TESTID",
        warehouse="TEST_WH",
        username="test",
        password="test@123",
        database="TEST_DATABASE",
        role="TESTROLE",
        log_schema="TEST_SCHEMA",
    )
    assert (
        snowflake_dest.get_sql_alchemy_url()
        == "snowflake://test:test%40123@TESTID?application=acryl_datahub&authenticator=SNOWFLAKE&role=TESTROLE&warehouse=TEST_WH"
    )


@freeze_time(FROZEN_TIME)
def test_fivetran_bigquery_destination_config():
    bigquery_dest = BigQueryDestinationConfig(
        credential=BigQueryCredential(
            private_key_id="testprivatekey",
            project_id="test-project",
            client_email="fivetran-connector@test-project.iam.gserviceaccount.com",
            client_id="1234567",
            private_key="private-key",
        ),
        dataset="test_dataset",
    )
    assert bigquery_dest.get_sql_alchemy_url() == "bigquery://"


@freeze_time(FROZEN_TIME)
def test_rename_destination_config():
    config_dict = {
        "fivetran_log_config": {
            "destination_platform": "snowflake",
            "destination_config": {
                "account_id": "testid",
                "database": "test_database",
                "log_schema": "test",
            },
        },
    }
    with pytest.warns(
        ConfigurationWarning,
        match="destination_config is deprecated, please use snowflake_destination_config instead.",
    ):
        FivetranSourceConfig.parse_obj(config_dict)
