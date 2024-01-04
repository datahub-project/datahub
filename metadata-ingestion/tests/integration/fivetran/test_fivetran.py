import datetime
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.fivetran.config import (
    BigQueryDestinationConfig,
    FivetranSourceConfig,
    SnowflakeDestinationConfig,
)
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery
from datahub.ingestion.source_config.usage.bigquery_usage import BigQueryCredential
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-06-07 17:00:00"


def default_query_results(query):
    fivetran_log_query = FivetranLogQuery()
    fivetran_log_query.set_db("test")
    if query == fivetran_log_query.use_database("test_database"):
        return []
    elif query == fivetran_log_query.get_connectors_query():
        return [
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
    elif query == fivetran_log_query.get_table_lineage_query("calendar_elected"):
        return [
            {
                "source_table_id": "10040",
                "source_table_name": "employee",
                "source_schema_name": "public",
                "destination_table_id": "7779",
                "destination_table_name": "employee",
                "destination_schema_name": "postgres_public",
            },
            {
                "source_table_id": "10041",
                "source_table_name": "company",
                "source_schema_name": "public",
                "destination_table_id": "7780",
                "destination_table_name": "company",
                "destination_schema_name": "postgres_public",
            },
        ]
    elif query == fivetran_log_query.get_column_lineage_query(
        "10040", "7779"
    ) or query == fivetran_log_query.get_column_lineage_query("10041", "7780"):
        return [
            {
                "source_column_name": "id",
                "destination_column_name": "id",
            },
            {
                "source_column_name": "name",
                "destination_column_name": "name",
            },
        ]
    elif query == fivetran_log_query.get_user_query("reapply_phone"):
        return [
            {
                "user_id": "reapply_phone",
                "given_name": "Shubham",
                "family_name": "Jagtap",
            }
        ]
    elif query == fivetran_log_query.get_sync_start_logs_query("calendar_elected"):
        return [
            {
                "time_stamp": datetime.datetime(2023, 9, 20, 6, 37, 32, 606000),
                "sync_id": "4c9a03d6-eded-4422-a46a-163266e58243",
            },
            {
                "time_stamp": datetime.datetime(2023, 10, 3, 14, 35, 30, 345000),
                "sync_id": "f773d1e9-c791-48f4-894f-8cf9b3dfc834",
            },
            {
                "time_stamp": datetime.datetime(2023, 10, 3, 14, 35, 55, 401000),
                "sync_id": "63c2fc85-600b-455f-9ba0-f576522465be",
            },
        ]
    elif query == fivetran_log_query.get_sync_end_logs_query("calendar_elected"):
        return [
            {
                "time_stamp": datetime.datetime(2023, 9, 20, 6, 38, 5, 56000),
                "sync_id": "4c9a03d6-eded-4422-a46a-163266e58243",
                "message_data": '"{\\"status\\":\\"SUCCESSFUL\\"}"',
            },
            {
                "time_stamp": datetime.datetime(2023, 10, 3, 14, 35, 31, 512000),
                "sync_id": "f773d1e9-c791-48f4-894f-8cf9b3dfc834",
                "message_data": '"{\\"reason\\":\\"Sync has been cancelled because of a user action in the dashboard.Standard Config updated.\\",\\"status\\":\\"CANCELED\\"}"',
            },
            {
                "time_stamp": datetime.datetime(2023, 10, 3, 14, 36, 29, 678000),
                "sync_id": "63c2fc85-600b-455f-9ba0-f576522465be",
                "message_data": '"{\\"reason\\":\\"java.lang.RuntimeException: FATAL: too many connections for role \\\\\\"hxwraqld\\\\\\"\\",\\"taskType\\":\\"reconnect\\",\\"status\\":\\"FAILURE_WITH_TASK\\"}"',
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
def test_fivetran_with_bigquery_dest(pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/fivetran"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "fivetran_test_events.json"
    golden_file = test_resources_dir / "fivetran_bigquery_golden.json"

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
                            "destination_platform": "bigquery",
                            "bigquery_destination_config": {
                                "credential": {
                                    "private_key_id": "3b76ebdbbe3ee53f9851930e8a974c43cf118b0e",
                                    "project_id": "harshal-playground-306419",
                                    "client_email": "fivetran-connector@harshal-playground-306419.iam.gserviceaccount.com",
                                    "client_id": "117962137286607512764",
                                    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCtLSE4hpsvigMq\nVGevfg+ZU60O1JYQ+PbkVXxgCLRwL48vzavphgQ/0L8F0u1X3daHiXAgqpID0Y8H\nAbV6u0igfADVNNngyQZ5XmDdMgSQ0B3xuVH4xOs6YodyaDbhRm2fpEf04tw5LzMG\nzGbcDlmXUu0gxl/D3fPbZ6Uo2o2yrh+3N8M5OptOuNbybFrjzi81wY/ZfRJ/lU8F\nQusTBAEN8uYXZ4nrPOvASeGDSgKpcjSuzmRRDk5d95P6YUvFut8bVO5DOVRJibC8\neVfKm8qZlNkWpeUtd8QUQLQ3QfPsAeBDpv0f3svNlZomw+jBBnQ+KQf7X2DXnfrf\ncBk51UCBAgMBAAECggEAKREyNxros0HZ1VB11C6jWVt4BzPPsI+x0N0a5RU7YP1O\nSepOsc7HtDB3FFT8VuFzLeogTobUoJYyAlnb5RKDuxcdBXKGlqFlaSPB+cKSRZss\n9rVZZJKiK9LQxwdCuHKAraKCM+cWjXMNeTfAzr+SbGnMdQDGFgdM/INcLc+CWdQ/\nv+JK5oF/RCzfTLueepL8P8Jw9ka/g+r5bwDFNx75PJAOXaK1pKOKuDzied/wP/jh\nU/xX9G5EaGnR2W0IU88euNl/7uf6948AxB4heNmV5D+1+EjldFpl9Yn1kQqlupMZ\nNzdAyA17xMSoITN7zaKkop0sshW555LEaSji4ix9DwKBgQDnUwNcGSok3L6gw9gB\nU/2y/0r1N+TARPzHt9sWrODlc6eR/aUPWp0+uYAdEyMY1h4Lw07sckQyGagZotJg\nlcHCrZzSw8DiVCS+b4nvVM04vO1HH7Hmi1WpcGOF8OvNwoObeGuOujoaLpUDdh6H\n8lr7XUOV4tVbdk5D4tZBD8AQywKBgQC/pjc05aoXwMCopw6msnSl+Tm0C0J0uhCU\nP++0LnVla+d57OvtEIJuoDkuO9BEulSO92ExkkX1P/dhlJ0hQE5UHN7tZO97epAb\nKEt8NeDGjwUtVk269k7FY+E29JtmuHTw1sms63lkTjVPHPjwz+Giz+JA1P0YQqd0\nC60bkz4GYwKBgQCVKuXOvPd9P8+5cbDX4maFx7R5fqqHBbWPwFKKQ77el37mWI7Y\n4NDngs1HcnC+ckx7v10kgebR+9N3fJR27fs9Y6DuFIdMK4BqQRjMeX3A6auEor6g\ncRczJSM4Wwxj6dveJIAPUK1o9sLmsYOsnK7oytJrVBPLX/XGNv98gwCSkwKBgF+i\nEsRbomAN+FVwX3hBbsHvtqKoR7Y1rtY7fWK9GCnPCYgm+KeoaF3JqB7xDAFvtYhN\n23tAEZbLH6fa3/nau/tvGLL/fh1w0H4swlYuEIckkSHJgio8hJwtNhIVR4KfowMX\nOzYFnuK1wBZFT9fi9CYpdC6D2fQENslKlkw/UmEfAoGBAN7pJxXRF7kgpjBVrf3h\nMSsOOtHiOI+aVVg74zZsOvAXYZpHMRG1Oeqi8OapjVA4d9I65G1yrk84PIII9xsx\nabqMYIWvInyL4mpIRRSanKLzrrD+6SpR/GPk6+Fh46guQaFm6FwVYT8kWceA+MJI\nAjkZAB2pZpLeBZRWATZ5vdjQ\n-----END PRIVATE KEY-----\n",
                                },
                                "dataset": "test",
                            },
                        },
                        "connector_patterns": {
                            "allow": [
                                "postgres",
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
