from unittest import mock
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource


def _base_config():
    return {"username": "user", "password": "password", "host_port": "host:1521"}


@patch("datahub.ingestion.source.sql.postgres.create_engine")
def test_initial_database(create_engine_mock):
    config = PostgresConfig.parse_obj(_base_config())
    assert config.initial_database == "postgres"
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())
    assert create_engine_mock.call_count == 1
    assert create_engine_mock.call_args[0][0].endswith("postgres")


@patch("datahub.ingestion.source.sql.postgres.create_engine")
def test_get_inspectors_multiple_databases(create_engine_mock):
    execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
    execute_mock.return_value = [{"datname": "db1"}, {"datname": "db2"}]

    config = PostgresConfig.parse_obj({**_base_config(), "initial_database": "db0"})
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())
    assert create_engine_mock.call_count == 3
    assert create_engine_mock.call_args_list[0][0][0].endswith("db0")
    assert create_engine_mock.call_args_list[1][0][0].endswith("db1")
    assert create_engine_mock.call_args_list[2][0][0].endswith("db2")


@patch("datahub.ingestion.source.sql.postgres.create_engine")
def tests_get_inspectors_with_database_provided(create_engine_mock):
    execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
    execute_mock.return_value = [{"datname": "db1"}, {"datname": "db2"}]

    config = PostgresConfig.parse_obj({**_base_config(), "database": "custom_db"})
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())
    assert create_engine_mock.call_count == 1
    assert create_engine_mock.call_args_list[0][0][0].endswith("custom_db")


@patch("datahub.ingestion.source.sql.postgres.create_engine")
def tests_get_inspectors_with_sqlalchemy_uri_provided(create_engine_mock):
    execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
    execute_mock.return_value = [{"datname": "db1"}, {"datname": "db2"}]

    config = PostgresConfig.parse_obj(
        {**_base_config(), "sqlalchemy_uri": "custom_url"}
    )
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())
    assert create_engine_mock.call_count == 1
    assert create_engine_mock.call_args_list[0][0][0] == "custom_url"


def test_database_in_identifier():
    config = PostgresConfig.parse_obj({**_base_config(), "database": "postgres"})
    mock_inspector = mock.MagicMock()
    assert (
        PostgresSource(config, PipelineContext(run_id="test")).get_identifier(
            schema="superset", entity="logs", inspector=mock_inspector
        )
        == "postgres.superset.logs"
    )


def test_current_sqlalchemy_database_in_identifier():
    config = PostgresConfig.parse_obj({**_base_config()})
    mock_inspector = mock.MagicMock()
    mock_inspector.engine.url.database = "current_db"
    assert (
        PostgresSource(config, PipelineContext(run_id="test")).get_identifier(
            schema="superset", entity="logs", inspector=mock_inspector
        )
        == "current_db.superset.logs"
    )
