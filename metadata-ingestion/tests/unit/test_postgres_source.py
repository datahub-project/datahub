from unittest import mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource


def _base_config():
    return {"username": "user", "password": "password", "host_port": "host:1521"}


def test_default_database():
    config = PostgresConfig.parse_obj(_base_config())
    assert config.database == "postgres"

    config = PostgresConfig.parse_obj({**_base_config(), "database": "my_database"})
    assert config.database == "my_database"


def test_ingest_multiple_databases_with_sqlachemy_uri():
    config = PostgresConfig.parse_obj(
        {
            **_base_config(),
            "ingest_multiple_databases": True,
        }
    )
    assert config.ingest_multiple_databases is True

    config = PostgresConfig.parse_obj(
        {
            **_base_config(),
            "sqlalchemy_uri": "postgresql://user:password@host:1521/postgres",
            "ingest_multiple_databases": False,
        }
    )
    assert config.sqlalchemy_uri == "postgresql://user:password@host:1521/postgres"

    with pytest.raises(ValueError):
        PostgresConfig.parse_obj(
            {
                **_base_config(),
                "ingest_multiple_databases": True,
                "sqlachemy_uri": "postgresql://user:password@host:1521/postgres",
            }
        )


def test_database_alias_takes_precendence():
    config = PostgresConfig.parse_obj(
        {
            **_base_config(),
            "database_alias": "ops_database",
            "database": "postgres",
        }
    )
    mock_inspector = mock.MagicMock()
    assert (
        PostgresSource(config, PipelineContext(run_id="test")).get_identifier(
            schema="superset", entity="logs", inspector=mock_inspector
        )
        == "ops_database.superset.logs"
    )


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
