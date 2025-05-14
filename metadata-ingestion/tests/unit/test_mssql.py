from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource


@pytest.fixture
def mssql_source():
    config = SQLServerConfig(
        host_port="localhost:1433",
        username="test",
        password="test",
        database="test_db",
        temporary_tables_pattern=["^temp_"],
        include_descriptions=False,  # Disable description loading to avoid DB connections
    )

    # Mock the parent class's __init__ to avoid DB connections
    with patch("datahub.ingestion.source.sql.sql_common.SQLAlchemySource.__init__"):
        source = SQLServerSource(config, MagicMock())
        source.discovered_datasets = {"test_db.dbo.regular_table"}
        return source


def test_is_temp_table(mssql_source):
    # Test tables matching temporary table patterns
    assert mssql_source.is_temp_table("test_db.dbo.temp_table") is True

    # Test tables starting with # (handled by startswith check in is_temp_table)
    assert mssql_source.is_temp_table("test_db.dbo.#some_table") is True

    # Test tables that are not in discovered_datasets
    assert mssql_source.is_temp_table("test_db.dbo.unknown_table") is True

    # Test regular tables that should return False
    assert mssql_source.is_temp_table("test_db.dbo.regular_table") is False

    # Test invalid table name format
    assert mssql_source.is_temp_table("invalid_table_name") is False
