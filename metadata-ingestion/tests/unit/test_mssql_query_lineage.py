from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource


def _base_config():
    return {
        "username": "sa",
        "password": "test",
        "host_port": "localhost:1433",
        "database": "TestDB",
    }


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_config_with_query_lineage(create_engine_mock):
    """Test MS SQL config with query lineage enabled."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "max_queries_to_extract": 500,
        }
    )
    assert config.include_query_lineage is True
    assert config.max_queries_to_extract == 500


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_usage_statistics_requires_query_lineage(create_engine_mock):
    """Test that usage statistics require query lineage to be enabled."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "include_usage_statistics": True,
        }
    )
    assert config.include_query_lineage is True
    assert config.include_usage_statistics is True

    with pytest.raises(
        ValueError, match="include_usage_statistics requires include_query_lineage"
    ):
        SQLServerConfig.model_validate(
            {
                **_base_config(),
                "include_query_lineage": False,
                "include_usage_statistics": True,
            }
        )


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_usage_statistics_requires_graph_connection(create_engine_mock):
    """Test that usage statistics validation fails when graph connection is missing."""
    config = SQLServerConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "include_usage_statistics": True,
        }
    )

    ctx = PipelineContext(run_id="test")
    assert ctx.graph is None

    with pytest.raises(ValueError, match="graph connection"):
        SQLServerSource(config, ctx)


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_sql_aggregator_initialization_failure(create_engine_mock):
    """Test that SQL aggregator initialization failure fails loudly."""
    with patch(
        "datahub.ingestion.source.sql.mssql.source.SqlParsingAggregator"
    ) as mock_aggregator:
        mock_aggregator.side_effect = Exception("Aggregator init failed")

        config = SQLServerConfig.model_validate(
            {**_base_config(), "include_query_lineage": True}
        )

        with pytest.raises(RuntimeError) as exc_info:
            SQLServerSource(config, PipelineContext(run_id="test"))

        error_message = str(exc_info.value)
        assert "explicitly enabled" in error_message.lower()
        assert "include_query_lineage: true" in error_message


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_max_queries_validation(create_engine_mock):
    """Test max_queries_to_extract validation."""
    with pytest.raises(ValueError, match="must be positive"):
        SQLServerConfig.model_validate({**_base_config(), "max_queries_to_extract": 0})

    with pytest.raises(ValueError, match="<= 10000"):
        SQLServerConfig.model_validate(
            {**_base_config(), "max_queries_to_extract": 20000}
        )

    config = SQLServerConfig.model_validate(
        {**_base_config(), "max_queries_to_extract": 5000}
    )
    assert config.max_queries_to_extract == 5000


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_min_query_calls_validation(create_engine_mock):
    """Test min_query_calls validation."""
    with pytest.raises(ValueError, match="non-negative"):
        SQLServerConfig.model_validate({**_base_config(), "min_query_calls": -1})

    config = SQLServerConfig.model_validate({**_base_config(), "min_query_calls": 5})
    assert config.min_query_calls == 5


@patch("datahub.ingestion.source.sql.mssql.source.create_engine")
def test_mssql_source_has_close_method(create_engine_mock):
    """Test that MSSQLSource has close() method."""
    config = SQLServerConfig.model_validate({**_base_config()})
    source = SQLServerSource(config, PipelineContext(run_id="test"))

    assert hasattr(source, "close")
    assert callable(source.close)

    # Should not crash when calling close
    source.close()
