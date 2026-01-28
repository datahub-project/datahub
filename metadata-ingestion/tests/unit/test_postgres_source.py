from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource


def _base_config():
    return {"username": "user", "password": "password", "host_port": "host:1521"}


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_initial_database(create_engine_mock):
    config = PostgresConfig.model_validate(_base_config())
    assert config.initial_database == "postgres"
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())
    assert create_engine_mock.call_count == 1
    assert create_engine_mock.call_args[0][0].endswith("postgres")


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_get_inspectors_multiple_databases(create_engine_mock):
    execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
    execute_mock.return_value = [{"datname": "db1"}, {"datname": "db2"}]

    config = PostgresConfig.model_validate(
        {**_base_config(), "initial_database": "db0"}
    )
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())
    assert create_engine_mock.call_count == 3
    assert create_engine_mock.call_args_list[0][0][0].endswith("db0")
    assert create_engine_mock.call_args_list[1][0][0].endswith("db1")
    assert create_engine_mock.call_args_list[2][0][0].endswith("db2")


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def tests_get_inspectors_with_database_provided(create_engine_mock):
    execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
    execute_mock.return_value = [{"datname": "db1"}, {"datname": "db2"}]

    config = PostgresConfig.model_validate({**_base_config(), "database": "custom_db"})
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())
    assert create_engine_mock.call_count == 1
    assert create_engine_mock.call_args_list[0][0][0].endswith("custom_db")


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def tests_get_inspectors_with_sqlalchemy_uri_provided(create_engine_mock):
    execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
    execute_mock.return_value = [{"datname": "db1"}, {"datname": "db2"}]

    config = PostgresConfig.model_validate(
        {**_base_config(), "sqlalchemy_uri": "custom_url"}
    )
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())
    assert create_engine_mock.call_count == 1
    assert create_engine_mock.call_args_list[0][0][0] == "custom_url"


def test_database_in_identifier():
    config = PostgresConfig.model_validate({**_base_config(), "database": "postgres"})
    mock_inspector = mock.MagicMock()
    assert (
        PostgresSource(config, PipelineContext(run_id="test")).get_identifier(
            schema="superset", entity="logs", inspector=mock_inspector
        )
        == "postgres.superset.logs"
    )


def test_current_sqlalchemy_database_in_identifier():
    config = PostgresConfig.model_validate({**_base_config()})
    mock_inspector = mock.MagicMock()
    mock_inspector.engine.url.database = "current_db"
    assert (
        PostgresSource(config, PipelineContext(run_id="test")).get_identifier(
            schema="superset", entity="logs", inspector=mock_inspector
        )
        == "current_db.superset.logs"
    )


def test_max_queries_to_extract_validation():
    """Test that max_queries_to_extract is validated."""
    config = PostgresConfig.model_validate(
        {**_base_config(), "max_queries_to_extract": 5000}
    )
    assert config.max_queries_to_extract == 5000

    with pytest.raises(
        ValidationError, match="max_queries_to_extract must be positive"
    ):
        PostgresConfig.model_validate({**_base_config(), "max_queries_to_extract": 0})

    with pytest.raises(
        ValidationError, match="max_queries_to_extract must be positive"
    ):
        PostgresConfig.model_validate(
            {**_base_config(), "max_queries_to_extract": -100}
        )

    with pytest.raises(
        ValidationError,
        match="max_queries_to_extract must be <= 10000 to avoid memory issues",
    ):
        PostgresConfig.model_validate(
            {**_base_config(), "max_queries_to_extract": 20000}
        )


def test_min_query_calls_validation():
    """Test that min_query_calls is validated."""
    config = PostgresConfig.model_validate({**_base_config(), "min_query_calls": 10})
    assert config.min_query_calls == 10

    config = PostgresConfig.model_validate({**_base_config(), "min_query_calls": None})
    assert config.min_query_calls is None

    with pytest.raises(ValidationError, match="min_query_calls must be non-negative"):
        PostgresConfig.model_validate({**_base_config(), "min_query_calls": -5})


def test_query_exclude_patterns_validation():
    """Test that query_exclude_patterns is validated."""
    config = PostgresConfig.model_validate(
        {**_base_config(), "query_exclude_patterns": ["%temp%", "%staging%"]}
    )
    assert config.query_exclude_patterns == ["%temp%", "%staging%"]

    config = PostgresConfig.model_validate(
        {**_base_config(), "query_exclude_patterns": None}
    )
    assert config.query_exclude_patterns is None

    with pytest.raises(
        ValidationError,
        match="query_exclude_patterns must have <= 100 patterns to avoid performance issues",
    ):
        PostgresConfig.model_validate(
            {
                **_base_config(),
                "query_exclude_patterns": [f"%pattern_{i}%" for i in range(101)],
            }
        )

    with pytest.raises(
        ValidationError,
        match="exceeds 500 characters",
    ):
        PostgresConfig.model_validate(
            {**_base_config(), "query_exclude_patterns": ["%" + "x" * 501 + "%"]}
        )


def test_usage_statistics_requires_query_lineage():
    """Test that include_usage_statistics requires include_query_lineage."""
    config = PostgresConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "include_usage_statistics": True,
        }
    )
    assert config.include_query_lineage is True
    assert config.include_usage_statistics is True

    config = PostgresConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": False,
            "include_usage_statistics": False,
        }
    )
    assert config.include_query_lineage is False
    assert config.include_usage_statistics is False

    config = PostgresConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "include_usage_statistics": False,
        }
    )
    assert config.include_query_lineage is True
    assert config.include_usage_statistics is False

    with pytest.raises(
        ValidationError,
        match="include_usage_statistics requires include_query_lineage to be enabled",
    ):
        PostgresConfig.model_validate(
            {
                **_base_config(),
                "include_query_lineage": False,
                "include_usage_statistics": True,
            }
        )


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_sql_aggregator_initialization_failure(create_engine_mock):
    """Test that SQL aggregator initialization failure is handled gracefully."""
    with patch(
        "datahub.ingestion.source.sql.postgres.source.SqlParsingAggregator"
    ) as mock_aggregator:
        mock_aggregator.side_effect = Exception("Aggregator init failed")

        config = PostgresConfig.model_validate(
            {**_base_config(), "include_query_lineage": True}
        )
        source = PostgresSource(config, PipelineContext(run_id="test"))

        assert source.sql_aggregator is None
        assert source.report.failures
        failure_messages = [f.message for f in source.report.failures]
        assert any("explicitly enabled" in msg.lower() for msg in failure_messages), (
            "Should mention feature was explicitly enabled"
        )
        assert any("failed to start" in msg.lower() for msg in failure_messages), (
            "Should mention initialization failure"
        )


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_sql_aggregator_none_reports_warning(create_engine_mock):
    """Test that attempting lineage extraction with None aggregator reports warning."""
    with patch(
        "datahub.ingestion.source.sql.postgres.source.SqlParsingAggregator"
    ) as mock_aggregator:
        mock_aggregator.side_effect = Exception("Aggregator init failed")

        config = PostgresConfig.model_validate(
            {**_base_config(), "include_query_lineage": True}
        )
        source = PostgresSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_inspector.engine.connect.return_value.__enter__.return_value = MagicMock()

        with patch.object(source, "get_inspectors", return_value=[mock_inspector]):
            list(source._get_query_based_lineage_workunits())

            assert source.report.warnings
            warning_reasons = [w.message for w in source.report.warnings]
            assert any(
                "failed to initialize" in reason.lower() for reason in warning_reasons
            ), "Should report warning about failed initialization"


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_query_lineage_extraction_failure(create_engine_mock):
    """Test that query lineage extraction failure doesn't crash the source."""
    config = PostgresConfig.model_validate(
        {**_base_config(), "include_query_lineage": True}
    )

    with patch("datahub.ingestion.source.sql.postgres.source.SqlParsingAggregator"):
        source = PostgresSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_inspector.engine.connect.return_value.__enter__.return_value = MagicMock()

        with (
            patch.object(source, "get_inspectors", return_value=[mock_inspector]),
            patch(
                "datahub.ingestion.source.sql.postgres.source.PostgresLineageExtractor"
            ) as mock_extractor_class,
        ):
            mock_extractor = mock_extractor_class.return_value
            mock_extractor.populate_lineage_from_queries.side_effect = Exception(
                "Lineage extraction failed"
            )

            list(source._get_query_based_lineage_workunits())

            assert source.report.failures


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_query_lineage_prerequisites_failure(create_engine_mock):
    """Test that ingestion continues when pg_stat_statements prerequisites fail."""
    config = PostgresConfig.model_validate(
        {**_base_config(), "include_query_lineage": True}
    )

    with patch("datahub.ingestion.source.sql.postgres.source.SqlParsingAggregator"):
        source = PostgresSource(config, PipelineContext(run_id="test"))

        mock_inspector = MagicMock()
        mock_connection = MagicMock()
        mock_inspector.engine.connect.return_value.__enter__.return_value = (
            mock_connection
        )

        with (
            patch.object(source, "get_inspectors", return_value=[mock_inspector]),
            patch(
                "datahub.ingestion.source.sql.postgres.source.PostgresLineageExtractor"
            ) as mock_extractor_class,
        ):
            mock_extractor = mock_extractor_class.return_value
            mock_extractor.extract_query_history.return_value = []

            def mock_populate_with_failure() -> None:
                source.report.report_failure(
                    message="pg_stat_statements extension is not installed",
                    context="pg_stat_statements_not_ready",
                )

            mock_extractor.populate_lineage_from_queries.side_effect = (
                mock_populate_with_failure
            )

            workunits = list(source._get_query_based_lineage_workunits())

            assert len(workunits) == 0
            assert source.report.failures
            failure_messages = [f.message for f in source.report.failures]
            assert any("pg_stat_statements" in msg.lower() for msg in failure_messages)
