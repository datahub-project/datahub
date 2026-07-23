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

    config = PostgresConfig.model_validate(_base_config())
    assert config.min_query_calls == 1

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


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_sql_aggregator_initialization_failure(create_engine_mock):
    """Test that SQL aggregator initialization failure fails loudly when feature is explicitly enabled."""
    with patch(
        "datahub.ingestion.source.sql.postgres.source.SqlParsingAggregator"
    ) as mock_aggregator:
        mock_aggregator.side_effect = Exception("Aggregator init failed")

        config = PostgresConfig.model_validate(
            {**_base_config(), "include_query_lineage": True}
        )

        # Should raise RuntimeError when the explicitly enabled feature fails to initialize
        with pytest.raises(RuntimeError) as exc_info:
            PostgresSource(config, PipelineContext(run_id="test"))

        error_message = str(exc_info.value)
        assert "explicitly enabled" in error_message.lower(), (
            "Should mention feature was explicitly enabled"
        )
        assert "include_query_lineage: true" in error_message, (
            "Should mention the config flag"
        )


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_usage_statistics_requires_graph_connection(create_engine_mock):
    """Test that usage statistics validation fails when graph connection is missing."""
    config = PostgresConfig.model_validate(
        {
            **_base_config(),
            "include_query_lineage": True,
            "include_usage_statistics": True,
        }
    )

    ctx = PipelineContext(run_id="test")
    assert ctx.graph is None, "Test setup: context should not have graph"

    with pytest.raises(ValueError) as exc_info:
        PostgresSource(config, ctx)

    error_message = str(exc_info.value)
    assert "graph connection" in error_message.lower(), (
        "Should mention graph connection requirement"
    )
    assert "include_usage_statistics" in error_message.lower(), (
        "Should mention the usage statistics flag"
    )


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
def test_view_lineage_empty_returns_iterator(create_engine_mock):
    """Test that _get_view_lineage_workunits returns empty iterator, not None."""
    config = PostgresConfig.model_validate({**_base_config()})
    source = PostgresSource(config, PipelineContext(run_id="test"))

    mock_inspector = MagicMock()

    # Mock _get_view_lineage_elements to return empty dict
    with patch.object(source, "_get_view_lineage_elements", return_value={}):
        # This should not crash even though lineage_elements is empty
        workunits = list(source._get_view_lineage_workunits(mock_inspector))
        assert workunits == [], "Should return empty list, not crash with None"


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
                source.report.failure(
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


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_get_procedures_for_schema(create_engine_mock):
    """Test that get_procedures_for_schema maps DB rows to BaseProcedure correctly.

    Verifies that:
    - Fields are mapped correctly (name, language, arguments, comment, definition)
    - Language is normalized to uppercase (postgres returns lowercase "sql")
    - procedure_definition uses prosrc (body only, no CREATE PROCEDURE wrapper)
    """
    from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure

    config = PostgresConfig.model_validate({**_base_config(), "database": "testdb"})
    source = PostgresSource(config, PipelineContext(run_id="test"))

    mock_inspector = MagicMock()
    mock_conn = MagicMock()
    mock_inspector.engine.connect.return_value.__enter__.return_value = mock_conn

    mock_row = MagicMock()
    mock_row.name = "etl_process_orders"
    mock_row.language = "sql"  # postgres returns lowercase
    mock_row.arguments = ""
    mock_row.definition = (
        "    INSERT INTO processed_orders (order_id, customer_id, total)\n"
        "    SELECT id AS order_id, customer_id, amount AS total FROM raw_orders;\n"
    )
    mock_row.comment = "ETL procedure to process orders"
    mock_conn.execute.return_value = [mock_row]

    procedures = source.get_procedures_for_schema(mock_inspector, "public", "testdb")

    assert len(procedures) == 1
    proc = procedures[0]
    assert isinstance(proc, BaseProcedure)
    assert proc.name == "etl_process_orders"
    assert proc.language == "SQL"  # normalized to uppercase
    assert proc.argument_signature == ""
    assert proc.comment == "ETL procedure to process orders"
    assert proc.procedure_definition is not None
    assert "INSERT INTO processed_orders" in proc.procedure_definition
    # prosrc returns body only — no CREATE PROCEDURE wrapper that would break lineage
    assert not proc.procedure_definition.strip().upper().startswith("CREATE")


def test_postgres_special_types_map_to_datahub_types():
    """
    PostGIS, pgvector, built-in geometric, xml, ltree, citext, cidr and range
    columns must map to real DataHub types instead of NullType (#18575).
    """
    from geoalchemy2 import Geography, Geometry, Raster
    from sqlalchemy.dialects.postgresql import (
        CIDR,
        DATERANGE,
        INT4RANGE,
        INT8RANGE,
        NUMRANGE,
        TSRANGE,
        TSTZRANGE,
        base as pg_base,
    )

    from datahub.ingestion.source.sql.sql_common import get_column_type
    from datahub.ingestion.source.sql.sql_report import SQLSourceReport
    from datahub.metadata.schema_classes import (
        ArrayTypeClass,
        RecordTypeClass,
        StringTypeClass,
    )

    cases = [
        (Geometry(), RecordTypeClass),
        (Geography(), RecordTypeClass),
        (Raster(), RecordTypeClass),
        (pg_base.ischema_names["vector"](), ArrayTypeClass),
        (pg_base.ischema_names["halfvec"](), ArrayTypeClass),
        (pg_base.ischema_names["sparsevec"](), ArrayTypeClass),
        (pg_base.ischema_names["point"](), RecordTypeClass),
        (pg_base.ischema_names["line"](), RecordTypeClass),
        (pg_base.ischema_names["lseg"](), RecordTypeClass),
        (pg_base.ischema_names["box"](), RecordTypeClass),
        (pg_base.ischema_names["path"](), RecordTypeClass),
        (pg_base.ischema_names["polygon"](), RecordTypeClass),
        (pg_base.ischema_names["circle"](), RecordTypeClass),
        (pg_base.ischema_names["xml"](), StringTypeClass),
        (pg_base.ischema_names["ltree"](), StringTypeClass),
        (pg_base.ischema_names["citext"](), StringTypeClass),
        (CIDR(), StringTypeClass),
        (INT4RANGE(), StringTypeClass),
        (INT8RANGE(), StringTypeClass),
        (NUMRANGE(), StringTypeClass),
        (DATERANGE(), StringTypeClass),
        (TSRANGE(), StringTypeClass),
        (TSTZRANGE(), StringTypeClass),
    ]

    report = SQLSourceReport()
    for column_type, expected_class in cases:
        actual = get_column_type(report, "test_dataset", column_type)
        assert isinstance(actual.type, expected_class), (
            f"{column_type!r} mapped to {actual.type}, expected {expected_class.__name__}"
        )

    # None of these should have hit the "Unable to map" fallback.
    assert not report.infos


def test_postgres_special_types_preserve_native_names():
    """nativeDataType must carry the real type name, not 'null' (#18575)."""
    from sqlalchemy.dialects.postgresql import CIDR, INT4RANGE, base as pg_base
    from sqlalchemy.dialects.postgresql.base import PGDialect

    from datahub.utilities.sqlalchemy_type_converter import (
        get_native_data_type_for_sqlalchemy_type,
    )

    inspector = mock.MagicMock()
    inspector.dialect = PGDialect()

    expected_native = {
        "vector": "VECTOR",
        "point": "POINT",
        "line": "LINE",
        "lseg": "LSEG",
        "box": "BOX",
        "path": "PATH",
        "polygon": "POLYGON",
        "circle": "CIRCLE",
        "xml": "XML",
        "ltree": "LTREE",
        "citext": "CITEXT",
    }
    for ischema_key, native in expected_native.items():
        column_type = pg_base.ischema_names[ischema_key]()
        assert (
            get_native_data_type_for_sqlalchemy_type(column_type, inspector) == native
        )

    assert get_native_data_type_for_sqlalchemy_type(CIDR(), inspector) == "CIDR"
    assert (
        get_native_data_type_for_sqlalchemy_type(INT4RANGE(), inspector) == "INT4RANGE"
    )

    # Reflection passes type modifiers through, e.g. a vector(4) column.
    assert (
        get_native_data_type_for_sqlalchemy_type(
            pg_base.ischema_names["vector"](4), inspector
        )
        == "VECTOR(4)"
    )
