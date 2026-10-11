from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import (
    CIDR,
    CITEXT,
    INT4MULTIRANGE,
    INT4RANGE,
    TSTZMULTIRANGE,
    base as pg_base,
)
from sqlalchemy.dialects.postgresql.base import PGDialect

from datahub.ingestion.agent.probe_methods import _provider_class
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource

# Importing the placeholder types directly (rather than relying on the
# module-level registration side effects of importing the source) makes the
# dependency on postgres/source.py explicit and enforced.
from datahub.ingestion.source.sql.postgres.source import (
    BOX,
    CIRCLE,
    LINE,
    LSEG,
    LTREE,
    PATH,
    POINT,
    POLYGON,
    VECTOR,
    XML,
)
from datahub.ingestion.source.sql.sql_common import get_column_type
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    StringTypeClass,
)
from datahub.utilities.sqlalchemy_type_converter import (
    get_native_data_type_for_sqlalchemy_type,
)


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
    execute_mock.return_value.mappings.return_value = [
        {"datname": "db1"},
        {"datname": "db2"},
    ]

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


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_engines_default_to_autocommit(create_engine_mock):
    # On SA 2.0 a failed statement would otherwise abort the autobegun
    # transaction and fail every later query on the connection (25P02).
    execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
    execute_mock.return_value.mappings.return_value = [{"datname": "db1"}]

    config = PostgresConfig.model_validate(
        {**_base_config(), "options": {"pool_size": 3}}
    )
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())

    # Both the initial-database engine and the per-database engine.
    assert create_engine_mock.call_count == 2
    for call in create_engine_mock.call_args_list:
        assert call.kwargs == {"isolation_level": "AUTOCOMMIT", "pool_size": 3}


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_user_isolation_level_overrides_autocommit_default(create_engine_mock):
    config = PostgresConfig.model_validate(
        {
            **_base_config(),
            "database": "custom_db",
            "options": {"isolation_level": "REPEATABLE READ"},
        }
    )
    source = PostgresSource(config, PipelineContext(run_id="test"))
    _ = list(source.get_inspectors())

    assert create_engine_mock.call_args.kwargs == {"isolation_level": "REPEATABLE READ"}


def test_view_names_include_materialized_views():
    # SA 2.0's PG get_view_names() omits materialized views (1.4 included them).
    source = PostgresSource(
        PostgresConfig.model_validate(_base_config()), PipelineContext(run_id="test")
    )
    inspector = mock.MagicMock()
    inspector.get_view_names.return_value = ["v1", "mv_shared"]
    inspector.get_materialized_view_names.return_value = ["mv1", "mv_shared"]

    assert source._get_view_names(inspector, "public") == ["v1", "mv_shared", "mv1"]


def test_view_names_survive_materialized_view_listing_failure():
    source = PostgresSource(
        PostgresConfig.model_validate(_base_config()), PipelineContext(run_id="test")
    )
    inspector = mock.MagicMock()
    inspector.get_view_names.return_value = ["v1"]
    inspector.get_materialized_view_names.side_effect = RuntimeError("boom")

    assert source._get_view_names(inspector, "public") == ["v1"]
    assert source.report.warnings


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
    from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure

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
    PostGIS, pgvector, built-in geometric, xml, ltree, citext, cidr, range and
    multirange columns must map to real DataHub types instead of NullType
    (#18575).
    """
    # Resolve through ischema_names, as reflection does, rather than through
    # DataHub's placeholders: when SQLAlchemy ships a native class for a name
    # (e.g. the multiranges and CITEXT on 2.0) the placeholder is never used,
    # and only the native class's mapping matters.
    expected_by_ischema_name = {
        "geometry": BytesTypeClass,
        "geography": BytesTypeClass,
        "raster": BytesTypeClass,
        "vector": ArrayTypeClass,
        "halfvec": ArrayTypeClass,
        "sparsevec": ArrayTypeClass,
        "point": BytesTypeClass,
        "line": BytesTypeClass,
        "lseg": BytesTypeClass,
        "box": BytesTypeClass,
        "path": BytesTypeClass,
        "polygon": BytesTypeClass,
        "circle": BytesTypeClass,
        "xml": StringTypeClass,
        "ltree": StringTypeClass,
        "citext": StringTypeClass,
        "cidr": StringTypeClass,
        "int4range": StringTypeClass,
        "int8range": StringTypeClass,
        "numrange": StringTypeClass,
        "daterange": StringTypeClass,
        "tsrange": StringTypeClass,
        "tstzrange": StringTypeClass,
        "int4multirange": StringTypeClass,
        "int8multirange": StringTypeClass,
        "nummultirange": StringTypeClass,
        "datemultirange": StringTypeClass,
        "tsmultirange": StringTypeClass,
        "tstzmultirange": StringTypeClass,
    }

    report = SQLSourceReport()
    for ischema_name, expected_class in expected_by_ischema_name.items():
        column_type = pg_base.ischema_names[ischema_name]()
        actual = get_column_type(report, "test_dataset", column_type)
        assert isinstance(actual.type, expected_class), (
            f"{ischema_name} ({column_type!r}) mapped to {actual.type}, "
            f"expected {expected_class.__name__}"
        )

    # None of these should have hit the "Unable to map" fallback.
    assert not report.infos


def test_postgres_special_types_preserve_native_names():
    """nativeDataType must carry the real type name, not 'null' (#18575)."""
    inspector = mock.MagicMock()
    inspector.dialect = PGDialect()

    expected_native = {
        VECTOR: "VECTOR",
        POINT: "POINT",
        LINE: "LINE",
        LSEG: "LSEG",
        BOX: "BOX",
        PATH: "PATH",
        POLYGON: "POLYGON",
        CIRCLE: "CIRCLE",
        XML: "XML",
        LTREE: "LTREE",
        CITEXT: "CITEXT",
        INT4MULTIRANGE: "INT4MULTIRANGE",
        TSTZMULTIRANGE: "TSTZMULTIRANGE",
    }
    for column_type_cls, native in expected_native.items():
        assert (
            get_native_data_type_for_sqlalchemy_type(column_type_cls(), inspector)
            == native
        )

    assert get_native_data_type_for_sqlalchemy_type(CIDR(), inspector) == "CIDR"
    assert (
        get_native_data_type_for_sqlalchemy_type(INT4RANGE(), inspector) == "INT4RANGE"
    )

    # Reflection passes type modifiers through, e.g. a vector(4) column.
    assert get_native_data_type_for_sqlalchemy_type(VECTOR(4), inspector) == "VECTOR(4)"


def test_probe_support_loads_with_core_dependencies():
    # Needs only core dependencies, so the registry-wide probe contract tests
    # are guaranteed at least this provider in any environment.
    assert _provider_class("postgres") is not None
