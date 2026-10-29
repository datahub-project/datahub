from types import SimpleNamespace
from typing import Any, Dict, List
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

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.probe_methods import _provider_class
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource
from datahub.ingestion.source.sql.postgres.query import POSTGRES_SYSTEM_DATABASES

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
    StatusClass,
    StringTypeClass,
    UpstreamLineageClass,
)
from datahub.utilities.sqlalchemy_type_converter import (
    get_native_data_type_for_sqlalchemy_type,
)
from tests.unit.conftest import as_mock, log_contexts, single_aspect
from tests.unit.sql_test_helpers import FakeConnection, FakeResult


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


# --- RDS IAM transport, probe contract and database listing ---


def test_rds_iam_tls_is_upgraded_and_judged_by_sslmode():
    """IAM tokens are bearer credentials. The listener-level cases (no sslmode,
    ``prefer``, verified modes) live in test_rds_iam_shared.py; this pins the
    config helpers themselves: ``disable`` is upgraded too, only a verify-* mode
    counts as verified, and the hint names the mode to set."""
    config = PostgresConfig.model_validate(_base_config())

    disabled: Dict[str, Any] = {"sslmode": "disable"}
    config.apply_rds_iam_ssl(disabled)
    assert disabled["sslmode"] == "require"
    assert not config.rds_iam_tls_is_verified(disabled)

    assert config.rds_iam_tls_is_verified({"sslmode": "verify-full"})
    assert "verify-full" in config.rds_iam_tls_hint()


def test_probe_catalog_scope_admits_pg_catalog_relation_by_relation():
    """pg_catalog is never allowed at schema level: pg_stats carries sampled
    user values and pg_shadow password hashes."""
    scope = PostgresConfig.probe_catalog_scope()
    assert "pg_catalog" not in scope.schemas
    assert "pg_catalog.pg_class" in scope.relations
    assert "pg_catalog.pg_attribute" in scope.relations
    for withheld in ("pg_stats", "pg_statistic", "pg_shadow", "pg_authid"):
        assert f"pg_catalog.{withheld}" not in scope.relations


def test_default_databases_match_the_listing_query_exclusions():
    """`probe filter` reports these as excluded_by default_database; they must
    be exactly the names PostgresQuery.list_databases leaves out."""
    defaults = PostgresConfig.default_databases()
    assert defaults == frozenset(POSTGRES_SYSTEM_DATABASES)
    assert {"template0", "template1", "rdsadmin"} <= defaults


def test_usage_statistics_require_query_lineage():
    with pytest.raises(ValidationError, match="include_query_lineage"):
        PostgresConfig.model_validate(
            {**_base_config(), "include_usage_statistics": True}
        )


def test_create_builds_source_from_recipe_dict():
    source = PostgresSource.create(
        {**_base_config(), "database": "custom_db"}, PipelineContext(run_id="test")
    )
    assert isinstance(source, PostgresSource)
    assert source.config.database == "custom_db"


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_get_inspectors_skips_databases_denied_by_pattern(
    create_engine_mock: MagicMock,
) -> None:
    """Denied databases are never connected to: one engine for the initial
    database's listing plus one per allowed database."""
    execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
    execute_mock.return_value = FakeResult(
        [{"datname": "keep_me"}, {"datname": "skip_me"}]
    )
    config = PostgresConfig.model_validate(
        {**_base_config(), "database_pattern": {"deny": ["skip_me"]}}
    )
    source = PostgresSource(config, PipelineContext(run_id="test"))

    inspectors = list(source.get_inspectors())

    assert len(inspectors) == 1
    assert create_engine_mock.call_count == 2
    assert create_engine_mock.call_args_list[1][0][0].endswith("keep_me")


# --- view lineage fallback for views sqlglot could not parse ---


def _view_lineage_inspector(rows: List[Dict[str, str]]) -> MagicMock:
    inspector = MagicMock()
    inspector.engine.url.database = "db"
    inspector.engine.connect.return_value = FakeConnection({"": rows})
    return inspector


def _dependency_row(view: str, source: str, schema: str = "public") -> Dict[str, str]:
    return {
        "source_table": source,
        "source_schema": schema,
        "dependent_view": view,
        "dependent_schema": schema,
    }


def test_view_lineage_elements_group_sources_and_apply_patterns():
    """pg_depend rows are grouped per view, and views denied by view_pattern or
    schema_pattern are dropped (and reported as filtered)."""
    config = PostgresConfig.model_validate(
        {
            **_base_config(),
            "view_pattern": {"deny": ["denied_view"]},
            "schema_pattern": {"deny": ["hidden"]},
        }
    )
    source = PostgresSource(config, PipelineContext(run_id="test"))
    inspector = _view_lineage_inspector(
        [
            _dependency_row("orders_v", "orders"),
            _dependency_row("orders_v", "customers"),
            _dependency_row("denied_view", "orders"),
            _dependency_row("secret_v", "orders", schema="hidden"),
        ]
    )

    elements = source._get_view_lineage_elements(inspector)

    assert list(elements) == [("orders_v", "public")]
    assert elements[("orders_v", "public")] == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.customers,PROD)",
    ]
    assert set(source.report.filtered) == {"hidden.secret_v", "public.denied_view"}


def test_view_lineage_elements_empty_when_query_returns_no_rows():
    source = PostgresSource(
        PostgresConfig.model_validate(_base_config()), PipelineContext(run_id="test")
    )

    assert source._get_view_lineage_elements(_view_lineage_inspector([])) == {}


def test_view_lineage_workunits_only_cover_views_that_failed_parsing():
    """Catalog-derived lineage is the fallback for views whose SQL sqlglot
    could not parse; views that parsed already have lineage from the aggregator."""
    source = PostgresSource(
        PostgresConfig.model_validate(_base_config()), PipelineContext(run_id="test")
    )
    source.views_failed_parsing = {"db.public.unparsed_v"}
    inspector = _view_lineage_inspector(
        [
            _dependency_row("unparsed_v", "orders"),
            _dependency_row("parsed_v", "orders"),
        ]
    )

    workunits = list(source._get_view_lineage_workunits(inspector))

    assert [wu.get_urn() for wu in workunits] == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.unparsed_v,PROD)"
    ]
    lineage = single_aspect(workunits, UpstreamLineageClass)
    assert [u.dataset for u in lineage.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.orders,PROD)"
    ]


def _query_lineage_source() -> PostgresSource:
    """A source with query lineage on and an autospecced aggregator."""
    config = PostgresConfig.model_validate(
        {**_base_config(), "database": "db", "include_query_lineage": True}
    )
    with patch(
        "datahub.ingestion.source.sql.postgres.source.SqlParsingAggregator",
        autospec=True,
    ):
        return PostgresSource(config, PipelineContext(run_id="test"))


def _workunit(name: str) -> MagicMock:
    return MagicMock(spec=MetadataWorkUnit, name=name)


def test_workunits_run_view_fallback_only_when_views_failed_parsing() -> None:
    source = _query_lineage_source()
    base_wu = _workunit("base")
    view_wu = _workunit("view")
    query_wu = _workunit("query")
    with (
        patch(
            "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_workunits_internal",
            side_effect=lambda *_: iter([base_wu]),
        ),
        patch.object(source, "get_inspectors", return_value=[MagicMock()]),
        patch.object(
            source,
            "_get_view_lineage_workunits",
            side_effect=lambda *_: iter([view_wu]),
        ) as view_lineage,
        patch.object(
            source,
            "_get_query_based_lineage_workunits",
            side_effect=lambda: iter([query_wu]),
        ),
    ):
        assert list(source.get_workunits_internal()) == [base_wu, query_wu]
        view_lineage.assert_not_called()

        source.views_failed_parsing = {"db.public.v"}
        assert list(source.get_workunits_internal()) == [base_wu, view_wu, query_wu]


# --- query-based lineage emission ---


@patch(
    "datahub.ingestion.source.sql.postgres.source.PostgresLineageExtractor",
    autospec=True,
)
def test_query_lineage_workunits_come_from_the_aggregator(
    extractor_class: MagicMock,
) -> None:
    source = _query_lineage_source()
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t,PROD)",
        aspect=StatusClass(removed=False),
    )
    as_mock(source.sql_aggregator).gen_metadata.return_value = iter([mcp])

    with patch.object(source, "get_inspectors", return_value=[MagicMock()]):
        workunits = list(source._get_query_based_lineage_workunits())

    extractor_class.return_value.populate_lineage_from_queries.assert_called_once()
    assert [wu.metadata for wu in workunits] == [mcp]
    assert not source.report.failures


@patch(
    "datahub.ingestion.source.sql.postgres.source.PostgresLineageExtractor",
    autospec=True,
)
def test_query_lineage_metadata_generation_failure_is_reported(
    extractor_class: MagicMock,
) -> None:
    source = _query_lineage_source()
    as_mock(source.sql_aggregator).gen_metadata.side_effect = RuntimeError(
        "schema resolution failed"
    )

    with patch.object(source, "get_inspectors", return_value=[MagicMock()]):
        workunits = list(source._get_query_based_lineage_workunits())

    assert workunits == []
    assert "lineage_metadata_generation_failed" in log_contexts(source.report.failures)


def test_close_closes_the_aggregator():
    source = _query_lineage_source()
    source.close()
    as_mock(source.sql_aggregator).close.assert_called_once()


# --- profiling metadata ---


def test_add_profile_metadata_records_table_sizes_by_identifier():
    config = PostgresConfig.model_validate({**_base_config(), "database": "db"})
    source = PostgresSource(config, PipelineContext(run_id="test"))
    inspector = MagicMock()
    inspector.engine.connect.return_value = FakeConnection(
        {
            "": [
                SimpleNamespace(
                    table_schema="public", table_name="orders", table_size=4096
                )
            ]
        }
    )

    source.add_profile_metadata(inspector)

    assert source.profile_metadata_info.dataset_name_to_storage_bytes == {
        "db.public.orders": 4096
    }


def test_add_profile_metadata_survives_permission_errors():
    config = PostgresConfig.model_validate({**_base_config(), "database": "db"})
    source = PostgresSource(config, PipelineContext(run_id="test"))
    inspector = MagicMock()
    inspector.engine.connect.return_value = FakeConnection(
        {"": RuntimeError("permission denied for pg_table_size")}
    )

    source.add_profile_metadata(inspector)

    assert source.profile_metadata_info.dataset_name_to_storage_bytes == {}
