"""OracleSource behaviour at the workunit level: inspector selection, stored
procedure and materialized view emission, V$SQL edge cases, profiling
candidates and config validation, against stubbed SQLAlchemy objects."""

import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import oracledb
import pytest
import sqlalchemy.exc
from pydantic import ValidationError
from sqlalchemy.engine import Connection

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.oracle import (
    DataDictionaryMode,
    OracleConfig,
    OracleInspectorObjectWrapper,
    OracleSource,
    _extract_oracle_error_code,
    output_type_handler,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DomainsClass,
    QueryLanguageClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import ObservedQuery


def _source(**overrides: Any) -> OracleSource:
    base: Dict[str, Any] = {
        "username": "test_user",
        "password": "test_pass",
        "host_port": "localhost:1521",
        "service_name": "test_service",
    }
    if "database" in overrides:
        base.pop("service_name")
    base.update(overrides)
    return OracleSource(OracleConfig.model_validate(base), PipelineContext(run_id="t"))


def _aspects(workunits: List[MetadataWorkUnit], aspect_type: type) -> List[Any]:
    return [
        aspect
        for aspect in (wu.get_aspect_of_type(aspect_type) for wu in workunits)
        if aspect is not None
    ]


# --- inspectors ---


@pytest.mark.parametrize(
    "mode, wrapped",
    [(DataDictionaryMode.DBA, True), (DataDictionaryMode.ALL, False)],
)
def test_get_inspectors_wraps_the_inspector_only_in_dba_mode(
    mode: DataDictionaryMode, wrapped: bool
) -> None:
    source = _source(data_dictionary_mode=mode.value)
    raw = MagicMock(name="inspector")

    with (
        patch.object(SQLAlchemySource, "get_inspectors", return_value=iter([raw])),
        patch("datahub.ingestion.source.sql.oracle.event.listen") as listen,
    ):
        (inspector,) = list(source.get_inspectors())

    assert isinstance(inspector, OracleInspectorObjectWrapper) == wrapped
    if not wrapped:
        assert inspector is raw
    # CLOB/BLOB output conversion is attached to every engine ingestion reads.
    listen.assert_called_once()
    assert listen.call_args.args[:2] == (raw.engine, "before_cursor_execute")


def test_clob_and_blob_columns_are_fetched_as_long_types() -> None:
    cursor = MagicMock()
    cursor.arraysize = 50

    output_type_handler(cursor, "body", oracledb.CLOB, None, None, None)
    cursor.var.assert_called_with(oracledb.DB_TYPE_LONG, arraysize=50)

    output_type_handler(cursor, "blob", oracledb.BLOB, None, None, None)
    cursor.var.assert_called_with(oracledb.DB_TYPE_LONG_RAW, arraysize=50)

    cursor.var.reset_mock()
    assert output_type_handler(cursor, "n", oracledb.NUMBER, None, None, None) is None
    cursor.var.assert_not_called()


# --- schema-level walk: procedures and materialized views ---


def test_schema_level_workunits_add_materialized_views_and_report_their_failure() -> (
    None
):
    source = _source(include_materialized_views=True)
    base_wu = MagicMock(name="base")
    mview_wu = MagicMock(name="mview")

    with (
        patch.object(
            SQLAlchemySource,
            "get_schema_level_workunits",
            side_effect=lambda *_, **__: iter([base_wu]),
        ),
        patch.object(
            source, "loop_materialized_views", return_value=iter([mview_wu])
        ) as loop,
    ):
        assert list(source.get_schema_level_workunits(Mock(), "HR", "ORCL")) == [
            base_wu,
            mview_wu,
        ]

        loop.side_effect = sqlalchemy.exc.DatabaseError("ORA-00942", None, Exception())
        assert list(source.get_schema_level_workunits(Mock(), "HR", "ORCL")) == [
            base_wu
        ]

    assert len(source.report.warnings) == 1
    assert "Materialized Views" in str(source.report.warnings[0])


def test_loop_stored_procedures_builds_containers_from_the_connection_database() -> (
    None
):
    # urn_db_name only shapes dataset URNs; the procedure's container hierarchy
    # must still hang off the database the connection actually reaches.
    source = _source(include_stored_procedures=True, urn_db_name="alias")
    procedure = Mock(spec=BaseProcedure)
    done = MagicMock(name="done")

    with (
        patch.object(source, "get_db_name", return_value="orcl"),
        patch.object(source, "fetch_procedures_for_schema", return_value=[procedure]),
        patch.object(
            source, "_process_procedures", return_value=iter([done])
        ) as process,
    ):
        assert list(source.loop_stored_procedures(Mock(), "hr", source.config)) == [
            done
        ]
    process.assert_called_once_with([procedure], "orcl", "hr")


def _procedure(**overrides: Any) -> BaseProcedure:
    fields: Dict[str, Any] = {
        "name": "load_audit",
        "procedure_definition": (
            "BEGIN INSERT INTO hr.audit_log SELECT * FROM hr.employees; END;"
        ),
        "created": None,
        "last_altered": None,
        "comment": None,
        "argument_signature": None,
        "return_type": None,
        "language": QueryLanguageClass.SQL,
        "extra_properties": None,
    }
    fields.update(overrides)
    return BaseProcedure(**fields)


def test_catalog_dependencies_become_input_jobs_alongside_parsed_lineage() -> None:
    source = _source(include_stored_procedures=True)
    procedure = _procedure(
        extra_properties={
            "upstream_dependencies": "HR.HELPER (PROCEDURE), HR.EMPLOYEES (TABLE)"
        }
    )

    workunits = list(
        source._process_procedure(
            procedure, "hr", "orcl", procedure_registry={"hr.helper": "helper_v2"}
        )
    )

    (lineage,) = _aspects(workunits, DataJobInputOutputClass)
    assert lineage.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:oracle,orcl.hr.employees,PROD)"
    ]
    assert lineage.outputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:oracle,orcl.hr.audit_log,PROD)"
    ]
    # The registry resolves an overloaded name to its identifier; tables are
    # left to the SQL parser.
    assert len(lineage.inputDatajobs) == 1
    assert lineage.inputDatajobs[0].endswith(",helper_v2)")


def test_procedures_without_a_registry_get_no_input_jobs() -> None:
    source = _source(include_stored_procedures=True)
    procedure = _procedure(
        extra_properties={"upstream_dependencies": "HR.HELPER (PROCEDURE)"}
    )

    workunits = list(source._process_procedure(procedure, "hr", "orcl"))

    (lineage,) = _aspects(workunits, DataJobInputOutputClass)
    assert not lineage.inputDatajobs


def test_a_procedure_that_fails_to_emit_is_reported_not_raised() -> None:
    source = _source(include_stored_procedures=True)

    with patch(
        "datahub.ingestion.source.sql.oracle.generate_procedure_workunits",
        side_effect=RuntimeError("bad definition"),
    ):
        assert list(source._process_procedure(_procedure(), "hr", "orcl")) == []

    assert len(source.report.warnings) == 1
    assert "orcl.hr.load_audit" in str(source.report.warnings[0])


# --- materialized views ---


def test_materialized_views_denied_by_view_pattern_are_dropped() -> None:
    source = _source(
        include_materialized_views=True, view_pattern={"deny": [r".*\.mv_secret"]}
    )
    inspector = Mock(spec=OracleInspectorObjectWrapper)
    inspector.get_materialized_view_names.return_value = ["mv_secret", "mv_sales"]
    processed = MagicMock(name="processed")

    with (
        patch.object(
            source,
            "get_identifier",
            side_effect=lambda schema, entity, inspector: f"{schema}.{entity}",
        ),
        patch.object(
            source, "_process_materialized_view", return_value=iter([processed])
        ) as process,
    ):
        workunits = list(source.loop_materialized_views(inspector, "hr", source.config))

    assert workunits == [processed]
    assert process.call_args.kwargs["mview"] == "mv_sales"
    assert list(source.report.filtered) == ["hr.mv_secret"]


def test_materialized_view_fallbacks_degrade_to_nothing_when_the_query_fails() -> None:
    source = _source()
    inspector = Mock()
    inspector.dialect.denormalize_name.side_effect = lambda x: x.upper()
    inspector.dialect.default_schema_name = "HR"
    inspector.engine.connect.side_effect = sqlalchemy.exc.DatabaseError(
        "ORA-00942", None, Exception()
    )
    inspector.bind = Mock(spec=Connection)
    inspector.bind.execute.side_effect = sqlalchemy.exc.DatabaseError(
        "ORA-00942", None, Exception()
    )

    assert source._get_materialized_view_names_fallback(inspector, "hr") == []
    assert (
        source._get_materialized_view_definition_fallback(inspector, "mv", "hr") is None
    )


def _mview_patches(source: OracleSource) -> List[Any]:
    return [
        patch.object(source, "get_table_properties", return_value=("desc", {}, None)),
        patch.object(source, "_get_columns", return_value=[]),
        patch.object(source, "add_table_to_schema_container", return_value=iter([])),
        patch.object(source, "get_db_name", return_value="orcl"),
    ]


def test_materialized_view_from_the_dba_wrapper_carries_instance_and_domain() -> None:
    source = _source(
        include_materialized_views=True,
        platform_instance="prod",
        domain={"urn:li:domain:people": {"allow": [r"hr\..*"]}},
    )
    inspector = Mock(spec=OracleInspectorObjectWrapper)
    inspector.get_materialized_view_definition.return_value = (
        "SELECT * FROM hr.employees"
    )
    patches = _mview_patches(source)
    for p in patches:
        p.start()
    try:
        workunits = list(
            source._process_materialized_view(
                "hr.mv_emp", inspector, "hr", "mv_emp", source.config
            )
        )
    finally:
        for p in patches:
            p.stop()

    inspector.get_materialized_view_definition.assert_called_once_with(
        mview_name="mv_emp", schema="hr"
    )
    (view_props,) = _aspects(workunits, ViewPropertiesClass)
    assert view_props.materialized
    assert view_props.viewLogic == "SELECT * FROM hr.employees"
    assert _aspects(workunits, DataPlatformInstanceClass)
    (domains,) = _aspects(workunits, DomainsClass)
    assert domains.domains == ["urn:li:domain:people"]
    assert not source.report.warnings


def test_a_materialized_view_that_fails_is_reported_not_raised() -> None:
    source = _source(include_materialized_views=True)
    inspector = Mock(spec=OracleInspectorObjectWrapper)
    inspector.get_materialized_view_definition.side_effect = (
        sqlalchemy.exc.DatabaseError("ORA-01031", None, Exception())
    )

    workunits = list(
        source._process_materialized_view(
            "hr.mv_emp", inspector, "hr", "mv_emp", source.config
        )
    )

    assert workunits == []
    assert len(source.report.warnings) == 1
    assert "hr.mv_emp" in str(source.report.warnings[0])


# --- V$SQL ---


def test_vsql_prerequisite_check_names_unexpected_oracle_errors() -> None:
    source = _source(include_query_usage=True)
    engine = Mock()
    engine.connect.side_effect = sqlalchemy.exc.DatabaseError(
        "ORA-12154: TNS could not resolve", None, Exception()
    )

    result = source._check_vsql_prerequisites(engine)

    assert not result.is_accessible
    assert "ORA-12154" in result.message


def test_oracle_error_code_is_absent_from_non_oracle_errors() -> None:
    assert _extract_oracle_error_code(RuntimeError("connection reset")) is None
    assert _extract_oracle_error_code(RuntimeError("ORA-00942: no table")) == 942


def test_vsql_first_load_time_accepts_both_oracle_formats() -> None:
    source = _source(include_query_usage=True)

    def row(sql_id: str, first_load_time: str) -> Dict[str, Any]:
        return {
            "sql_id": sql_id,
            "sql_text": f"SELECT {sql_id} FROM dual",
            "parsing_schema_name": "HR",
            "first_load_time": first_load_time,
        }

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.mappings.return_value = [
        row("a", "2024-01-01/12:00:00"),
        row("b", "2024-01-01 12:00:00"),
        row("c", "yesterday"),
    ]

    queries: List[ObservedQuery] = list(source._extract_queries_from_vsql(engine))

    assert [q.timestamp for q in queries] == [
        datetime.datetime(2024, 1, 1, 12),
        datetime.datetime(2024, 1, 1, 12),
        None,
    ]


@patch("datahub.ingestion.source.sql.oracle.create_engine")
def test_metadata_engine_uses_the_recipe_url_and_options(
    create_engine: MagicMock,
) -> None:
    source = _source(options={"pool_pre_ping": True})

    engine = source._get_metadata_engine()

    assert engine is create_engine.return_value
    create_engine.assert_called_once_with(
        source.config.get_sql_alchemy_url(), pool_pre_ping=True
    )


def test_lineage_is_emitted_only_after_schemas_and_vsql_are_loaded() -> None:
    # View definitions and V$SQL statements need the schema resolver populated
    # before column lineage can resolve, so the aggregator must flush last.
    source = _source()
    schema_wu = MagicMock(name="schema")
    lineage_wu = MagicMock(name="lineage")
    order: List[str] = []

    def base_walk(*_: Any, **__: Any) -> Any:
        order.append("schema")
        yield schema_wu
        # The parent flushes the aggregator at the end of its own walk; Oracle
        # defers that so nothing is emitted here.
        assert list(source._generate_aggregator_workunits()) == []

    def flush(*_: Any, **__: Any) -> Any:
        order.append("lineage")
        yield lineage_wu

    with (
        patch.object(SQLAlchemySource, "get_workunits_internal", side_effect=base_walk),
        patch.object(
            source,
            "_populate_aggregator_from_queries",
            side_effect=lambda: order.append("vsql"),
        ),
        patch.object(
            SQLAlchemySource, "_generate_aggregator_workunits", side_effect=flush
        ),
    ):
        assert list(source.get_workunits_internal()) == [schema_wu, lineage_wu]

    assert order == ["schema", "vsql", "lineage"]


# --- profiling ---


@pytest.mark.parametrize(
    "mode, tables_view",
    [(DataDictionaryMode.ALL, "ALL_TABLES"), (DataDictionaryMode.DBA, "DBA_TABLES")],
)
def test_profile_candidates_come_from_the_stats_filtered_catalog_query(
    mode: DataDictionaryMode, tables_view: str
) -> None:
    source = _source(
        data_dictionary_mode=mode.value,
        profiling={"enabled": True, "profile_table_row_limit": 1000},
    )
    inspector = Mock()
    inspector.bind = Mock(spec=Connection)
    inspector.dialect.denormalize_name.side_effect = lambda x: x.upper()
    inspector.dialect.normalize_name.side_effect = lambda x: x.lower()
    inspector.bind.execute.return_value = [
        ("HR", "SMALL_TABLE", 10, None, 0.0),
        ("HR", "UNANALYZED", None, None, 0.0),
    ]

    with patch.object(
        source,
        "get_identifier",
        side_effect=lambda schema, entity, inspector: f"{schema}.{entity}",
    ):
        candidates = source.generate_profile_candidates(inspector, None, "hr")

    assert candidates == ["hr.small_table", "hr.unanalyzed"]
    statement, params = inspector.bind.execute.call_args.args
    assert tables_view in str(statement)
    assert params["owner"] == "HR"
    assert params["table_row_limit"] == 1000


# --- config ---


def test_query_usage_config_limits() -> None:
    with pytest.raises(ValidationError, match="must be positive"):
        _source(max_queries_to_extract=0)
    with pytest.raises(ValidationError, match="<= 10000"):
        _source(max_queries_to_extract=20000)
    with pytest.raises(ValidationError, match="<= 100 patterns"):
        _source(query_exclude_patterns=[f"p{i}" for i in range(101)])
    assert _source(query_exclude_patterns=None).config.query_exclude_patterns is None


def test_database_and_urn_db_name_are_mutually_exclusive() -> None:
    with pytest.raises(ValidationError, match="urn_db_name"):
        _source(database="orcl", urn_db_name="alias")


@patch("datahub.ingestion.source.sql.oracle.platform.system", return_value="Darwin")
def test_thick_mode_needs_a_client_library_dir_on_mac_and_windows(
    _system: MagicMock,
) -> None:
    with pytest.raises(ValidationError, match="thick_mode_lib_dir"):
        OracleConfig.model_validate(
            {
                "username": "u",
                "password": "p",
                "host_port": "localhost:1521",
                "service_name": "svc",
                "enable_thick_mode": True,
            }
        )
